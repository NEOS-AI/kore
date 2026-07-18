use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use crate::search_index::DistanceMetric;

/// Vector search result
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub doc_id: Bytes,
    pub score: f32,
    pub vector: Vec<f32>,
}

/// HNSW (Hierarchical Navigable Small World) graph layer
#[derive(Debug, Clone)]
struct HNSWLayer {
    /// Maps node ID to its neighbors
    neighbors: HashMap<Bytes, Vec<Bytes>>,
}

impl HNSWLayer {
    fn new() -> Self {
        Self {
            neighbors: HashMap::new(),
        }
    }

    /// Insert or reset a node with an empty neighbor list.
    ///
    /// Always clears edges so a re-`add` of the same id cannot revive stale
    /// neighbors left behind by a prior life (Batch CS).
    fn add_node(&mut self, node_id: Bytes) {
        self.neighbors.insert(node_id, Vec::new());
    }

    /// Add a directed edge, skipping self-loops and duplicates.
    fn add_edge(&mut self, from: Bytes, to: Bytes) {
        if from == to {
            return;
        }
        let edges = self.neighbors.entry(from).or_insert_with(Vec::new);
        if !edges.iter().any(|n| n == &to) {
            edges.push(to);
        }
    }

    fn get_neighbors(&self, node_id: &Bytes) -> Vec<Bytes> {
        self.neighbors.get(node_id).cloned().unwrap_or_default()
    }

    fn set_neighbors(&mut self, node_id: Bytes, neighbors: Vec<Bytes>) {
        self.neighbors.insert(node_id, neighbors);
    }

    /// Drop every directed edge that points at `node_id` (and the node entry).
    fn unlink_node(&mut self, node_id: &Bytes) {
        for edges in self.neighbors.values_mut() {
            edges.retain(|n| n != node_id);
        }
        self.neighbors.remove(node_id);
    }
}

fn cmp_f32(a: f32, b: f32) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Min-heap candidate (BinaryHeap is max-heap, so Ord is reversed by distance).
#[derive(Clone)]
struct MinCand {
    distance: f32,
    id: Bytes,
}

impl MinCand {
    fn new(distance: f32, id: Bytes) -> Self {
        Self { distance, id }
    }

    fn dist(&self) -> f32 {
        self.distance
    }
}

impl PartialEq for MinCand {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.id == other.id
    }
}
impl Eq for MinCand {}

impl Ord for MinCand {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: smaller distance pops first
        cmp_f32(other.distance, self.distance).then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for MinCand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Max-heap candidate for the dynamic nearest set W (furthest on top).
#[derive(Clone)]
struct MaxCand {
    distance: f32,
    id: Bytes,
}

impl MaxCand {
    fn new(distance: f32, id: Bytes) -> Self {
        Self { distance, id }
    }

    fn dist(&self) -> f32 {
        self.distance
    }
}

impl PartialEq for MaxCand {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.id == other.id
    }
}
impl Eq for MaxCand {}

impl Ord for MaxCand {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_f32(self.distance, other.distance).then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for MaxCand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search.
///
/// **Batch CQ:** query search walks neighbor edges (SEARCH-LAYER) with candidate list
/// size `ef_search`. Insert still assigns all nodes to **layer 0** only (multi-layer
/// assignment is simplified); edges on that layer are real and used at query time.
///
/// **Batch CS:** `remove` unlinks reverse edges and clears the layer entry; insert uses
/// layer-0 `M_max ≈ 2M` and force-keeps at least one reverse edge so new nodes stay
/// reachable from `entry_point` at insert time; existing-id `add` rewires
/// (remove + re-insert).
///
/// **Batch CT/CU:** on hard-delete, former neighbors are snapshotted as an
/// **undirected** adjacency (outgoing ∪ reverse scan), then reconnected with a
/// spanning structure among survivors (full clique when degree fits, else a
/// nearest-neighbor path) and force-keep of those tree/clique edges on both
/// endpoints. Fixes bidirectional 2-chains, asymmetric incoming-only links, and
/// multi-way stars under degree caps. Not a global non-partition guarantee under
/// arbitrary later hub churn. Force-keep on insert remains insert-time only.
/// This is approximate ANN, not a full RedisSearch HNSW port.
#[derive(Debug)]
pub struct HNSWIndex {
    /// Vectors stored in the index
    vectors: HashMap<Bytes, Vec<f32>>,
    /// HNSW graph layers (layer 0 is the base layer with all nodes)
    layers: Vec<HNSWLayer>,
    /// Maximum number of connections per node (M parameter)
    m: usize,
    /// Size of the dynamic candidate list during construction (ef_construction)
    ef_construction: usize,
    /// Size of the dynamic candidate list during search (ef_search)
    ef_search: usize,
    /// Distance metric
    distance_metric: DistanceMetric,
    /// Entry point (node ID) for search
    entry_point: Option<Bytes>,
}

impl HNSWIndex {
    pub fn new(m: usize, ef_construction: usize, distance_metric: DistanceMetric) -> Self {
        Self {
            vectors: HashMap::new(),
            layers: vec![HNSWLayer::new()],
            m: m.max(1),
            ef_construction: ef_construction.max(1),
            ef_search: ef_construction.max(1),
            distance_metric,
            entry_point: None,
        }
    }

    /// Max outgoing edges kept after prune. Layer 0 uses `≈ 2M` (classic HNSW
    /// `M_max0`) so reverse links survive better under degree caps.
    fn max_edges(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m.saturating_mul(2).max(self.m)
        } else {
            self.m
        }
    }

    /// Add a vector to the index.
    ///
    /// Neighbors are selected via graph search **before** the vector is stored, so the
    /// new node is never chosen as its own neighbor. Existing IDs rewire the graph
    /// (unlink old edges, re-select neighbors for the new vector).
    pub fn add(&mut self, doc_id: Bytes, vector: Vec<f32>) {
        // Update-in-place: full graph rewire so queries near the new location work.
        if self.vectors.contains_key(&doc_id) {
            self.remove(&doc_id);
        }

        // Simplified multi-layer: all nodes live on layer 0.
        let layer = 0;
        while self.layers.len() <= layer {
            self.layers.push(HNSWLayer::new());
        }

        // First node becomes the entry point; no edges yet.
        if self.entry_point.is_none() || self.vectors.is_empty() {
            self.layers[layer].add_node(doc_id.clone());
            self.vectors.insert(doc_id.clone(), vector);
            self.entry_point = Some(doc_id);
            return;
        }

        // Find neighbors via graph walk *before* inserting self into `vectors`.
        let entry = self
            .entry_point
            .clone()
            .expect("entry_point set when index non-empty");
        let ef = self.ef_construction.max(self.m);
        let candidates = self.search_layer(&vector, &entry, ef, layer);
        let neighbors = Self::select_top_m(candidates, self.m);

        // Insert vector + node (empty neighbor list — no stale revive), then edges.
        self.vectors.insert(doc_id.clone(), vector);
        self.layers[layer].add_node(doc_id.clone());

        let max_m = self.max_edges(layer);
        for neighbor in &neighbors {
            debug_assert_ne!(neighbor, &doc_id, "neighbor selection must exclude self");
            self.layers[layer].add_edge(doc_id.clone(), neighbor.clone());
            self.layers[layer].add_edge(neighbor.clone(), doc_id.clone());
            // Cap degree on the existing neighbor; force-keep reverse edge to the
            // new node so it remains reachable from entry via outgoing walks.
            self.prune_neighbors_keeping(neighbor, layer, max_m, std::slice::from_ref(&doc_id));
        }
        self.prune_neighbors_keeping(&doc_id, layer, max_m, &[]);
    }

    /// Remove a vector and fully unlink it from the HNSW graph.
    ///
    /// Strips reverse edges, removes the node from every layer map, reconnects
    /// former neighbors so a bridge/cut-vertex delete does not permanently
    /// partition the layer (Batch CT/CU), and reassigns `entry_point` when needed
    /// (prefer a remaining node that still has edges).
    pub fn remove(&mut self, doc_id: &Bytes) {
        if !self.vectors.contains_key(doc_id) {
            // Still scrub any orphaned layer residue (defensive).
            for layer in &mut self.layers {
                layer.unlink_node(doc_id);
            }
            if self.entry_point.as_ref() == Some(doc_id) {
                self.entry_point = self.pick_entry_point();
            }
            return;
        }

        // Undirected former-neighbor snapshot per layer (Batch CU):
        // outgoing neighbors of deleted ∪ nodes that list deleted as a neighbor.
        // Outgoing-only missed asymmetric predecessors and left them unrepaired.
        let former_by_layer: Vec<Vec<Bytes>> = self
            .layers
            .iter()
            .map(|layer| {
                let mut seen: HashSet<Bytes> = HashSet::new();
                let mut former: Vec<Bytes> = Vec::new();
                for n in layer.get_neighbors(doc_id) {
                    if n != *doc_id && self.vectors.contains_key(&n) && seen.insert(n.clone()) {
                        former.push(n);
                    }
                }
                for (id, neighs) in &layer.neighbors {
                    if id == doc_id {
                        continue;
                    }
                    if !neighs.iter().any(|n| n == doc_id) {
                        continue;
                    }
                    if self.vectors.contains_key(id) && seen.insert(id.clone()) {
                        former.push(id.clone());
                    }
                }
                former
            })
            .collect();

        self.vectors.remove(doc_id);
        for layer in &mut self.layers {
            layer.unlink_node(doc_id);
        }

        // Reconnect survivors that used the deleted id as a bridge.
        for (layer_idx, former) in former_by_layer.iter().enumerate() {
            self.bridge_reconnect_neighbors(former, layer_idx);
        }

        if self.entry_point.as_ref() == Some(doc_id) {
            self.entry_point = self.pick_entry_point();
        }
    }

    /// After unlinking a deleted id, reconnect its former neighbors so they are
    /// not left isolated from each other when the deleted node was a cut vertex.
    ///
    /// Heuristic (Batch CU): build a **spanning** structure among survivors still
    /// in `vectors` — full bidirectional clique when `n-1 ≤ max_edges(layer)`,
    /// otherwise a nearest-neighbor path (degree ≤ 2). Force-keep those spanning
    /// edges on **both** endpoints when pruning so multi-way hubs stay directed-
    /// reachable from an entry-adjacent survivor even under degree saturation.
    /// Extra closest-peer edges may be added for density but only spanning edges
    /// are force-kept.
    fn bridge_reconnect_neighbors(&mut self, former: &[Bytes], layer: usize) {
        // Dedup while preserving first-seen order.
        let mut seen: HashSet<Bytes> = HashSet::new();
        let survivors: Vec<Bytes> = former
            .iter()
            .filter(|id| self.vectors.contains_key(id.as_ref()))
            .filter(|id| seen.insert((*id).clone()))
            .cloned()
            .collect();

        if survivors.len() < 2 {
            return;
        }

        let max_m = self.max_edges(layer);
        let n = survivors.len();

        // Spanning force-keep set per survivor (tree/clique neighbors).
        let mut force_keep: HashMap<Bytes, Vec<Bytes>> = HashMap::new();
        for s in &survivors {
            force_keep.insert(s.clone(), Vec::new());
        }

        let push_keep = |fk: &mut HashMap<Bytes, Vec<Bytes>>, u: &Bytes, v: &Bytes| {
            if let Some(list) = fk.get_mut(u) {
                if !list.iter().any(|x| x == v) {
                    list.push(v.clone());
                }
            }
        };

        // Full clique when every survivor can hold edges to all others under max_m.
        if n - 1 <= max_m {
            for i in 0..n {
                for j in (i + 1)..n {
                    let u = &survivors[i];
                    let v = &survivors[j];
                    if let Some(l) = self.layers.get_mut(layer) {
                        l.add_edge(u.clone(), v.clone());
                        l.add_edge(v.clone(), u.clone());
                    }
                    push_keep(&mut force_keep, u, v);
                    push_keep(&mut force_keep, v, u);
                }
            }
        } else {
            // Nearest-neighbor path among survivors (max degree 2) so force-keep
            // fits under small max_m; then add denser closest-peer edges as bonus.
            let mut remaining: Vec<Bytes> = survivors.clone();
            let mut path: Vec<Bytes> = Vec::with_capacity(n);
            path.push(remaining.remove(0));
            while !remaining.is_empty() {
                let last = path.last().expect("path non-empty").clone();
                let Some(last_vec) = self.vectors.get(&last).cloned() else {
                    path.push(remaining.remove(0));
                    continue;
                };
                let mut best_idx = 0usize;
                let mut best_dist = f32::MAX;
                for (idx, cand) in remaining.iter().enumerate() {
                    if let Some(cv) = self.vectors.get(cand) {
                        let d = self.compute_distance(&last_vec, cv);
                        if d < best_dist {
                            best_dist = d;
                            best_idx = idx;
                        }
                    }
                }
                let next = remaining.remove(best_idx);
                let prev = path.last().expect("path non-empty").clone();
                if let Some(l) = self.layers.get_mut(layer) {
                    l.add_edge(prev.clone(), next.clone());
                    l.add_edge(next.clone(), prev.clone());
                }
                push_keep(&mut force_keep, &prev, &next);
                push_keep(&mut force_keep, &next, &prev);
                path.push(next);
            }

            // Bonus density: each survivor ↔ up to max_m closest other survivors.
            for u in &survivors {
                let Some(u_vec) = self.vectors.get(u).cloned() else {
                    continue;
                };
                let mut peers: Vec<(Bytes, f32)> = survivors
                    .iter()
                    .filter(|v| *v != u)
                    .filter_map(|v| {
                        self.vectors
                            .get(v)
                            .map(|vv| (v.clone(), self.compute_distance(&u_vec, vv)))
                    })
                    .collect();
                peers.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
                let connect_to = peers.len().min(max_m);
                for (peer, _) in peers.into_iter().take(connect_to) {
                    if let Some(l) = self.layers.get_mut(layer) {
                        l.add_edge(u.clone(), peer.clone());
                        l.add_edge(peer, u.clone());
                    }
                }
            }
        }

        // Cap degree; force-keep spanning edges on both endpoints.
        for u in &survivors {
            let keep = force_keep.get(u).map(|v| v.as_slice()).unwrap_or(&[]);
            self.prune_neighbors_keeping(u, layer, max_m, keep);
        }
    }

    /// Choose a new entry point among remaining vectors; prefer one with edges.
    fn pick_entry_point(&self) -> Option<Bytes> {
        if self.vectors.is_empty() {
            return None;
        }
        let layer0 = self.layers.first();
        let mut fallback: Option<Bytes> = None;
        for id in self.vectors.keys() {
            let has_edges = layer0
                .map(|l| !l.get_neighbors(id).is_empty())
                .unwrap_or(false);
            if has_edges {
                return Some(id.clone());
            }
            if fallback.is_none() {
                fallback = Some(id.clone());
            }
        }
        fallback
    }

    /// Search for k nearest neighbors by walking the HNSW graph (layer 0).
    ///
    /// Uses `ef_search` (at least `k`) as the dynamic candidate list size. This is
    /// approximate: only nodes reachable via edges from the entry point are considered.
    /// Fallback: if the entry point is missing from `vectors`, brute-force the map
    /// (should not happen after normal `add` paths).
    pub fn search(&self, query_vector: &[f32], k: usize) -> Vec<VectorSearchResult> {
        if self.vectors.is_empty() || k == 0 {
            return Vec::new();
        }
        let Some(entry) = self.entry_point.as_ref() else {
            return Vec::new();
        };

        let ef = self.ef_search.max(k);
        let layer = 0;
        let candidates = self.search_layer(query_vector, entry, ef, layer);

        candidates
            .into_iter()
            .take(k)
            .filter_map(|(doc_id, distance)| {
                self.vectors.get(&doc_id).map(|vector| VectorSearchResult {
                    doc_id,
                    score: self.distance_to_score(distance),
                    vector: vector.clone(),
                })
            })
            .collect()
    }

    /// SEARCH-LAYER (Malkov & Yashunin): greedy expansion of neighbors with ef bound.
    ///
    /// Returns up to `ef` nearest visited nodes by distance (closest first).
    /// Does **not** scan the full `vectors` map; only follows graph edges.
    ///
    /// If `entry` is absent from `vectors`, falls back to brute-force top-`ef`
    /// (defensive; normal indexes always keep entry in `vectors`).
    fn search_layer(
        &self,
        query: &[f32],
        entry: &Bytes,
        ef: usize,
        layer: usize,
    ) -> Vec<(Bytes, f32)> {
        if ef == 0 {
            return Vec::new();
        }

        let Some(entry_vec) = self.vectors.get(entry) else {
            return self.brute_force_top(query, ef);
        };

        let mut visited: HashSet<Bytes> = HashSet::new();
        visited.insert(entry.clone());

        let d0 = self.compute_distance(query, entry_vec);
        let mut candidates: BinaryHeap<MinCand> = BinaryHeap::new();
        let mut w: BinaryHeap<MaxCand> = BinaryHeap::new();
        candidates.push(MinCand::new(d0, entry.clone()));
        w.push(MaxCand::new(d0, entry.clone()));

        while let Some(current) = candidates.pop() {
            let furthest = w.peek().map(|c| c.dist()).unwrap_or(f32::MAX);
            if current.dist() > furthest {
                break;
            }

            let neighbors = self
                .layers
                .get(layer)
                .map(|l| l.get_neighbors(&current.id))
                .unwrap_or_default();

            for neighbor in neighbors {
                if !visited.insert(neighbor.clone()) {
                    continue;
                }
                let Some(nvec) = self.vectors.get(&neighbor) else {
                    continue;
                };
                let dist = self.compute_distance(query, nvec);
                let furthest = w.peek().map(|c| c.dist()).unwrap_or(f32::MAX);
                if dist < furthest || w.len() < ef {
                    candidates.push(MinCand::new(dist, neighbor.clone()));
                    w.push(MaxCand::new(dist, neighbor));
                    if w.len() > ef {
                        w.pop();
                    }
                }
            }
        }

        let mut results: Vec<(Bytes, f32)> = w
            .into_iter()
            .map(|c| {
                let dist = c.distance;
                (c.id, dist)
            })
            .collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        results
    }

    /// Defensive full scan used only when the entry point vector is missing.
    fn brute_force_top(&self, query: &[f32], ef: usize) -> Vec<(Bytes, f32)> {
        let mut candidates: Vec<(Bytes, f32)> = self
            .vectors
            .iter()
            .map(|(id, v)| (id.clone(), self.compute_distance(query, v)))
            .collect();
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        candidates.truncate(ef);
        candidates
    }

    fn select_top_m(candidates: Vec<(Bytes, f32)>, m: usize) -> Vec<Bytes> {
        candidates.into_iter().take(m).map(|(id, _)| id).collect()
    }

    /// Keep at most `max_m` nearest neighbors of `node_id` (by distance to that node).
    ///
    /// **Must-keep policy (Batch CW):** ids in `must_keep` that still exist in
    /// `vectors` are prioritized. If `|must_keep| > max_m`, only the closest
    /// `max_m` must-edges (by distance to `node_id`) are retained; farther
    /// required ids are dropped *before* the force-keep loop. Force-keep never
    /// pops an id that is still in that capped required set — only non-required
    /// edges are evicted to make room. Used so reverse edges `neighbor → new`
    /// survive insert prune, and so bridge-repair spanning edges survive on both
    /// endpoints (Batch CU multi-way).
    fn prune_neighbors_keeping(
        &mut self,
        node_id: &Bytes,
        layer: usize,
        max_m: usize,
        must_keep: &[Bytes],
    ) {
        let Some(layer_ref) = self.layers.get(layer) else {
            return;
        };
        let neighbors = layer_ref.get_neighbors(node_id);
        let Some(node_vec) = self.vectors.get(node_id).cloned() else {
            return;
        };

        if max_m == 0 {
            if let Some(layer_mut) = self.layers.get_mut(layer) {
                layer_mut.set_neighbors(node_id.clone(), Vec::new());
            }
            return;
        }

        let mut scored: Vec<(Bytes, f32)> = neighbors
            .into_iter()
            .filter(|n| n != node_id)
            .filter_map(|n| {
                self.vectors
                    .get(&n)
                    .map(|v| (n, self.compute_distance(&node_vec, v)))
            })
            .collect();

        // Ensure must_keep ids are present even if somehow missing from the list.
        for keep in must_keep {
            if keep != node_id
                && self.vectors.contains_key(keep)
                && !scored.iter().any(|(id, _)| id == keep)
            {
                if let Some(v) = self.vectors.get(keep) {
                    let dist = self.compute_distance(&node_vec, v);
                    scored.push((keep.clone(), dist));
                }
            }
        }

        // Cap must-keep to max_m (closest by distance to node) so force-keep
        // never needs to pop a still-required edge when the set is oversized.
        let mut must_scored: Vec<(Bytes, f32)> = Vec::new();
        let mut must_seen: HashSet<Bytes> = HashSet::new();
        for keep in must_keep {
            if keep == node_id || !self.vectors.contains_key(keep) {
                continue;
            }
            if !must_seen.insert(keep.clone()) {
                continue;
            }
            let dist = scored
                .iter()
                .find(|(id, _)| id == keep)
                .map(|(_, d)| *d)
                .unwrap_or_else(|| {
                    self.vectors
                        .get(keep)
                        .map(|v| self.compute_distance(&node_vec, v))
                        .unwrap_or(f32::MAX)
                });
            must_scored.push((keep.clone(), dist));
        }
        if must_scored.len() > max_m {
            must_scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            must_scored.truncate(max_m);
        }
        let must: Vec<Bytes> = must_scored.into_iter().map(|(id, _)| id).collect();
        let is_must = |id: &Bytes| must.iter().any(|m| m == id);

        if scored.len() <= max_m {
            // Still rewrite if we had to re-add must_keep or clean self-loops.
            if let Some(layer_mut) = self.layers.get_mut(layer) {
                let kept: Vec<Bytes> = scored.into_iter().map(|(id, _)| id).collect();
                layer_mut.set_neighbors(node_id.clone(), kept);
            }
            return;
        }

        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        // Dedup by id (keep first = closest)
        let mut seen = HashSet::new();
        scored.retain(|(id, _)| seen.insert(id.clone()));

        let mut kept: Vec<Bytes> = scored
            .iter()
            .take(max_m)
            .map(|(id, _)| id.clone())
            .collect();

        // Force-keep each required neighbor, evicting furthest non-required first.
        // Never pop a still-required id (must already capped to max_m above).
        for keep in &must {
            if kept.iter().any(|id| id == keep) {
                continue;
            }
            if kept.len() >= max_m {
                if let Some(pos) = kept.iter().rposition(|id| !is_must(id)) {
                    kept.remove(pos);
                } else {
                    // Capacity filled with required edges; skip rather than
                    // drop a still-required id (Batch CW).
                    continue;
                }
            }
            kept.push(keep.clone());
        }

        if let Some(layer_mut) = self.layers.get_mut(layer) {
            layer_mut.set_neighbors(node_id.clone(), kept);
        }
    }

    /// Compute distance between two vectors
    fn compute_distance(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        match &self.distance_metric {
            DistanceMetric::Cosine => {
                // Cosine distance = 1 - cosine similarity
                1.0 - self.cosine_similarity(vec1, vec2)
            }
            DistanceMetric::L2 => self.l2_distance(vec1, vec2),
            DistanceMetric::IP => {
                // For IP, we want to maximize, so we use negative
                -self.inner_product(vec1, vec2)
            }
        }
    }

    /// Convert distance to score (higher is better)
    fn distance_to_score(&self, distance: f32) -> f32 {
        match &self.distance_metric {
            DistanceMetric::Cosine => 1.0 - distance, // Convert back to similarity
            DistanceMetric::L2 => 1.0 / (1.0 + distance), // Inverse distance
            DistanceMetric::IP => -distance, // Negate back
        }
    }

    fn cosine_similarity(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        let dot_product: f32 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
        let norm1: f32 = vec1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm2: f32 = vec2.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm1 == 0.0 || norm2 == 0.0 {
            return 0.0;
        }

        dot_product / (norm1 * norm2)
    }

    fn l2_distance(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        vec1.iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    fn inner_product(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum()
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// Flat (brute-force) vector index
#[derive(Debug)]
pub struct FlatVectorIndex {
    vectors: HashMap<Bytes, Vec<f32>>,
    distance_metric: DistanceMetric,
}

impl FlatVectorIndex {
    pub fn new(distance_metric: DistanceMetric) -> Self {
        Self {
            vectors: HashMap::new(),
            distance_metric,
        }
    }

    pub fn add(&mut self, doc_id: Bytes, vector: Vec<f32>) {
        self.vectors.insert(doc_id, vector);
    }

    pub fn remove(&mut self, doc_id: &Bytes) {
        self.vectors.remove(doc_id);
    }

    pub fn search(&self, query_vector: &[f32], k: usize) -> Vec<VectorSearchResult> {
        let mut results: Vec<VectorSearchResult> = self
            .vectors
            .iter()
            .map(|(doc_id, vector)| {
                let score = self.compute_similarity(query_vector, vector);
                VectorSearchResult {
                    doc_id: doc_id.clone(),
                    score,
                    vector: vector.clone(),
                }
            })
            .collect();

        // Sort by score (higher is better)
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));

        results.into_iter().take(k).collect()
    }

    fn compute_similarity(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        match &self.distance_metric {
            DistanceMetric::Cosine => self.cosine_similarity(vec1, vec2),
            DistanceMetric::L2 => {
                let distance = self.l2_distance(vec1, vec2);
                1.0 / (1.0 + distance)
            }
            DistanceMetric::IP => self.inner_product(vec1, vec2),
        }
    }

    fn cosine_similarity(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        let dot_product: f32 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
        let norm1: f32 = vec1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm2: f32 = vec2.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm1 == 0.0 || norm2 == 0.0 {
            return 0.0;
        }

        dot_product / (norm1 * norm2)
    }

    fn l2_distance(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        vec1.iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    fn inner_product(&self, vec1: &[f32], vec2: &[f32]) -> f32 {
        vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum()
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// Vector index wrapper that can use different algorithms
#[derive(Debug)]
pub enum VectorIndex {
    Flat(FlatVectorIndex),
    HNSW(HNSWIndex),
}

impl VectorIndex {
    pub fn new_flat(distance_metric: DistanceMetric) -> Self {
        VectorIndex::Flat(FlatVectorIndex::new(distance_metric))
    }

    pub fn new_hnsw(m: usize, ef_construction: usize, distance_metric: DistanceMetric) -> Self {
        VectorIndex::HNSW(HNSWIndex::new(m, ef_construction, distance_metric))
    }

    pub fn add(&mut self, doc_id: Bytes, vector: Vec<f32>) {
        match self {
            VectorIndex::Flat(index) => index.add(doc_id, vector),
            VectorIndex::HNSW(index) => index.add(doc_id, vector),
        }
    }

    pub fn remove(&mut self, doc_id: &Bytes) {
        match self {
            VectorIndex::Flat(index) => index.remove(doc_id),
            VectorIndex::HNSW(index) => index.remove(doc_id),
        }
    }

    pub fn search(&self, query_vector: &[f32], k: usize) -> Vec<VectorSearchResult> {
        match self {
            VectorIndex::Flat(index) => index.search(query_vector, k),
            VectorIndex::HNSW(index) => index.search(query_vector, k),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            VectorIndex::Flat(index) => index.len(),
            VectorIndex::HNSW(index) => index.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            VectorIndex::Flat(index) => index.is_empty(),
            VectorIndex::HNSW(index) => index.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flat_index_cosine() {
        let mut index = FlatVectorIndex::new(DistanceMetric::Cosine);

        index.add(Bytes::from("doc1"), vec![1.0, 0.0, 0.0]);
        index.add(Bytes::from("doc2"), vec![0.0, 1.0, 0.0]);
        index.add(Bytes::from("doc3"), vec![1.0, 1.0, 0.0]);

        let results = index.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, Bytes::from("doc1"));
    }

    #[test]
    fn test_flat_index_l2() {
        let mut index = FlatVectorIndex::new(DistanceMetric::L2);

        index.add(Bytes::from("doc1"), vec![0.0, 0.0]);
        index.add(Bytes::from("doc2"), vec![3.0, 4.0]);
        index.add(Bytes::from("doc3"), vec![1.0, 1.0]);

        let results = index.search(&[0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, Bytes::from("doc1"));
        assert_eq!(results[1].doc_id, Bytes::from("doc3"));
    }

    #[test]
    fn test_hnsw_index() {
        let mut index = HNSWIndex::new(16, 200, DistanceMetric::Cosine);

        index.add(Bytes::from("doc1"), vec![1.0, 0.0, 0.0]);
        index.add(Bytes::from("doc2"), vec![0.0, 1.0, 0.0]);
        index.add(Bytes::from("doc3"), vec![0.9, 0.1, 0.0]);

        let results = index.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        // The closest should be doc1
        assert_eq!(results[0].doc_id, Bytes::from("doc1"));
    }

    /// Small fixed set: HNSW top-1 should match FLAT exact top-1 (correctness
    /// check under graph search; not a throughput benchmark — see `docs/benchmarks.md`).
    #[test]
    fn hnsw_top1_matches_flat_on_small_set() {
        let vectors: Vec<(&str, Vec<f32>)> = vec![
            ("a", vec![1.0, 0.0, 0.0, 0.0]),
            ("b", vec![0.0, 1.0, 0.0, 0.0]),
            ("c", vec![0.0, 0.0, 1.0, 0.0]),
            ("d", vec![0.7, 0.7, 0.0, 0.0]),
            ("e", vec![0.9, 0.1, 0.0, 0.0]),
            ("f", vec![0.1, 0.9, 0.0, 0.0]),
            ("g", vec![0.0, 0.0, 0.9, 0.1]),
            ("h", vec![0.5, 0.5, 0.5, 0.5]),
        ];
        let query = [1.0f32, 0.05, 0.0, 0.0];

        let mut flat = FlatVectorIndex::new(DistanceMetric::Cosine);
        let mut hnsw = HNSWIndex::new(8, 64, DistanceMetric::Cosine);
        for (id, v) in &vectors {
            flat.add(Bytes::from(*id), v.clone());
            hnsw.add(Bytes::from(*id), v.clone());
        }

        let flat_top = flat.search(&query, 1);
        let hnsw_top = hnsw.search(&query, 1);
        assert_eq!(flat_top.len(), 1);
        assert_eq!(hnsw_top.len(), 1);
        assert_eq!(
            flat_top[0].doc_id, hnsw_top[0].doc_id,
            "HNSW top-1 should match FLAT exact top-1 on tiny set (flat={:?} hnsw={:?})",
            flat_top[0].doc_id,
            hnsw_top[0].doc_id
        );
    }

    /// Inserts must never wire a node as its own neighbor (Batch CQ).
    #[test]
    fn hnsw_add_excludes_self_from_neighbors() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        for i in 0..12 {
            index.add(Bytes::from(format!("d{i}")), vec![i as f32, 0.0]);
        }
        assert!(!index.layers.is_empty());
        for (id, neighs) in &index.layers[0].neighbors {
            assert!(
                !neighs.iter().any(|n| n == id),
                "self-loop on neighbor list for {:?}",
                id
            );
        }
    }

    /// Crafted connectivity: an isolated closer vector must not win top-1 if search
    /// only walks edges. Full-scan (pre-CQ) would return the isolated point.
    #[test]
    fn hnsw_search_follows_edges_not_full_scan() {
        let mut index = HNSWIndex::new(8, 64, DistanceMetric::L2);
        index.add(Bytes::from("entry"), vec![0.0, 0.0]);
        index.add(Bytes::from("near"), vec![1.0, 0.0]);
        index.add(Bytes::from("mid"), vec![10.0, 0.0]);
        index.add(Bytes::from("far_isolated"), vec![0.1, 0.0]);

        // Rebuild edges: entry -- mid -- near; leave far_isolated disconnected.
        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        layer.add_edge(Bytes::from("entry"), Bytes::from("mid"));
        layer.add_edge(Bytes::from("mid"), Bytes::from("entry"));
        layer.add_edge(Bytes::from("mid"), Bytes::from("near"));
        layer.add_edge(Bytes::from("near"), Bytes::from("mid"));
        // far_isolated intentionally has no edges

        index.entry_point = Some(Bytes::from("entry"));

        // Query sits on far_isolated; FLAT would rank it #1.
        let query = [0.1f32, 0.0];
        let mut flat = FlatVectorIndex::new(DistanceMetric::L2);
        for (id, v) in [
            ("entry", vec![0.0, 0.0]),
            ("near", vec![1.0, 0.0]),
            ("mid", vec![10.0, 0.0]),
            ("far_isolated", vec![0.1, 0.0]),
        ] {
            flat.add(Bytes::from(id), v);
        }
        let flat_top = flat.search(&query, 1);
        assert_eq!(
            flat_top[0].doc_id,
            Bytes::from("far_isolated"),
            "sanity: FLAT must prefer the isolated closer point"
        );

        let hnsw_top = index.search(&query, 1);
        assert_eq!(hnsw_top.len(), 1);
        assert_ne!(
            hnsw_top[0].doc_id,
            Bytes::from("far_isolated"),
            "graph search must not return a disconnected node (would indicate full-scan)"
        );
        // Closest among reachable {entry, mid, near} to [0.1, 0] is entry.
        assert_eq!(hnsw_top[0].doc_id, Bytes::from("entry"));

        // Reachable set size via graph walk should be 3, not 4.
        let layer_results = index.search_layer(&query, &Bytes::from("entry"), 16, 0);
        let ids: HashSet<_> = layer_results.iter().map(|(id, _)| id.clone()).collect();
        assert!(ids.contains(&Bytes::from("entry")));
        assert!(ids.contains(&Bytes::from("mid")));
        assert!(ids.contains(&Bytes::from("near")));
        assert!(
            !ids.contains(&Bytes::from("far_isolated")),
            "search_layer must not visit disconnected nodes"
        );
        assert_eq!(ids.len(), 3);
    }

    /// Normal insert builds a connected layer-0 graph; top-1 still works with edges.
    #[test]
    fn hnsw_graph_has_edges_after_inserts() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        for i in 0..8 {
            index.add(Bytes::from(format!("n{i}")), vec![i as f32, (i % 3) as f32]);
        }
        let total_edges: usize = index.layers[0]
            .neighbors
            .values()
            .map(|v| v.len())
            .sum();
        assert!(
            total_edges > 0,
            "expected bidirectional edges after multi-node insert"
        );
        // Every non-entry node should have at least one neighbor after insert.
        for (id, neighs) in &index.layers[0].neighbors {
            if Some(id) != index.entry_point.as_ref() {
                assert!(
                    !neighs.is_empty(),
                    "node {:?} should be connected",
                    id
                );
            }
        }
    }

    /// Batch CS: remove unlinks reverse edges and drops the layer entry.
    #[test]
    fn hnsw_remove_middle_unlinks_graph() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        index.add(Bytes::from("a"), vec![0.0, 0.0]);
        index.add(Bytes::from("b"), vec![1.0, 0.0]);
        index.add(Bytes::from("c"), vec![2.0, 0.0]);

        // Force a chain a <-> b <-> c so b is a bridge.
        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        layer.add_edge(Bytes::from("a"), Bytes::from("b"));
        layer.add_edge(Bytes::from("b"), Bytes::from("a"));
        layer.add_edge(Bytes::from("b"), Bytes::from("c"));
        layer.add_edge(Bytes::from("c"), Bytes::from("b"));
        index.entry_point = Some(Bytes::from("a"));

        index.remove(&Bytes::from("b"));

        assert!(!index.vectors.contains_key(&Bytes::from("b")));
        assert!(
            !index.layers[0].neighbors.contains_key(&Bytes::from("b")),
            "removed id must leave the layer map"
        );
        for (id, neighs) in &index.layers[0].neighbors {
            assert!(
                !neighs.iter().any(|n| n == &Bytes::from("b")),
                "stale reverse edge to b from {:?}",
                id
            );
        }
        // Survivors still present.
        assert!(index.vectors.contains_key(&Bytes::from("a")));
        assert!(index.vectors.contains_key(&Bytes::from("c")));
    }

    /// Batch CS: removing the entry point reassigns to a remaining vector.
    #[test]
    fn hnsw_remove_entry_reassigns() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        index.add(Bytes::from("entry"), vec![0.0, 0.0]);
        index.add(Bytes::from("other"), vec![1.0, 0.0]);
        index.add(Bytes::from("third"), vec![2.0, 0.0]);
        assert_eq!(index.entry_point, Some(Bytes::from("entry")));

        index.remove(&Bytes::from("entry"));

        let ep = index
            .entry_point
            .clone()
            .expect("entry_point must be reassigned");
        assert_ne!(ep, Bytes::from("entry"));
        assert!(
            index.vectors.contains_key(&ep),
            "new entry_point must still be in vectors"
        );
        assert!(!index.vectors.contains_key(&Bytes::from("entry")));
        assert!(
            !index.layers[0].neighbors.contains_key(&Bytes::from("entry")),
            "old entry must leave layer map"
        );

        // Empty index clears entry.
        index.remove(&Bytes::from("other"));
        index.remove(&Bytes::from("third"));
        assert!(index.entry_point.is_none());
        assert!(index.vectors.is_empty());
    }

    /// Batch CS: remove + re-add same id must not revive stale neighbors.
    #[test]
    fn hnsw_remove_readd_clears_stale_neighbors() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        index.add(Bytes::from("a"), vec![0.0, 0.0]);
        index.add(Bytes::from("b"), vec![1.0, 0.0]);
        index.add(Bytes::from("c"), vec![2.0, 0.0]);

        // Plant a stale neighbor list for "ghost" that is not in vectors.
        // Simulate pre-CS remove that left layer residue, then re-add.
        index.layers[0].set_neighbors(
            Bytes::from("ghost"),
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("stale")],
        );
        // Pre-condition: or_insert-style add_node would keep these.
        assert_eq!(
            index.layers[0].get_neighbors(&Bytes::from("ghost")).len(),
            3
        );

        // Real remove path (ghost not in vectors) still scrubs residue.
        index.remove(&Bytes::from("ghost"));
        assert!(!index.layers[0].neighbors.contains_key(&Bytes::from("ghost")));

        // Re-add ghost: neighbor list must start empty then get fresh edges.
        index.add(Bytes::from("ghost"), vec![10.0, 0.0]);
        let neighs = index.layers[0].get_neighbors(&Bytes::from("ghost"));
        assert!(
            !neighs.iter().any(|n| n == &Bytes::from("stale")),
            "stale neighbor must not revive on re-add: {:?}",
            neighs
        );
        // Fresh insert should connect to real neighbors only.
        for n in &neighs {
            assert!(
                index.vectors.contains_key(n),
                "neighbor {:?} must exist in vectors",
                n
            );
        }
        // And no reverse edge should point at a non-existent id.
        for (id, ns) in &index.layers[0].neighbors {
            for n in ns {
                assert!(
                    index.vectors.contains_key(n),
                    "edge {:?} → {:?} targets missing vector",
                    id,
                    n
                );
            }
        }
    }

    /// Batch CS: small M + many inserts — every id reachable from entry.
    #[test]
    fn hnsw_insert_preserves_reachability_from_entry() {
        // Small M stresses prune; M_max0=2M + force-keep reverse should still
        // leave every node reachable via outgoing walks from entry.
        let mut index = HNSWIndex::new(2, 16, DistanceMetric::L2);
        let n = 40;
        for i in 0..n {
            index.add(
                Bytes::from(format!("n{i}")),
                vec![i as f32, (i % 7) as f32],
            );
        }
        assert_eq!(index.len(), n);

        let entry = index
            .entry_point
            .clone()
            .expect("non-empty index has entry_point");

        // BFS over outgoing edges from entry must visit every vector id.
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut stack = vec![entry.clone()];
        seen.insert(entry);
        while let Some(cur) = stack.pop() {
            for nb in index.layers[0].get_neighbors(&cur) {
                if seen.insert(nb.clone()) {
                    stack.push(nb);
                }
            }
        }
        for id in index.vectors.keys() {
            assert!(
                seen.contains(id),
                "node {:?} unreachable from entry via outgoing edges (visited {})",
                id,
                seen.len()
            );
        }

        // search(self-vector, k=1) returns self for every id (distance 0).
        let snapshot: Vec<(Bytes, Vec<f32>)> = index
            .vectors
            .iter()
            .map(|(id, v)| (id.clone(), v.clone()))
            .collect();
        for (id, vec) in snapshot {
            let results = index.search(&vec, 1);
            assert_eq!(
                results.len(),
                1,
                "search must return a hit for {:?}",
                id
            );
            assert_eq!(
                results[0].doc_id, id,
                "self-search must return self (got {:?}); indicates unreachability",
                results[0].doc_id
            );
        }
    }

    /// Batch CS: existing-id add rewires so a large move is findable near the new loc.
    #[test]
    fn hnsw_update_rewires_graph() {
        let mut index = HNSWIndex::new(3, 8, DistanceMetric::L2);
        // Near-origin cluster.
        for i in 0..15 {
            index.add(
                Bytes::from(format!("near{i}")),
                vec![i as f32 * 0.1, 0.0],
            );
        }
        // Far cluster around x=100.
        for i in 0..15 {
            index.add(
                Bytes::from(format!("far{i}")),
                vec![100.0 + i as f32 * 0.1, 0.0],
            );
        }
        // Target starts near origin, then jumps to the far cluster.
        index.add(Bytes::from("target"), vec![0.5, 0.0]);
        index.add(Bytes::from("target"), vec![100.5, 0.0]);

        assert_eq!(
            index.vectors.get(&Bytes::from("target")),
            Some(&vec![100.5, 0.0]),
            "vector value must update"
        );

        // Query at the new location: rewired graph should rank target #1.
        let results = index.search(&[100.5f32, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].doc_id,
            Bytes::from("target"),
            "after large move, self-query at new location must find target"
        );

        // Target should have at least one neighbor still in vectors (rewired, not orphaned).
        let t_neighs = index.layers[0].get_neighbors(&Bytes::from("target"));
        assert!(
            !t_neighs.is_empty(),
            "updated node should be re-connected"
        );
        // At least one reverse edge into target (reachable from someone).
        let reverse: usize = index.layers[0]
            .neighbors
            .iter()
            .filter(|(id, ns)| {
                *id != &Bytes::from("target") && ns.iter().any(|n| n == &Bytes::from("target"))
            })
            .count();
        assert!(
            reverse > 0,
            "at least one reverse edge must point at the rewired node"
        );
    }

    /// Batch CT: removing a bridge reconnects former neighbors so survivors stay
    /// BFS-reachable / searchable from entry (fails on pre-CT hard-unlink-only).
    #[test]
    fn hnsw_bridge_remove_keeps_survivors_reachable() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        index.add(Bytes::from("a"), vec![0.0, 0.0]);
        index.add(Bytes::from("b"), vec![1.0, 0.0]);
        index.add(Bytes::from("c"), vec![2.0, 0.0]);

        // Force chain a <-> b <-> c so b is a cut vertex.
        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        layer.add_edge(Bytes::from("a"), Bytes::from("b"));
        layer.add_edge(Bytes::from("b"), Bytes::from("a"));
        layer.add_edge(Bytes::from("b"), Bytes::from("c"));
        layer.add_edge(Bytes::from("c"), Bytes::from("b"));
        index.entry_point = Some(Bytes::from("a"));

        index.remove(&Bytes::from("b"));

        assert!(!index.vectors.contains_key(&Bytes::from("b")));
        assert!(index.vectors.contains_key(&Bytes::from("a")));
        assert!(index.vectors.contains_key(&Bytes::from("c")));

        // Bridge repair must leave a bidirectional path a ↔ c (or via other edges).
        let entry = index
            .entry_point
            .clone()
            .expect("non-empty index has entry_point");
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut stack = vec![entry.clone()];
        seen.insert(entry);
        while let Some(cur) = stack.pop() {
            for nb in index.layers[0].get_neighbors(&cur) {
                if seen.insert(nb.clone()) {
                    stack.push(nb);
                }
            }
        }
        for id in index.vectors.keys() {
            assert!(
                seen.contains(id),
                "after bridge remove, {:?} must be BFS-reachable from entry (visited {:?})",
                id,
                seen
            );
        }

        // Self-search for each survivor must return self (graph walk, not brute-force).
        for (id, vec) in [
            (Bytes::from("a"), vec![0.0f32, 0.0]),
            (Bytes::from("c"), vec![2.0f32, 0.0]),
        ] {
            let results = index.search(&vec, 1);
            assert_eq!(results.len(), 1, "search must hit for {:?}", id);
            assert_eq!(
                results[0].doc_id, id,
                "survivor {:?} must remain searchable after bridge remove (got {:?})",
                id,
                results[0].doc_id
            );
        }
    }

    /// Batch CT: update (remove+reinsert) of a cut vertex must not permanently
    /// orphan the other partition of the forced chain.
    #[test]
    fn hnsw_bridge_update_keeps_ends_reachable() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        index.add(Bytes::from("a"), vec![0.0, 0.0]);
        index.add(Bytes::from("b"), vec![1.0, 0.0]);
        index.add(Bytes::from("c"), vec![2.0, 0.0]);

        // Force chain a <-> b <-> c; entry a. Updating b runs remove+reinsert.
        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        layer.add_edge(Bytes::from("a"), Bytes::from("b"));
        layer.add_edge(Bytes::from("b"), Bytes::from("a"));
        layer.add_edge(Bytes::from("b"), Bytes::from("c"));
        layer.add_edge(Bytes::from("c"), Bytes::from("b"));
        index.entry_point = Some(Bytes::from("a"));

        // Move b far away; remove step would orphan c without bridge repair.
        index.add(Bytes::from("b"), vec![100.0, 0.0]);

        assert_eq!(
            index.vectors.get(&Bytes::from("b")),
            Some(&vec![100.0, 0.0])
        );
        assert!(index.vectors.contains_key(&Bytes::from("a")));
        assert!(index.vectors.contains_key(&Bytes::from("c")));

        let entry = index
            .entry_point
            .clone()
            .expect("non-empty index has entry_point");
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut stack = vec![entry.clone()];
        seen.insert(entry);
        while let Some(cur) = stack.pop() {
            for nb in index.layers[0].get_neighbors(&cur) {
                if seen.insert(nb.clone()) {
                    stack.push(nb);
                }
            }
        }
        for id in [Bytes::from("a"), Bytes::from("b"), Bytes::from("c")] {
            assert!(
                seen.contains(&id),
                "after bridge update, {:?} must be BFS-reachable from entry (visited {:?})",
                id,
                seen
            );
        }

        // Ends remain self-searchable.
        for (id, vec) in [
            (Bytes::from("a"), vec![0.0f32, 0.0]),
            (Bytes::from("c"), vec![2.0f32, 0.0]),
        ] {
            let results = index.search(&vec, 1);
            assert_eq!(results.len(), 1);
            assert_eq!(
                results[0].doc_id, id,
                "end {:?} must stay searchable after updating bridge node",
                id
            );
        }
    }

    /// Batch CU: asymmetric edges — entry only points at the bridge; bridge↔leaf.
    /// Outgoing-only snapshot of `b` would miss `a` and leave `c` unreachable.
    #[test]
    fn hnsw_bridge_remove_asymmetric_incoming_reconnects() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        index.add(Bytes::from("a"), vec![0.0, 0.0]);
        index.add(Bytes::from("b"), vec![1.0, 0.0]);
        index.add(Bytes::from("c"), vec![2.0, 0.0]);

        // Force a→b only, b↔c. Entry a. (pre-CU: former={c}, no reconnect with a)
        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        layer.add_edge(Bytes::from("a"), Bytes::from("b"));
        layer.add_edge(Bytes::from("b"), Bytes::from("c"));
        layer.add_edge(Bytes::from("c"), Bytes::from("b"));
        index.entry_point = Some(Bytes::from("a"));

        index.remove(&Bytes::from("b"));

        assert!(!index.vectors.contains_key(&Bytes::from("b")));
        assert!(index.vectors.contains_key(&Bytes::from("a")));
        assert!(index.vectors.contains_key(&Bytes::from("c")));

        let entry = index
            .entry_point
            .clone()
            .expect("non-empty index has entry_point");
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut stack = vec![entry.clone()];
        seen.insert(entry);
        while let Some(cur) = stack.pop() {
            for nb in index.layers[0].get_neighbors(&cur) {
                if seen.insert(nb.clone()) {
                    stack.push(nb);
                }
            }
        }
        assert!(
            seen.contains(&Bytes::from("c")),
            "after asymmetric bridge remove, c must be BFS-reachable from entry (visited {:?})",
            seen
        );

        let results = index.search(&[2.0f32, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].doc_id,
            Bytes::from("c"),
            "leaf c must remain searchable after asymmetric bridge remove (got {:?})",
            results[0].doc_id
        );
    }

    /// Batch CU: multi-way star `a,c,d ↔ b` with degree-saturated leaves.
    /// Single nearest-peer force-keep orphans `d` when mutual-NN is `a↔c` and
    /// max_m is filled by closer decoys; spanning force-keep must keep a path.
    #[test]
    fn hnsw_bridge_remove_star_multiway_reconnects() {
        // M=1 → layer-0 max_m=2 so two close decoys fully saturate each leaf.
        let mut index = HNSWIndex::new(1, 8, DistanceMetric::L2);
        // a and c mutual-nearest among survivors; d far — single nearest force-keep
        // cannot span a–d without force-keeping the path edge on c.
        index.add(Bytes::from("a"), vec![0.0, 0.0]);
        index.add(Bytes::from("b"), vec![1.0, 0.0]);
        index.add(Bytes::from("c"), vec![0.05, 0.0]);
        index.add(Bytes::from("d"), vec![10.0, 0.0]);
        // Decoys closer to each leaf than the far former peers.
        index.add(Bytes::from("da1"), vec![0.0, 0.01]);
        index.add(Bytes::from("da2"), vec![0.0, 0.02]);
        index.add(Bytes::from("dc1"), vec![0.05, 0.01]);
        index.add(Bytes::from("dc2"), vec![0.05, 0.02]);
        index.add(Bytes::from("dd1"), vec![10.0, 0.01]);
        index.add(Bytes::from("dd2"), vec![10.0, 0.02]);

        // Force star a,c,d ↔ b; each of a,c,d also linked to its two decoys.
        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        for leaf in ["a", "c", "d"] {
            layer.add_edge(Bytes::from(leaf), Bytes::from("b"));
            layer.add_edge(Bytes::from("b"), Bytes::from(leaf));
        }
        layer.add_edge(Bytes::from("a"), Bytes::from("da1"));
        layer.add_edge(Bytes::from("a"), Bytes::from("da2"));
        layer.add_edge(Bytes::from("da1"), Bytes::from("a"));
        layer.add_edge(Bytes::from("da2"), Bytes::from("a"));
        layer.add_edge(Bytes::from("c"), Bytes::from("dc1"));
        layer.add_edge(Bytes::from("c"), Bytes::from("dc2"));
        layer.add_edge(Bytes::from("dc1"), Bytes::from("c"));
        layer.add_edge(Bytes::from("dc2"), Bytes::from("c"));
        layer.add_edge(Bytes::from("d"), Bytes::from("dd1"));
        layer.add_edge(Bytes::from("d"), Bytes::from("dd2"));
        layer.add_edge(Bytes::from("dd1"), Bytes::from("d"));
        layer.add_edge(Bytes::from("dd2"), Bytes::from("d"));
        index.entry_point = Some(Bytes::from("a"));

        index.remove(&Bytes::from("b"));

        assert!(!index.vectors.contains_key(&Bytes::from("b")));

        let entry = index
            .entry_point
            .clone()
            .expect("non-empty index has entry_point");
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut stack = vec![entry.clone()];
        seen.insert(entry);
        while let Some(cur) = stack.pop() {
            for nb in index.layers[0].get_neighbors(&cur) {
                if seen.insert(nb.clone()) {
                    stack.push(nb);
                }
            }
        }
        for id in [Bytes::from("a"), Bytes::from("c"), Bytes::from("d")] {
            assert!(
                seen.contains(&id),
                "after multi-way star remove, {:?} must be BFS-reachable from entry (visited {:?})",
                id,
                seen
            );
        }
    }

    /// Batch CW: ≥4 former neighbors with `max_m=2` forces the NN-path reconnect
    /// branch (`n-1 > max_m`), not the full clique covered by the star test.
    #[test]
    fn hnsw_bridge_remove_path_branch_reconnects() {
        // M=1 → layer-0 max_m=2; 4 survivors → n-1=3 > 2 → path branch.
        let mut index = HNSWIndex::new(1, 8, DistanceMetric::L2);
        index.add(Bytes::from("hub"), vec![0.0, 0.0]);
        // Leaves on a line so the NN-path is a–b–c–d (well-defined).
        index.add(Bytes::from("a"), vec![1.0, 0.0]);
        index.add(Bytes::from("b"), vec![2.0, 0.0]);
        index.add(Bytes::from("c"), vec![3.0, 0.0]);
        index.add(Bytes::from("d"), vec![4.0, 0.0]); // farthest leaf

        // Force star: all leaves ↔ hub only (4 former neighbors).
        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        for leaf in ["a", "b", "c", "d"] {
            layer.add_edge(Bytes::from(leaf), Bytes::from("hub"));
            layer.add_edge(Bytes::from("hub"), Bytes::from(leaf));
        }
        index.entry_point = Some(Bytes::from("a"));

        index.remove(&Bytes::from("hub"));

        assert!(!index.vectors.contains_key(&Bytes::from("hub")));
        for id in ["a", "b", "c", "d"] {
            assert!(index.vectors.contains_key(&Bytes::from(id)));
        }

        let entry = index
            .entry_point
            .clone()
            .expect("non-empty index has entry_point");
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut stack = vec![entry.clone()];
        seen.insert(entry);
        while let Some(cur) = stack.pop() {
            for nb in index.layers[0].get_neighbors(&cur) {
                if seen.insert(nb.clone()) {
                    stack.push(nb);
                }
            }
        }
        for id in [Bytes::from("a"), Bytes::from("b"), Bytes::from("c"), Bytes::from("d")] {
            assert!(
                seen.contains(&id),
                "after path-branch hub remove, {:?} must be BFS-reachable from entry (visited {:?})",
                id,
                seen
            );
        }

        // Farthest leaf self-search must hit via the repaired path (graph walk).
        let results = index.search(&[4.0f32, 0.0], 1);
        assert_eq!(results.len(), 1, "search must hit for farthest leaf d");
        assert_eq!(
            results[0].doc_id,
            Bytes::from("d"),
            "farthest leaf d must remain searchable after path-branch bridge remove (got {:?})",
            results[0].doc_id
        );
    }

    /// Batch CW: when `|must_keep| > max_m`, keep the closest required edges and
    /// never drop a still-required id via `kept.pop()` (old oversize fallback).
    #[test]
    fn prune_neighbors_keeping_caps_must_keep_by_distance() {
        let mut index = HNSWIndex::new(1, 8, DistanceMetric::L2);
        index.add(Bytes::from("node"), vec![0.0, 0.0]);
        index.add(Bytes::from("m1"), vec![1.0, 0.0]);
        index.add(Bytes::from("m2"), vec![2.0, 0.0]);
        index.add(Bytes::from("m3"), vec![3.0, 0.0]); // farthest must — should be dropped
        index.add(Bytes::from("filler"), vec![0.5, 0.0]); // closer non-must

        for layer in &mut index.layers {
            for (_id, neigh) in layer.neighbors.iter_mut() {
                neigh.clear();
            }
        }
        let layer = &mut index.layers[0];
        // Node linked to all three musts + a closer filler (degree 4 > max_m=2).
        for id in ["m1", "m2", "m3", "filler"] {
            layer.add_edge(Bytes::from("node"), Bytes::from(id));
        }

        let must = [
            Bytes::from("m1"),
            Bytes::from("m2"),
            Bytes::from("m3"),
        ];
        // max_m=2 with 3 must-keeps: pre-CW popped a required edge to fit m3.
        index.prune_neighbors_keeping(&Bytes::from("node"), 0, 2, &must);

        let neigh = index.layers[0].get_neighbors(&Bytes::from("node"));
        assert_eq!(
            neigh.len(),
            2,
            "pruned neighbor list must respect max_m=2 (got {:?})",
            neigh
        );
        assert!(
            neigh.iter().any(|n| n == &Bytes::from("m1")),
            "closest must m1 must be kept (got {:?})",
            neigh
        );
        assert!(
            neigh.iter().any(|n| n == &Bytes::from("m2")),
            "second-closest must m2 must be kept — old pop-required dropped it for m3 (got {:?})",
            neigh
        );
        assert!(
            !neigh.iter().any(|n| n == &Bytes::from("m3")),
            "farthest must m3 must be dropped when |must| > max_m (got {:?})",
            neigh
        );
        assert!(
            !neigh.iter().any(|n| n == &Bytes::from("filler")),
            "non-required filler must yield to capped must-keep set (got {:?})",
            neigh
        );
    }

    /// Batch CT smoke: small M with hub remove/re-add churn — remaining ids stay
    /// reachable from entry (cheap connectivity check, not a global force-keep proof).
    #[test]
    fn hnsw_m1_hub_churn_preserves_reachability() {
        let mut index = HNSWIndex::new(1, 8, DistanceMetric::L2);
        // Hub near origin; leaves along a line.
        index.add(Bytes::from("hub"), vec![0.0, 0.0]);
        for i in 1..=12 {
            index.add(Bytes::from(format!("leaf{i}")), vec![i as f32, 0.0]);
        }

        // Remove and re-insert hub a few times with a slight move.
        for t in 0..3 {
            index.add(Bytes::from("hub"), vec![0.1 * t as f32, 0.0]);
        }
        // Drop hub entirely and ensure remaining leaves are still one component
        // from whatever entry pick_entry_point chooses (or stays).
        index.remove(&Bytes::from("hub"));

        assert!(!index.vectors.contains_key(&Bytes::from("hub")));
        assert_eq!(index.len(), 12);

        let entry = index
            .entry_point
            .clone()
            .expect("non-empty index has entry_point");
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut stack = vec![entry.clone()];
        seen.insert(entry);
        while let Some(cur) = stack.pop() {
            for nb in index.layers[0].get_neighbors(&cur) {
                if seen.insert(nb.clone()) {
                    stack.push(nb);
                }
            }
        }
        // Not every leaf is guaranteed reachable under M=1 after arbitrary hub
        // churn (force-keep is insert-time only), but bridge repair on hub remove
        // should reconnect the hub's former neighbors to each other. Count how
        // many remain reachable; require a majority of leaves stay connected.
        let reachable_leaves = (1..=12)
            .filter(|i| seen.contains(&Bytes::from(format!("leaf{i}"))))
            .count();
        assert!(
            reachable_leaves >= 8,
            "after M=1 hub churn+remove, expected most leaves reachable from entry; got {}/12 (visited {})",
            reachable_leaves,
            seen.len()
        );
    }

    /// Batch CV: moderate-N recall@k of HNSW vs FLAT ground truth + indicative
    /// search throughput (wall time over the query set).
    ///
    /// Methodology (also in `docs/benchmarks.md`):
    /// - N=300 random **unit** vectors, dim=16, Cosine; deterministic `StdRng` seed.
    /// - HNSW: M=16, ef_construction=ef_search=100 (reasonable defaults).
    /// - Q=40 independent random unit queries (same seed stream after corpus).
    /// - recall@k = |HNSW top-k ∩ FLAT top-k| / k, averaged over queries.
    ///
    /// Thresholds (pass a correct graph ANN; fail random/empty search):
    /// - mean recall@1  ≥ 0.90
    /// - mean recall@10 ≥ 0.80
    ///
    /// Throughput: total wall time for all Q searches (FLAT vs HNSW), printed via
    /// `eprintln!` (visible with `cargo test -- --nocapture`). Not a CI gate.
    #[test]
    fn hnsw_recall_at_k_vs_flat_and_throughput() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use std::time::Instant;

        const N: usize = 300;
        const DIM: usize = 16;
        const Q: usize = 40;
        const M: usize = 16;
        const EF: usize = 100;
        const SEED: u64 = 0xC0_FF_EE_42; // fixed — do not change without re-checking thresholds

        // Thresholds: high enough that a broken/random search fails, low enough
        // that a correct layer-0 HNSW with these params is stable across hosts.
        const MIN_RECALL_AT_1: f64 = 0.90;
        const MIN_RECALL_AT_10: f64 = 0.80;

        fn random_unit(rng: &mut StdRng, dim: usize) -> Vec<f32> {
            let mut v: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 1e-12 {
                for x in &mut v {
                    *x /= norm;
                }
            } else {
                v[0] = 1.0;
            }
            v
        }

        /// Standard recall@k = |approx ∩ exact| / k using the top-k id sets.
        fn recall_at_k(
            flat: &[VectorSearchResult],
            hnsw: &[VectorSearchResult],
            k: usize,
        ) -> f64 {
            let truth: HashSet<&Bytes> = flat.iter().take(k).map(|r| &r.doc_id).collect();
            let hits = hnsw
                .iter()
                .take(k)
                .filter(|r| truth.contains(&r.doc_id))
                .count();
            hits as f64 / k as f64
        }

        let mut rng = StdRng::seed_from_u64(SEED);
        let corpus: Vec<(Bytes, Vec<f32>)> = (0..N)
            .map(|i| (Bytes::from(format!("v{i}")), random_unit(&mut rng, DIM)))
            .collect();
        let queries: Vec<Vec<f32>> = (0..Q).map(|_| random_unit(&mut rng, DIM)).collect();

        let mut flat = FlatVectorIndex::new(DistanceMetric::Cosine);
        let mut hnsw = HNSWIndex::new(M, EF, DistanceMetric::Cosine);
        for (id, v) in &corpus {
            flat.add(id.clone(), v.clone());
            hnsw.add(id.clone(), v.clone());
        }
        assert_eq!(flat.len(), N);
        assert_eq!(hnsw.len(), N);

        // One search pass at k=10 per index; recall@1 uses the top-1 of that list.
        // Wall times cover only the Q searches (build excluded).
        let t_flat = Instant::now();
        let flat_top10: Vec<Vec<VectorSearchResult>> =
            queries.iter().map(|q| flat.search(q, 10)).collect();
        let flat_ms = t_flat.elapsed().as_secs_f64() * 1000.0;

        let t_hnsw = Instant::now();
        let hnsw_top10: Vec<Vec<VectorSearchResult>> =
            queries.iter().map(|q| hnsw.search(q, 10)).collect();
        let hnsw_ms = t_hnsw.elapsed().as_secs_f64() * 1000.0;

        let mut sum_r1 = 0.0f64;
        let mut sum_r10 = 0.0f64;
        for i in 0..Q {
            let f = &flat_top10[i];
            let h = &hnsw_top10[i];
            assert_eq!(f.len(), 10, "FLAT must return k=10 for query {i}");
            assert_eq!(h.len(), 10, "HNSW must return k=10 for query {i}");
            sum_r1 += recall_at_k(f, h, 1);
            sum_r10 += recall_at_k(f, h, 10);
        }

        let mean_r1 = sum_r1 / Q as f64;
        let mean_r10 = sum_r10 / Q as f64;
        let speedup = if hnsw_ms > 0.0 {
            flat_ms / hnsw_ms
        } else {
            f64::INFINITY
        };

        eprintln!(
            "hnsw_recall_at_k_vs_flat_and_throughput: N={N} dim={DIM} Q={Q} M={M} ef={EF} \
             mean_recall@1={mean_r1:.3} mean_recall@10={mean_r10:.3} \
             flat_search_k10={flat_ms:.3}ms hnsw_search_k10={hnsw_ms:.3}ms speedup≈{speedup:.2}× \
             (indicative single-host; not a CI gate)"
        );

        assert!(
            mean_r1 >= MIN_RECALL_AT_1,
            "mean recall@1 {mean_r1:.3} < {MIN_RECALL_AT_1} (N={N} dim={DIM} Q={Q} M={M} ef={EF})"
        );
        assert!(
            mean_r10 >= MIN_RECALL_AT_10,
            "mean recall@10 {mean_r10:.3} < {MIN_RECALL_AT_10} (N={N} dim={DIM} Q={Q} M={M} ef={EF})"
        );
    }
}
