use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
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
    ///
    /// Prefer [`unlink_collecting_undirected_former`] on the remove path so the
    /// reverse scan is not repeated (Batch CY).
    fn unlink_node(&mut self, node_id: &Bytes) {
        for edges in self.neighbors.values_mut() {
            edges.retain(|n| n != node_id);
        }
        self.neighbors.remove(node_id);
    }

    /// One full-layer reverse pass: collect undirected former neighbors of
    /// `node_id` (outgoing ∪ predecessors) among `live` ids, strip reverse
    /// edges, and drop the node entry.
    ///
    /// **Complexity (Batch CY):** O(N_layer + deg(node)) instead of two separate
    /// O(N_layer) scans (snapshot reverse-scan then `unlink_node`).
    fn unlink_collecting_undirected_former(
        &mut self,
        node_id: &Bytes,
        live: &HashMap<Bytes, Vec<f32>>,
    ) -> Vec<Bytes> {
        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut former: Vec<Bytes> = Vec::new();

        // Outgoing neighbors of the deleted node (asymmetric case: out only).
        if let Some(out) = self.neighbors.get(node_id) {
            for n in out {
                if n != node_id && live.contains_key(n) && seen.insert(n.clone()) {
                    former.push(n.clone());
                }
            }
        }

        // Reverse pass: strip edges → node_id and collect live predecessors.
        for (id, neighs) in self.neighbors.iter_mut() {
            if id == node_id {
                continue;
            }
            let had_edge = neighs.iter().any(|n| n == node_id);
            if !had_edge {
                continue;
            }
            neighs.retain(|n| n != node_id);
            if live.contains_key(id) && seen.insert(id.clone()) {
                former.push(id.clone());
            }
        }

        self.neighbors.remove(node_id);
        former
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
/// size `ef_search`.
///
/// **Batch FF (multi-layer insert):** each insert draws a random level with geometric
/// decay and is wired on layers `0..=level`:
/// - Level formula (Malkov & Yashunin / hnswlib):  
///   `ml = 1 / ln(max(M, 2))`,  
///   `level = floor(-ln(U) * ml)` for `U ~ Unif(0, 1)`, capped at [`HNSWIndex::MAX_LEVEL`].
/// - Upper layers (`level+1 ..= top`): greedy SEARCH-LAYER with `ef = 1` from the
///   entry point down to the insert level.
/// - Layers `0..=min(top, level)`: SEARCH-LAYER with `ef_construction`, select top-`M`
///   neighbors, bidirectional edges, prune to `max_edges(layer)` (`≈2M` on layer 0,
///   `M` above), force-keeping reverse edges to the new node.
/// - Entry point is updated when the new node's level exceeds the current top layer.
/// - Query search greedily descends layers `top…1` with `ef=1`, then runs layer 0
///   with `ef_search`.
///
/// Deterministic tests can force levels via [`HNSWIndex::enqueue_levels`] or seed
/// the level RNG with [`HNSWIndex::with_level_seed`].
///
/// **Persistence (Batch FV):** RDB v6+ can persist a durable graph snapshot
/// ([`HnswGraphSnapshot`]: `entry_point`, per-node levels, per-layer adjacency)
/// and restore it edge-identically via [`HNSWIndex::apply_graph_snapshot`] without
/// re-sampling levels. Neighbor lists are **canonicalized** (sorted by id) on
/// export for deterministic equality. AOF still rewrites `FT.CREATE` + docs only
/// — AOF-only load rebuilds the graph by re-`add` (levels re-sampled). Old RDB
/// files without a graph section keep the rebuild-by-readd path.
///
/// **Batch CS:** `remove` unlinks reverse edges and clears the layer entry; insert uses
/// layer-0 `M_max ≈ 2M` and force-keeps at least one reverse edge so new nodes stay
/// reachable from `entry_point` at insert time; existing-id `add` rewires
/// (remove + re-insert).
///
/// **Batch CT/CU/CY:** on hard-delete, former neighbors are snapshotted as an
/// **undirected** adjacency (outgoing ∪ reverse edges) in **one** reverse pass
/// that also unlinks the node (no second full-layer scan). Survivors are then
/// reconnected with a spanning structure (full clique when degree fits, else a
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
    /// Entry point (node ID) for search — a node present on the highest non-empty layer
    entry_point: Option<Bytes>,
    /// Level normalization: `ml = 1 / ln(max(M, 2))` for geometric level sampling.
    ml: f64,
    /// FIFO of forced insert levels (tests / injectors). Empty → geometric sample.
    level_assign_queue: VecDeque<usize>,
    /// Optional seedable RNG for level assignment. `None` → `thread_rng`.
    level_rng: Option<StdRng>,
}

impl HNSWIndex {
    /// Practical cap on sampled insert level (pathological tails of the geometric draw).
    pub const MAX_LEVEL: usize = 16;

    pub fn new(m: usize, ef_construction: usize, distance_metric: DistanceMetric) -> Self {
        let m = m.max(1);
        Self {
            vectors: HashMap::new(),
            layers: vec![HNSWLayer::new()],
            m,
            ef_construction: ef_construction.max(1),
            ef_search: ef_construction.max(1),
            distance_metric,
            entry_point: None,
            ml: Self::compute_ml(m),
            level_assign_queue: VecDeque::new(),
            level_rng: None,
        }
    }

    /// Seed the level-assignment RNG for reproducible multi-layer structure in tests.
    pub fn with_level_seed(mut self, seed: u64) -> Self {
        self.level_rng = Some(StdRng::seed_from_u64(seed));
        self
    }

    /// Enqueue forced insert levels (FIFO). Each `add` consumes one when present.
    ///
    /// Used for deterministic multi-layer unit tests so CI is not flaky on RNG tails.
    pub fn enqueue_levels(&mut self, levels: impl IntoIterator<Item = usize>) {
        for l in levels {
            self.level_assign_queue.push_back(l.min(Self::MAX_LEVEL));
        }
    }

    /// `ml = 1 / ln(max(M, 2))` — classic HNSW level multiplier (Malkov & Yashunin).
    fn compute_ml(m: usize) -> f64 {
        1.0 / (m.max(2) as f64).ln()
    }

    /// Assign insert level: forced queue first, else geometric decay.
    ///
    /// Formula: `level = floor(-ln(U) * ml)` with `U ~ Unif(0, 1)`, capped at
    /// [`Self::MAX_LEVEL`]. Expected fraction of nodes on layer ≥ ℓ is roughly
    /// `exp(-ℓ / ml)` ≈ `M^{-ℓ}` when `ml = 1/ln(M)`.
    fn assign_level(&mut self) -> usize {
        if let Some(level) = self.level_assign_queue.pop_front() {
            return level.min(Self::MAX_LEVEL);
        }
        let u: f64 = match self.level_rng.as_mut() {
            Some(rng) => loop {
                let x: f64 = rng.gen();
                if x > 0.0 && x < 1.0 {
                    break x;
                }
            },
            None => loop {
                let x: f64 = rand::random();
                if x > 0.0 && x < 1.0 {
                    break x;
                }
            },
        };
        let level = (-u.ln() * self.ml).floor() as usize;
        level.min(Self::MAX_LEVEL)
    }

    /// Highest layer index that still has at least one node, or 0 if only empty base.
    fn highest_nonempty_layer(&self) -> usize {
        for (i, layer) in self.layers.iter().enumerate().rev() {
            if !layer.neighbors.is_empty() {
                return i;
            }
        }
        0
    }

    /// Drop trailing empty upper layers (keep at least layer 0).
    fn trim_empty_upper_layers(&mut self) {
        while self.layers.len() > 1 {
            if self
                .layers
                .last()
                .map(|l| l.neighbors.is_empty())
                .unwrap_or(true)
            {
                self.layers.pop();
            } else {
                break;
            }
        }
    }

    /// Max outgoing edges kept after prune. Layer 0 uses `≈ 2M` (classic HNSW
    /// `M_max0`) so reverse links survive better under degree caps; upper layers use `M`.
    fn max_edges(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m.saturating_mul(2).max(self.m)
        } else {
            self.m
        }
    }

    /// Connect `doc_id` to `neighbors` on `layer` with bidirectional edges + prune.
    fn connect_on_layer(&mut self, doc_id: &Bytes, neighbors: &[Bytes], layer: usize) {
        let max_m = self.max_edges(layer);
        for neighbor in neighbors {
            debug_assert_ne!(neighbor, doc_id, "neighbor selection must exclude self");
            if let Some(l) = self.layers.get_mut(layer) {
                l.add_edge(doc_id.clone(), neighbor.clone());
                l.add_edge(neighbor.clone(), doc_id.clone());
            }
            // Cap degree on the existing neighbor; force-keep reverse edge to the
            // new node so it remains reachable from entry via outgoing walks.
            self.prune_neighbors_keeping(neighbor, layer, max_m, std::slice::from_ref(doc_id));
        }
        self.prune_neighbors_keeping(doc_id, layer, max_m, &[]);
    }

    /// Add a vector to the index (classic multi-layer HNSW insert — Batch FF).
    ///
    /// Neighbors are selected via graph search; the new node is never chosen as
    /// its own neighbor (SEARCH-LAYER only follows existing edges). Existing IDs
    /// rewire the graph (unlink old edges, re-select neighbors for the new vector
    /// and a freshly drawn level).
    pub fn add(&mut self, doc_id: Bytes, vector: Vec<f32>) {
        // Update-in-place: full graph rewire so queries near the new location work.
        if self.vectors.contains_key(&doc_id) {
            self.remove(&doc_id);
        }

        let level = self.assign_level();
        while self.layers.len() <= level {
            self.layers.push(HNSWLayer::new());
        }

        // First node becomes the entry point on every layer it occupies.
        if self.entry_point.is_none() || self.vectors.is_empty() {
            for l in 0..=level {
                self.layers[l].add_node(doc_id.clone());
            }
            self.vectors.insert(doc_id.clone(), vector);
            self.entry_point = Some(doc_id);
            return;
        }

        let mut ep = self
            .entry_point
            .clone()
            .expect("entry_point set when index non-empty");
        // Top layer of the *existing* graph before this insert (entry lives there).
        let top = self.highest_nonempty_layer();
        let ef = self.ef_construction.max(self.m);

        // Phase 1: greedy descent from top down to level+1 with ef=1.
        if top > level {
            for lc in ((level + 1)..=top).rev() {
                let cands = self.search_layer(&vector, &ep, 1, lc);
                if let Some((id, _)) = cands.first() {
                    ep = id.clone();
                }
            }
        }

        // Install vector + empty node shells on layers 0..=level (no stale revive).
        self.vectors.insert(doc_id.clone(), vector);
        for l in 0..=level {
            self.layers[l].add_node(doc_id.clone());
        }

        // Phase 2: SEARCH-LAYER + connect on each layer the node occupies that
        // already had graph structure (or layer 0). For layers above the previous
        // top, the new node is alone — no neighbors to connect.
        let connect_top = level.min(top);
        for lc in (0..=connect_top).rev() {
            let candidates = self.search_layer(
                self.vectors
                    .get(&doc_id)
                    .expect("vector just inserted")
                    .as_slice(),
                &ep,
                ef,
                lc,
            );
            let neighbors = Self::select_top_m(candidates.clone(), self.m);
            if let Some((id, _)) = candidates.first() {
                ep = id.clone();
            }
            self.connect_on_layer(&doc_id, &neighbors, lc);
        }

        // Promote entry point when the new node sits above the previous top.
        if level > top {
            self.entry_point = Some(doc_id);
        }
    }

    /// Remove a vector and fully unlink it from the HNSW graph.
    ///
    /// Strips reverse edges, removes the node from **every** layer map, reconnects
    /// former neighbors so a bridge/cut-vertex delete does not permanently
    /// partition the layer (Batch CT/CU/CY), reassigns `entry_point` when needed
    /// (prefer a remaining node on the highest non-empty layer that still has
    /// edges), and trims trailing empty upper layers (Batch FF multi-layer).
    ///
    /// Per layer, undirected former-neighbor collection and unlink share a
    /// single reverse pass (`unlink_collecting_undirected_former`) — O(N_layer)
    /// once, not snapshot-then-unlink twice.
    ///
    /// Bridge repair remains a per-layer heuristic residual — not a global
    /// non-partition guarantee under arbitrary later hub churn (same as CT–CY).
    pub fn remove(&mut self, doc_id: &Bytes) {
        if !self.vectors.contains_key(doc_id) {
            // Still scrub any orphaned layer residue (defensive).
            for layer in &mut self.layers {
                layer.unlink_node(doc_id);
            }
            self.trim_empty_upper_layers();
            if self.entry_point.as_ref() == Some(doc_id) {
                self.entry_point = self.pick_entry_point();
            }
            return;
        }

        // Fuse undirected snapshot + unlink per layer (Batch CY). Live set is
        // still `self.vectors` (deleted id is live until after the pass so its
        // outgoing list is readable; it is excluded from `former` by id check /
        // not being a predecessor of itself in the reverse pass).
        let former_by_layer: Vec<Vec<Bytes>> = {
            let vectors = &self.vectors;
            self.layers
                .iter_mut()
                .map(|layer| layer.unlink_collecting_undirected_former(doc_id, vectors))
                .collect()
        };

        self.vectors.remove(doc_id);

        // Reconnect survivors that used the deleted id as a bridge.
        for (layer_idx, former) in former_by_layer.iter().enumerate() {
            self.bridge_reconnect_neighbors(former, layer_idx);
        }

        self.trim_empty_upper_layers();

        if self.entry_point.as_ref() == Some(doc_id)
            || self
                .entry_point
                .as_ref()
                .map(|ep| !self.vectors.contains_key(ep))
                .unwrap_or(true)
        {
            self.entry_point = self.pick_entry_point();
        } else if let Some(ep) = self.entry_point.as_ref() {
            // Entry still live but may no longer sit on the highest layer after
            // this delete emptied the top — re-pick so entry is a top-layer node.
            let top = self.highest_nonempty_layer();
            let on_top = self
                .layers
                .get(top)
                .map(|l| l.neighbors.contains_key(ep))
                .unwrap_or(false);
            if !on_top {
                self.entry_point = self.pick_entry_point();
            }
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

    /// Choose a new entry point among remaining vectors.
    ///
    /// Prefers a node on the **highest non-empty layer** that still has edges
    /// (Batch FF multi-layer); falls back to any live id on that layer, then
    /// any vector.
    fn pick_entry_point(&self) -> Option<Bytes> {
        if self.vectors.is_empty() {
            return None;
        }
        let top = self.highest_nonempty_layer();
        if let Some(layer) = self.layers.get(top) {
            let mut fallback: Option<Bytes> = None;
            for id in layer.neighbors.keys() {
                if !self.vectors.contains_key(id) {
                    continue;
                }
                let has_edges = !layer.get_neighbors(id).is_empty();
                if has_edges {
                    return Some(id.clone());
                }
                if fallback.is_none() {
                    fallback = Some(id.clone());
                }
            }
            if fallback.is_some() {
                return fallback;
            }
        }
        // Defensive: any remaining vector (layer maps out of sync).
        self.vectors.keys().next().cloned()
    }

    /// Search for k nearest neighbors via multi-layer HNSW (Batch FF).
    ///
    /// Greedy descent on layers `top…1` with `ef=1`, then SEARCH-LAYER on layer 0
    /// with `ef_search` (at least `k`). Approximate: only nodes reachable via
    /// edges from the refined entry are considered on layer 0.
    /// Fallback: if the entry point is missing from `vectors`, brute-force the map
    /// (should not happen after normal `add` paths).
    pub fn search(&self, query_vector: &[f32], k: usize) -> Vec<VectorSearchResult> {
        if self.vectors.is_empty() || k == 0 {
            return Vec::new();
        }
        let Some(entry) = self.entry_point.as_ref() else {
            return Vec::new();
        };

        let mut ep = entry.clone();
        let top = self.highest_nonempty_layer();
        // Greedy upper-layer descent (ef=1) to refine the layer-0 entry.
        if top >= 1 {
            for lc in (1..=top).rev() {
                let cands = self.search_layer(query_vector, &ep, 1, lc);
                if let Some((id, _)) = cands.first() {
                    ep = id.clone();
                }
            }
        }

        let ef = self.ef_search.max(k);
        let candidates = self.search_layer(query_vector, &ep, ef, 0);

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

    /// Highest layer index where `doc_id` appears, if any (diagnostics / snapshot).
    pub fn node_level(&self, doc_id: &Bytes) -> Option<usize> {
        let mut max = None;
        for (i, layer) in self.layers.iter().enumerate() {
            if layer.neighbors.contains_key(doc_id) {
                max = Some(i);
            }
        }
        max
    }

    /// Entry point id, if the index is non-empty.
    pub fn entry_point(&self) -> Option<&Bytes> {
        self.entry_point.as_ref()
    }

    /// Outgoing neighbors of `doc_id` on `layer` (empty if absent).
    pub fn layer_neighbors(&self, layer: usize, doc_id: &Bytes) -> Vec<Bytes> {
        self.layers
            .get(layer)
            .map(|l| l.get_neighbors(doc_id))
            .unwrap_or_default()
    }

    /// Number of graph layers currently allocated (at least 1 when empty base exists).
    pub fn num_layers(&self) -> usize {
        self.layers.len()
    }

    /// Export durable graph structure: entry point, per-node levels, per-layer edges.
    ///
    /// Neighbor lists and node order are **canonicalized** (sorted by id) so two
    /// snapshots of the same undirected structure compare equal regardless of
    /// insert-time edge append order. Vectors are **not** included — caller must
    /// already hold the same vectors in `self` before [`apply_graph_snapshot`].
    pub fn snapshot_graph(&self) -> HnswGraphSnapshot {
        let mut levels: Vec<(Bytes, u32)> = self
            .vectors
            .keys()
            .filter_map(|id| {
                self.node_level(id)
                    .map(|lvl| (id.clone(), lvl as u32))
            })
            .collect();
        levels.sort_by(|a, b| a.0.cmp(&b.0));

        let top = self.highest_nonempty_layer();
        // Export layers 0..=top (or empty single layer when index empty).
        let layer_count = if self.vectors.is_empty() {
            0
        } else {
            top + 1
        };
        let mut layers = Vec::with_capacity(layer_count);
        for li in 0..layer_count {
            let mut adj: Vec<(Bytes, Vec<Bytes>)> = self.layers[li]
                .neighbors
                .iter()
                .map(|(id, neighs)| {
                    let mut n = neighs.clone();
                    n.sort();
                    n.dedup();
                    (id.clone(), n)
                })
                .collect();
            adj.sort_by(|a, b| a.0.cmp(&b.0));
            layers.push(adj);
        }

        HnswGraphSnapshot {
            entry_point: self.entry_point.clone(),
            levels,
            layers,
        }
    }

    /// Install a graph snapshot over **existing** vectors without re-sampling levels.
    ///
    /// Validates that every level/edge id exists in `self.vectors` and that
    /// layer membership is consistent with declared levels (node at level L
    /// must appear on layers `0..=L`). Neighbor lists are installed as given
    /// (callers that want canonical order should snapshot first or sort).
    ///
    /// Does **not** insert missing vectors — use [`install_vector`] first when
    /// restoring from a side channel that carries payloads.
    pub fn apply_graph_snapshot(&mut self, snap: &HnswGraphSnapshot) -> Result<(), String> {
        // Empty snapshot → clear graph structure but keep vectors? Prefer:
        // empty snap with no vectors → empty index graph; if vectors remain and
        // snap is empty, reject (would orphan vectors from search).
        if snap.levels.is_empty() && snap.layers.is_empty() {
            if self.vectors.is_empty() {
                self.layers = vec![HNSWLayer::new()];
                self.entry_point = None;
                return Ok(());
            }
            return Err(
                "HNSW graph snapshot is empty but index still has vectors".into(),
            );
        }

        // Validate levels cover exactly the vector set (or subset with all edge ids).
        let mut level_map: HashMap<Bytes, usize> = HashMap::new();
        for (id, lvl) in &snap.levels {
            if !self.vectors.contains_key(id) {
                return Err(format!(
                    "HNSW snapshot level node {:?} missing from vectors",
                    id
                ));
            }
            if *lvl as usize > Self::MAX_LEVEL {
                return Err(format!(
                    "HNSW snapshot level {} exceeds MAX_LEVEL {}",
                    lvl,
                    Self::MAX_LEVEL
                ));
            }
            if level_map.insert(id.clone(), *lvl as usize).is_some() {
                return Err(format!("HNSW snapshot duplicate level for {:?}", id));
            }
        }
        // Every vector must have a level entry.
        for id in self.vectors.keys() {
            if !level_map.contains_key(id) {
                return Err(format!(
                    "HNSW snapshot missing level for vector {:?}",
                    id
                ));
            }
        }

        let max_level = level_map.values().copied().max().unwrap_or(0);
        if snap.layers.len() != max_level + 1 {
            return Err(format!(
                "HNSW snapshot layer count {} != max_level+1 ({})",
                snap.layers.len(),
                max_level + 1
            ));
        }

        // Validate adjacency and build new layers.
        let mut new_layers: Vec<HNSWLayer> = (0..=max_level)
            .map(|_| HNSWLayer::new())
            .collect();

        for (li, adj) in snap.layers.iter().enumerate() {
            for (id, neighs) in adj {
                let Some(&node_lvl) = level_map.get(id) else {
                    return Err(format!(
                        "HNSW snapshot layer {li} has node {:?} without level",
                        id
                    ));
                };
                if li > node_lvl {
                    return Err(format!(
                        "HNSW snapshot node {:?} on layer {li} > its level {node_lvl}",
                        id
                    ));
                }
                if !self.vectors.contains_key(id) {
                    return Err(format!(
                        "HNSW snapshot edge source {:?} missing from vectors",
                        id
                    ));
                }
                let mut cleaned = Vec::with_capacity(neighs.len());
                for n in neighs {
                    if n == id {
                        continue; // drop self-loops
                    }
                    if !self.vectors.contains_key(n) {
                        return Err(format!(
                            "HNSW snapshot neighbor {:?} of {:?} missing from vectors",
                            n, id
                        ));
                    }
                    let Some(&n_lvl) = level_map.get(n) else {
                        return Err(format!(
                            "HNSW snapshot neighbor {:?} missing level",
                            n
                        ));
                    };
                    if li > n_lvl {
                        return Err(format!(
                            "HNSW snapshot neighbor {:?} on layer {li} > its level {n_lvl}",
                            n
                        ));
                    }
                    if !cleaned.iter().any(|x| x == n) {
                        cleaned.push(n.clone());
                    }
                }
                new_layers[li].neighbors.insert(id.clone(), cleaned);
            }
            // Ensure every node with level >= li has a layer entry (possibly empty).
            for (id, &lvl) in &level_map {
                if lvl >= li {
                    new_layers[li]
                        .neighbors
                        .entry(id.clone())
                        .or_insert_with(Vec::new);
                }
            }
        }

        if let Some(ep) = &snap.entry_point {
            if !self.vectors.contains_key(ep) {
                return Err(format!(
                    "HNSW snapshot entry_point {:?} missing from vectors",
                    ep
                ));
            }
            // Entry should sit on the top layer.
            if !new_layers[max_level].neighbors.contains_key(ep) {
                return Err(format!(
                    "HNSW snapshot entry_point {:?} not present on top layer {}",
                    ep, max_level
                ));
            }
        } else if !self.vectors.is_empty() {
            return Err("HNSW snapshot missing entry_point for non-empty index".into());
        }

        self.layers = new_layers;
        self.entry_point = snap.entry_point.clone();
        Ok(())
    }

    /// Insert or replace a vector **without** wiring graph edges or sampling a level.
    ///
    /// Used when restoring vectors before [`apply_graph_snapshot`]. Removes any
    /// prior graph presence of `doc_id` so stale edges cannot revive.
    pub fn install_vector(&mut self, doc_id: Bytes, vector: Vec<f32>) {
        if self.vectors.contains_key(&doc_id) {
            // Unlink from layers only (no bridge repair — full snapshot follows).
            for layer in &mut self.layers {
                layer.unlink_node(&doc_id);
            }
        }
        self.vectors.insert(doc_id, vector);
        if self.entry_point.as_ref().map(|ep| !self.vectors.contains_key(ep)).unwrap_or(false) {
            self.entry_point = None;
        }
    }

    /// Drop all vectors and graph structure (keeps M / ef / metric).
    pub fn clear(&mut self) {
        self.vectors.clear();
        self.layers = vec![HNSWLayer::new()];
        self.entry_point = None;
        self.level_assign_queue.clear();
    }

    /// Iterate stored vectors (doc_id, components) for persistence export.
    pub fn iter_vectors(&self) -> impl Iterator<Item = (&Bytes, &Vec<f32>)> {
        self.vectors.iter()
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

/// Durable HNSW graph snapshot (Batch FV): entry point, per-node levels, edges.
///
/// Vectors are intentionally omitted — restore loads vectors first, then applies
/// this structure so levels are **not** re-sampled. Neighbor order is sorted on
/// export ([`HNSWIndex::snapshot_graph`]) for deterministic equality.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HnswGraphSnapshot {
    pub entry_point: Option<Bytes>,
    /// `(doc_id, level)` sorted by `doc_id` on export.
    pub levels: Vec<(Bytes, u32)>,
    /// `layers[i]` = adjacency for layer `i`: `(node_id, sorted neighbor ids)`.
    pub layers: Vec<Vec<(Bytes, Vec<Bytes>)>>,
}

impl HnswGraphSnapshot {
    /// True when there is no graph data to persist.
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty() && self.layers.is_empty() && self.entry_point.is_none()
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
        // Force level 0 for all so "entry" stays entry (no geometric promote).
        index.enqueue_levels([0, 0, 0]);
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
    ///
    /// **Batch FF residual:** multi-layer random level assignment made this test
    /// flaky (~35% fail under `thread_rng`). Force layer-0 for every insert so the
    /// assertion is pure rewire connectivity, not RNG-dependent upper-layer routing.
    #[test]
    fn hnsw_update_rewires_graph() {
        let mut index = HNSWIndex::new(3, 8, DistanceMetric::L2).with_level_seed(0xC5_FE_1E);
        // 15 near + 15 far + target insert + target update = 32 adds.
        index.enqueue_levels(std::iter::repeat(0).take(32));
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

    /// Batch CW/CY: ≥4 former neighbors with `max_m=2` forces the NN-path
    /// reconnect branch (`n-1 > max_m`). Degree-saturating decoys (Batch CY)
    /// fill each leaf's `max_m` with closer non-survivors so **bonus density
    /// alone cannot reconnect** the line — path force-keep is load-bearing.
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
        // max_m=2 decoys per leaf, closer than any other survivor (mirror CU star).
        // Without path force-keep, prune keeps only decoys and orphans the line.
        index.add(Bytes::from("da1"), vec![1.0, 0.01]);
        index.add(Bytes::from("da2"), vec![1.0, 0.02]);
        index.add(Bytes::from("db1"), vec![2.0, 0.01]);
        index.add(Bytes::from("db2"), vec![2.0, 0.02]);
        index.add(Bytes::from("dc1"), vec![3.0, 0.01]);
        index.add(Bytes::from("dc2"), vec![3.0, 0.02]);
        index.add(Bytes::from("dd1"), vec![4.0, 0.01]);
        index.add(Bytes::from("dd2"), vec![4.0, 0.02]);

        // Force star: leaves ↔ hub only as former set; each leaf also saturates
        // degree with its two decoys (decoys are not hub neighbors).
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
        for (leaf, d1, d2) in [
            ("a", "da1", "da2"),
            ("b", "db1", "db2"),
            ("c", "dc1", "dc2"),
            ("d", "dd1", "dd2"),
        ] {
            layer.add_edge(Bytes::from(leaf), Bytes::from(d1));
            layer.add_edge(Bytes::from(leaf), Bytes::from(d2));
            layer.add_edge(Bytes::from(d1), Bytes::from(leaf));
            layer.add_edge(Bytes::from(d2), Bytes::from(leaf));
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
                "after path-branch hub remove under degree pressure, {:?} must be BFS-reachable from entry (visited {:?})",
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
            "farthest leaf d must remain searchable after path-branch bridge remove under decoy pressure (got {:?})",
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

    /// Deterministic unit vector for recall benches (shared by CV/DK gates).
    fn random_unit_vec(rng: &mut rand::rngs::StdRng, dim: usize) -> Vec<f32> {
        use rand::Rng;
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
    fn recall_at_k(flat: &[VectorSearchResult], hnsw: &[VectorSearchResult], k: usize) -> f64 {
        let truth: HashSet<&Bytes> = flat.iter().take(k).map(|r| &r.doc_id).collect();
        let hits = hnsw
            .iter()
            .take(k)
            .filter(|r| truth.contains(&r.doc_id))
            .count();
        hits as f64 / k as f64
    }

    /// Median of a small `f64` sample (used by the ignored larger-N bench).
    fn median_f64(mut xs: Vec<f64>) -> f64 {
        assert!(!xs.is_empty());
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = xs.len() / 2;
        if xs.len() % 2 == 1 {
            xs[mid]
        } else {
            (xs[mid - 1] + xs[mid]) / 2.0
        }
    }

    /// Batch CV + DK: moderate-N recall@k of HNSW vs FLAT ground truth + indicative
    /// **single-shot** search throughput (wall time over the query set).
    ///
    /// Methodology (also in `docs/benchmarks.md`):
    /// - N=300 random **unit** vectors, dim=16, Cosine; deterministic `StdRng` seed.
    /// - HNSW: M=8, ef_construction=ef_search=32 (**Batch DK**: tighter than CV's
    ///   M=16/ef=100 so recall@10 is not trivially 1.0 while staying CI-fast).
    /// - Q=40 independent random unit queries (same seed stream after corpus).
    /// - recall@k = |HNSW top-k ∩ FLAT top-k| / k, averaged over queries.
    ///
    /// Thresholds (Batch DK + **DL** headroom polish):
    /// - mean recall@1  ≥ 0.975
    /// - mean recall@10 ≥ 0.93  (**Batch DL**: was 0.95; ~3.5pp headroom vs observed
    ///   ≈0.985 was thin under f32 graph ops / cross-arch variance. Floor 0.93 keeps
    ///   ~5.5pp cushion while remaining load-bearing — still far above CV's 0.80 and
    ///   well below the easy M=16/ef=100 free-1.0 regime.)
    ///
    /// Throughput: single-shot total wall time for all Q searches (FLAT vs HNSW),
    /// printed via `eprintln!` (`--nocapture`). Not a CI gate on absolute ms.
    /// For larger-N + median-of-3 timings see
    /// [`hnsw_recall_larger_n_median_throughput`] (`#[ignore]`).
    /// Post-delete/update recall: [`hnsw_recall_after_remove_update_churn`].
    #[test]
    fn hnsw_recall_at_k_vs_flat_and_throughput() {
        use rand::rngs::StdRng;
        use rand::SeedableRng;
        use std::time::Instant;

        const N: usize = 300;
        const DIM: usize = 16;
        const Q: usize = 40;
        const M: usize = 8;
        const EF: usize = 32;
        const SEED: u64 = 0xC0_FF_EE_42; // fixed — do not change without re-checking thresholds

        // Batch DK: raised vs CV 0.90/0.80; M/ef lowered so r@10 is not a free 1.0.
        // Batch DL: r@10 0.95 → 0.93 for cross-arch f32 headroom (still load-bearing).
        const MIN_RECALL_AT_1: f64 = 0.975;
        const MIN_RECALL_AT_10: f64 = 0.93;

        let mut rng = StdRng::seed_from_u64(SEED);
        let corpus: Vec<(Bytes, Vec<f32>)> = (0..N)
            .map(|i| (Bytes::from(format!("v{i}")), random_unit_vec(&mut rng, DIM)))
            .collect();
        let queries: Vec<Vec<f32>> = (0..Q).map(|_| random_unit_vec(&mut rng, DIM)).collect();

        let mut flat = FlatVectorIndex::new(DistanceMetric::Cosine);
        let mut hnsw = HNSWIndex::new(M, EF, DistanceMetric::Cosine);
        for (id, v) in &corpus {
            flat.add(id.clone(), v.clone());
            hnsw.add(id.clone(), v.clone());
        }
        assert_eq!(flat.len(), N);
        assert_eq!(hnsw.len(), N);

        // One search pass at k=10 per index; recall@1 uses the top-1 of that list.
        // Wall times cover only the Q searches (build excluded). Single-shot.
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
             (single-shot; indicative single-host; not a CI gate on ms)"
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

    /// Batch DK: larger-N HNSW vs FLAT recall + **median-of-3** search timings.
    ///
    /// **Not run in normal CI** (`#[ignore]`). Prefer release for throughput ratios:
    /// ```text
    /// cargo test --release --lib hnsw_recall_larger_n_median_throughput -- --ignored --nocapture
    /// ```
    ///
    /// Methodology:
    /// - N=5000 unit vectors, dim=16, Cosine; fixed seed (independent of unit gate).
    /// - HNSW M=16 / ef=100 (defaults-class; not the tightened unit-gate M/ef).
    /// - Q=40 queries; recall@k vs FLAT; search wall times are median of 3 passes
    ///   (build excluded, timed once).
    ///
    /// Soft recall floors catch broken search when the bench is run; absolute ms
    /// are **not** gated. At this N, HNSW often beats FLAT on search wall time
    /// (host-dependent) — see `docs/benchmarks.md`.
    #[test]
    #[ignore = "large-N HNSW bench; run with --ignored (prefer --release)"]
    fn hnsw_recall_larger_n_median_throughput() {
        use rand::rngs::StdRng;
        use rand::SeedableRng;
        use std::time::Instant;

        const N: usize = 5000;
        const DIM: usize = 16;
        const Q: usize = 40;
        const M: usize = 16;
        const EF: usize = 100;
        const TIMING_PASSES: usize = 3;
        // Distinct from unit-gate seed so corpus/query streams are independent.
        const SEED: u64 = 0xD1_A6_E5_01;

        // Soft floors: correct HNSW on this seed is near-perfect; broken search fails.
        const MIN_RECALL_AT_1: f64 = 0.95;
        const MIN_RECALL_AT_10: f64 = 0.90;

        let mut rng = StdRng::seed_from_u64(SEED);
        let corpus: Vec<(Bytes, Vec<f32>)> = (0..N)
            .map(|i| (Bytes::from(format!("v{i}")), random_unit_vec(&mut rng, DIM)))
            .collect();
        let queries: Vec<Vec<f32>> = (0..Q).map(|_| random_unit_vec(&mut rng, DIM)).collect();

        let t_build = Instant::now();
        let mut flat = FlatVectorIndex::new(DistanceMetric::Cosine);
        let mut hnsw = HNSWIndex::new(M, EF, DistanceMetric::Cosine);
        for (id, v) in &corpus {
            flat.add(id.clone(), v.clone());
            hnsw.add(id.clone(), v.clone());
        }
        let build_ms = t_build.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(flat.len(), N);
        assert_eq!(hnsw.len(), N);

        // Recall from first pass; timings from median of TIMING_PASSES.
        let mut flat_ms_samples = Vec::with_capacity(TIMING_PASSES);
        let mut hnsw_ms_samples = Vec::with_capacity(TIMING_PASSES);
        let mut flat_top10: Vec<Vec<VectorSearchResult>> = Vec::new();
        let mut hnsw_top10: Vec<Vec<VectorSearchResult>> = Vec::new();

        for pass in 0..TIMING_PASSES {
            let t_flat = Instant::now();
            let flat_pass: Vec<Vec<VectorSearchResult>> =
                queries.iter().map(|q| flat.search(q, 10)).collect();
            flat_ms_samples.push(t_flat.elapsed().as_secs_f64() * 1000.0);

            let t_hnsw = Instant::now();
            let hnsw_pass: Vec<Vec<VectorSearchResult>> =
                queries.iter().map(|q| hnsw.search(q, 10)).collect();
            hnsw_ms_samples.push(t_hnsw.elapsed().as_secs_f64() * 1000.0);

            if pass == 0 {
                flat_top10 = flat_pass;
                hnsw_top10 = hnsw_pass;
            }
        }

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

        let flat_ms = median_f64(flat_ms_samples);
        let hnsw_ms = median_f64(hnsw_ms_samples);
        let speedup = if hnsw_ms > 0.0 {
            flat_ms / hnsw_ms
        } else {
            f64::INFINITY
        };

        eprintln!(
            "hnsw_recall_larger_n_median_throughput: N={N} dim={DIM} Q={Q} M={M} ef={EF} \
             mean_recall@1={mean_r1:.3} mean_recall@10={mean_r10:.3} \
             build={build_ms:.1}ms \
             flat_search_k10_median3={flat_ms:.3}ms hnsw_search_k10_median3={hnsw_ms:.3}ms \
             speedup≈{speedup:.2}× (median-of-{TIMING_PASSES}; indicative; not a CI ms gate)"
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

    /// Batch DL: fixed-seed recall@k after **remove + update churn** (vs FLAT).
    ///
    /// The always-on CV/DK gate builds once then searches; graph repair after
    /// delete/update is covered by structural HNSW unit tests but not recall@k.
    /// This micro builds HNSW+FLAT, removes a slice of ids, rewrites another
    /// slice in place (large vector move → remove+reinsert rewire), then asserts
    /// mean recall still clears a soft floor. CI-fast (N=120, Q=24).
    ///
    /// Soft floors (load-bearing but looser than the no-churn gate — bridge
    /// reconnect is heuristic, not exact ANN recovery):
    /// - mean recall@1  ≥ 0.90
    /// - mean recall@10 ≥ 0.85
    #[test]
    fn hnsw_recall_after_remove_update_churn() {
        use rand::rngs::StdRng;
        use rand::SeedableRng;

        const N: usize = 120;
        const DIM: usize = 16;
        const Q: usize = 24;
        const M: usize = 8;
        const EF: usize = 32;
        // Distinct from CV/DK unit-gate and larger-N seeds.
        const SEED: u64 = 0xD1_C4_01_77;
        const N_REMOVE: usize = 15;
        const N_UPDATE: usize = 15;

        const MIN_RECALL_AT_1: f64 = 0.90;
        const MIN_RECALL_AT_10: f64 = 0.85;

        let mut rng = StdRng::seed_from_u64(SEED);
        let mut corpus: Vec<(Bytes, Vec<f32>)> = (0..N)
            .map(|i| (Bytes::from(format!("v{i}")), random_unit_vec(&mut rng, DIM)))
            .collect();
        let queries: Vec<Vec<f32>> = (0..Q).map(|_| random_unit_vec(&mut rng, DIM)).collect();

        let mut flat = FlatVectorIndex::new(DistanceMetric::Cosine);
        let mut hnsw = HNSWIndex::new(M, EF, DistanceMetric::Cosine);
        for (id, v) in &corpus {
            flat.add(id.clone(), v.clone());
            hnsw.add(id.clone(), v.clone());
        }
        assert_eq!(flat.len(), N);
        assert_eq!(hnsw.len(), N);

        // Deterministic remove: first N_REMOVE corpus ids (fixed seed → fixed set).
        let remove_ids: Vec<Bytes> = corpus.iter().take(N_REMOVE).map(|(id, _)| id.clone()).collect();
        for id in &remove_ids {
            flat.remove(id);
            hnsw.remove(id);
        }

        // Update-in-place: next N_UPDATE survivors get far-away unit vectors.
        let update_ids: Vec<Bytes> = corpus
            .iter()
            .skip(N_REMOVE)
            .take(N_UPDATE)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &update_ids {
            let new_v = random_unit_vec(&mut rng, DIM);
            flat.add(id.clone(), new_v.clone());
            hnsw.add(id.clone(), new_v.clone());
            // Keep corpus in sync for debugging only (not re-used for search).
            if let Some(slot) = corpus.iter_mut().find(|(cid, _)| cid == id) {
                slot.1 = new_v;
            }
        }

        let expected_len = N - N_REMOVE;
        assert_eq!(flat.len(), expected_len);
        assert_eq!(hnsw.len(), expected_len);

        let mut sum_r1 = 0.0f64;
        let mut sum_r10 = 0.0f64;
        for (i, q) in queries.iter().enumerate() {
            let f = flat.search(q, 10);
            let h = hnsw.search(q, 10);
            assert_eq!(f.len(), 10, "FLAT must return k=10 for query {i}");
            assert_eq!(h.len(), 10, "HNSW must return k=10 for query {i}");
            sum_r1 += recall_at_k(&f, &h, 1);
            sum_r10 += recall_at_k(&f, &h, 10);
        }
        let mean_r1 = sum_r1 / Q as f64;
        let mean_r10 = sum_r10 / Q as f64;

        eprintln!(
            "hnsw_recall_after_remove_update_churn: N={N} remove={N_REMOVE} update={N_UPDATE} \
             dim={DIM} Q={Q} M={M} ef={EF} mean_recall@1={mean_r1:.3} mean_recall@10={mean_r10:.3}"
        );

        assert!(
            mean_r1 >= MIN_RECALL_AT_1,
            "post-churn mean recall@1 {mean_r1:.3} < {MIN_RECALL_AT_1} \
             (N={N} remove={N_REMOVE} update={N_UPDATE} M={M} ef={EF})"
        );
        assert!(
            mean_r10 >= MIN_RECALL_AT_10,
            "post-churn mean recall@10 {mean_r10:.3} < {MIN_RECALL_AT_10} \
             (N={N} remove={N_REMOVE} update={N_UPDATE} M={M} ef={EF})"
        );
    }

    // ── Batch FF: multi-layer insert ──────────────────────────────────────

    /// Forced levels place nodes on layers > 0; entry promotes when a higher
    /// level arrives; all nodes still present on layer 0.
    #[test]
    fn hnsw_multilayer_forced_levels_place_nodes_above_zero() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        // a@0, b@0, c@2 (new top), d@1
        index.enqueue_levels([0, 0, 2, 1]);
        index.add(Bytes::from("a"), vec![0.0, 0.0]);
        index.add(Bytes::from("b"), vec![1.0, 0.0]);
        index.add(Bytes::from("c"), vec![2.0, 0.0]);
        index.add(Bytes::from("d"), vec![3.0, 0.0]);

        assert_eq!(index.node_level(&Bytes::from("a")), Some(0));
        assert_eq!(index.node_level(&Bytes::from("b")), Some(0));
        assert_eq!(index.node_level(&Bytes::from("c")), Some(2));
        assert_eq!(index.node_level(&Bytes::from("d")), Some(1));

        // All nodes on layer 0.
        for id in ["a", "b", "c", "d"] {
            assert!(
                index.layers[0].neighbors.contains_key(&Bytes::from(id)),
                "layer 0 must contain {id}"
            );
        }
        // c on layers 0,1,2; d on 0,1; not on 2.
        assert!(index.layers[2].neighbors.contains_key(&Bytes::from("c")));
        assert!(!index.layers[2].neighbors.contains_key(&Bytes::from("d")));
        assert!(index.layers[1].neighbors.contains_key(&Bytes::from("d")));

        // Entry should be the highest-level node (c @ 2).
        assert_eq!(index.entry_point, Some(Bytes::from("c")));
        assert_eq!(index.highest_nonempty_layer(), 2);

        // Search still finds nearest (self at origin).
        let hits = index.search(&[0.0f32, 0.0], 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, Bytes::from("a"));
    }

    /// Seeded level RNG: large N produces some nodes on layers > 0 (high probability).
    #[test]
    fn hnsw_multilayer_seeded_inserts_use_upper_layers() {
        const N: usize = 200;
        const M: usize = 8;
        const EF: usize = 32;
        const SEED: u64 = 0xFF_00_BA_7C;

        let mut index = HNSWIndex::new(M, EF, DistanceMetric::L2).with_level_seed(SEED);
        for i in 0..N {
            index.add(
                Bytes::from(format!("n{i}")),
                vec![i as f32 * 0.01, (i % 11) as f32],
            );
        }
        assert_eq!(index.len(), N);

        let mut above_zero = 0usize;
        let mut max_lvl = 0usize;
        for i in 0..N {
            let id = Bytes::from(format!("n{i}"));
            let lvl = index.node_level(&id).expect("node present");
            max_lvl = max_lvl.max(lvl);
            if lvl > 0 {
                above_zero += 1;
            }
            // Every node is on layer 0.
            assert!(index.layers[0].neighbors.contains_key(&id));
        }

        assert!(
            above_zero > 0,
            "expected some nodes on layer > 0 with M={M} N={N} seed={SEED:#x} (got 0)"
        );
        assert!(
            max_lvl >= 1,
            "expected top level ≥ 1 (got {max_lvl}); multi-layer insert not active?"
        );
        assert_eq!(
            index.highest_nonempty_layer(),
            max_lvl,
            "index top layer should match max node level"
        );

        // Entry on the top layer.
        let entry = index.entry_point.as_ref().expect("entry");
        assert_eq!(index.node_level(entry), Some(max_lvl));

        // Search still returns k hits.
        let results = index.search(&[1.0f32, 0.0], 5);
        assert_eq!(results.len(), 5);
    }

    /// Remove of top-layer-only hub reassigns entry and trims empty upper layers;
    /// update-in-place (re-add) rewires multi-layer.
    #[test]
    fn hnsw_multilayer_remove_update_smoke() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        // hub@3 entry; several base nodes; mid@1
        index.enqueue_levels([3, 0, 0, 0, 1, 0]);
        index.add(Bytes::from("hub"), vec![0.0, 0.0]);
        index.add(Bytes::from("a"), vec![1.0, 0.0]);
        index.add(Bytes::from("b"), vec![2.0, 0.0]);
        index.add(Bytes::from("c"), vec![3.0, 0.0]);
        index.add(Bytes::from("mid"), vec![1.5, 0.0]);
        index.add(Bytes::from("d"), vec![4.0, 0.0]);

        assert_eq!(index.entry_point, Some(Bytes::from("hub")));
        assert_eq!(index.highest_nonempty_layer(), 3);
        assert!(index.layers.len() >= 4);

        // Remove the top hub — entry reassigns; upper empty layers trim.
        index.remove(&Bytes::from("hub"));
        assert!(!index.vectors.contains_key(&Bytes::from("hub")));
        assert!(index.entry_point.is_some());
        assert_ne!(index.entry_point, Some(Bytes::from("hub")));
        // mid was the next-highest (level 1); top should be ≤ 1 after trim.
        assert!(
            index.highest_nonempty_layer() <= 1,
            "top after hub remove should be ≤ 1, got {}",
            index.highest_nonempty_layer()
        );
        // No residue of hub on any layer.
        for (li, layer) in index.layers.iter().enumerate() {
            assert!(
                !layer.neighbors.contains_key(&Bytes::from("hub")),
                "hub residue on layer {li}"
            );
            for (id, neighs) in &layer.neighbors {
                assert!(
                    !neighs.iter().any(|n| n == &Bytes::from("hub")),
                    "stale hub edge from {:?} on layer {li}",
                    id
                );
            }
        }

        // Survivors still searchable.
        let hits = index.search(&[1.0f32, 0.0], 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, Bytes::from("a"));

        // Update mid → far location; rewire must place it near far query.
        index.enqueue_levels([1]); // re-add draws a fresh level from queue
        index.add(Bytes::from("mid"), vec![100.0, 0.0]);
        assert_eq!(
            index.vectors.get(&Bytes::from("mid")),
            Some(&vec![100.0, 0.0])
        );
        let far = index.search(&[100.0f32, 0.0], 1);
        assert_eq!(far.len(), 1);
        assert_eq!(
            far[0].doc_id,
            Bytes::from("mid"),
            "updated mid should rank #1 near new location"
        );
    }

    /// Upper-layer edges exist after multi-layer insert (not only layer-0 shells).
    #[test]
    fn hnsw_multilayer_upper_layer_has_edges() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        // Two nodes on layer 1 so they can link above the base.
        index.enqueue_levels([1, 1, 0, 0, 0]);
        for (id, v) in [
            ("p", vec![0.0f32, 0.0]),
            ("q", vec![1.0, 0.0]),
            ("r", vec![2.0, 0.0]),
            ("s", vec![3.0, 0.0]),
            ("t", vec![4.0, 0.0]),
        ] {
            index.add(Bytes::from(id), v);
        }
        assert!(
            index.layers.len() >= 2,
            "expected at least layers 0 and 1"
        );
        let upper_edges: usize = index.layers[1]
            .neighbors
            .values()
            .map(|v| v.len())
            .sum();
        assert!(
            upper_edges > 0,
            "layer 1 should have edges between multi-level nodes (got 0)"
        );
        // p and q both on layer 1.
        assert!(index.layers[1].neighbors.contains_key(&Bytes::from("p")));
        assert!(index.layers[1].neighbors.contains_key(&Bytes::from("q")));
    }

    // ── Batch FV: durable graph snapshot ──────────────────────────────────

    /// Forced multi-layer graph: snapshot → fresh index with same vectors →
    /// apply → levels, entry, and edges match (canonical neighbor order).
    #[test]
    fn hnsw_graph_snapshot_roundtrip_forced_levels() {
        let mut src = HNSWIndex::new(4, 32, DistanceMetric::L2);
        src.enqueue_levels([0, 0, 2, 1, 0]);
        src.add(Bytes::from("a"), vec![0.0, 0.0]);
        src.add(Bytes::from("b"), vec![1.0, 0.0]);
        src.add(Bytes::from("c"), vec![2.0, 0.0]);
        src.add(Bytes::from("d"), vec![3.0, 0.0]);
        src.add(Bytes::from("e"), vec![4.0, 0.0]);

        let snap = src.snapshot_graph();
        assert_eq!(snap.entry_point, Some(Bytes::from("c")));
        assert_eq!(snap.levels.len(), 5);
        assert_eq!(src.node_level(&Bytes::from("c")), Some(2));
        assert!(!snap.layers.is_empty());

        // Fresh index: install vectors only (no graph), then apply snapshot.
        let mut dst = HNSWIndex::new(4, 32, DistanceMetric::L2);
        for (id, v) in src.iter_vectors() {
            dst.install_vector(id.clone(), v.clone());
        }
        dst.apply_graph_snapshot(&snap).expect("apply");

        assert_eq!(dst.snapshot_graph(), snap);
        assert_eq!(dst.entry_point, Some(Bytes::from("c")));
        assert_eq!(dst.node_level(&Bytes::from("c")), Some(2));
        assert_eq!(dst.node_level(&Bytes::from("d")), Some(1));
        assert_eq!(dst.node_level(&Bytes::from("a")), Some(0));

        // Search still works after exact restore.
        let hits = dst.search(&[0.0f32, 0.0], 1);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, Bytes::from("a"));
    }

    /// apply_graph_snapshot rejects neighbor ids not present in vectors.
    #[test]
    fn hnsw_graph_snapshot_rejects_missing_neighbor() {
        let mut idx = HNSWIndex::new(4, 32, DistanceMetric::L2);
        idx.install_vector(Bytes::from("a"), vec![0.0, 0.0]);
        idx.install_vector(Bytes::from("b"), vec![1.0, 0.0]);
        let bad = HnswGraphSnapshot {
            entry_point: Some(Bytes::from("a")),
            levels: vec![(Bytes::from("a"), 0), (Bytes::from("b"), 0)],
            layers: vec![vec![
                (Bytes::from("a"), vec![Bytes::from("ghost")]),
                (Bytes::from("b"), vec![]),
            ]],
        };
        let err = idx.apply_graph_snapshot(&bad).unwrap_err();
        assert!(
            err.contains("missing"),
            "expected missing-neighbor error, got {err}"
        );
    }

    /// Snapshot equality is stable under re-export (canonical neighbor sort).
    #[test]
    fn hnsw_graph_snapshot_canonical_reexport() {
        let mut index = HNSWIndex::new(4, 32, DistanceMetric::L2);
        index.enqueue_levels([1, 1, 0]);
        index.add(Bytes::from("x"), vec![0.0, 0.0]);
        index.add(Bytes::from("y"), vec![1.0, 0.0]);
        index.add(Bytes::from("z"), vec![2.0, 0.0]);
        let s1 = index.snapshot_graph();
        index.apply_graph_snapshot(&s1).unwrap();
        let s2 = index.snapshot_graph();
        assert_eq!(s1, s2);
    }
}
