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

    fn add_node(&mut self, node_id: Bytes) {
        self.neighbors.entry(node_id).or_insert_with(Vec::new);
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

    /// Add a vector to the index.
    ///
    /// Neighbors are selected via graph search **before** the vector is stored, so the
    /// new node is never chosen as its own neighbor. Existing IDs only update the vector.
    pub fn add(&mut self, doc_id: Bytes, vector: Vec<f32>) {
        // Update-in-place for existing documents (keep graph wiring).
        if self.vectors.contains_key(&doc_id) {
            self.vectors.insert(doc_id, vector);
            return;
        }

        // Simplified multi-layer: all nodes live on layer 0.
        let layer = 0;
        while self.layers.len() <= layer {
            self.layers.push(HNSWLayer::new());
        }

        // First node becomes the entry point; no edges yet.
        if self.entry_point.is_none() {
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

        // Insert vector + node, then bidirectional edges (no self-loops).
        self.vectors.insert(doc_id.clone(), vector);
        self.layers[layer].add_node(doc_id.clone());

        for neighbor in &neighbors {
            debug_assert_ne!(neighbor, &doc_id, "neighbor selection must exclude self");
            self.layers[layer].add_edge(doc_id.clone(), neighbor.clone());
            self.layers[layer].add_edge(neighbor.clone(), doc_id.clone());
            // Cap degree on the existing neighbor (simple M-prune, not full HNSW heuristic).
            self.prune_neighbors(neighbor, layer, self.m);
        }
        self.prune_neighbors(&doc_id, layer, self.m);
    }

    /// Remove a vector from the index
    pub fn remove(&mut self, doc_id: &Bytes) {
        self.vectors.remove(doc_id);
        // Note: In a full implementation, we'd also clean up the graph connections
        if self.entry_point.as_ref() == Some(doc_id) {
            self.entry_point = self.vectors.keys().next().cloned();
        }
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
    fn prune_neighbors(&mut self, node_id: &Bytes, layer: usize, max_m: usize) {
        let Some(layer_ref) = self.layers.get(layer) else {
            return;
        };
        let neighbors = layer_ref.get_neighbors(node_id);
        if neighbors.len() <= max_m {
            return;
        }
        let Some(node_vec) = self.vectors.get(node_id).cloned() else {
            return;
        };

        let mut scored: Vec<(Bytes, f32)> = neighbors
            .into_iter()
            .filter(|n| n != node_id)
            .filter_map(|n| {
                self.vectors
                    .get(&n)
                    .map(|v| (n, self.compute_distance(&node_vec, v)))
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        // Dedup by id (keep first = closest)
        let mut seen = HashSet::new();
        scored.retain(|(id, _)| seen.insert(id.clone()));
        let kept: Vec<Bytes> = scored.into_iter().take(max_m).map(|(id, _)| id).collect();
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
}
