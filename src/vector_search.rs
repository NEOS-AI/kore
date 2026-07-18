use bytes::Bytes;
use std::collections::HashMap;
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

    fn add_edge(&mut self, from: Bytes, to: Bytes) {
        self.neighbors.entry(from).or_insert_with(Vec::new).push(to);
    }

    fn get_neighbors(&self, node_id: &Bytes) -> Vec<Bytes> {
        self.neighbors.get(node_id).cloned().unwrap_or_default()
    }
}

/// HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search
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
            m,
            ef_construction,
            ef_search: ef_construction,
            distance_metric,
            entry_point: None,
        }
    }

    /// Add a vector to the index
    pub fn add(&mut self, doc_id: Bytes, vector: Vec<f32>) {
        // Store the vector
        self.vectors.insert(doc_id.clone(), vector.clone());

        // Determine the layer for this node (simplified: all go to layer 0 for now)
        // In a full implementation, we'd use exponential decay to assign layers
        let layer = 0;

        // Ensure we have enough layers
        while self.layers.len() <= layer {
            self.layers.push(HNSWLayer::new());
        }

        // Add node to layer
        self.layers[layer].add_node(doc_id.clone());

        // If this is the first node, make it the entry point
        if self.entry_point.is_none() {
            self.entry_point = Some(doc_id.clone());
            return;
        }

        // Find neighbors and connect
        let neighbors = self.find_neighbors(&vector, self.m, layer);
        for neighbor in neighbors {
            self.layers[layer].add_edge(doc_id.clone(), neighbor.clone());
            self.layers[layer].add_edge(neighbor, doc_id.clone());
        }
    }

    /// Remove a vector from the index
    pub fn remove(&mut self, doc_id: &Bytes) {
        self.vectors.remove(doc_id);
        // Note: In a full implementation, we'd also clean up the graph connections
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query_vector: &[f32], k: usize) -> Vec<VectorSearchResult> {
        if self.vectors.is_empty() || self.entry_point.is_none() {
            return Vec::new();
        }

        // For simplicity, we'll do a brute-force search on layer 0
        // In a full HNSW implementation, we'd navigate the graph from top to bottom
        let mut results: Vec<VectorSearchResult> = self.vectors
            .iter()
            .map(|(doc_id, vector)| {
                let distance = self.compute_distance(query_vector, vector);
                VectorSearchResult {
                    doc_id: doc_id.clone(),
                    score: self.distance_to_score(distance),
                    vector: vector.clone(),
                }
            })
            .collect();

        // Sort by score (higher is better)
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

        results.into_iter().take(k).collect()
    }

    /// Find neighbors for a vector at a given layer
    fn find_neighbors(&self, vector: &[f32], m: usize, layer: usize) -> Vec<Bytes> {
        let mut candidates: Vec<(Bytes, f32)> = self.vectors
            .iter()
            .map(|(doc_id, doc_vector)| {
                let distance = self.compute_distance(vector, doc_vector);
                (doc_id.clone(), distance)
            })
            .collect();

        // Sort by distance (lower is better for distance)
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top m neighbors
        candidates
            .into_iter()
            .take(m)
            .map(|(doc_id, _)| doc_id)
            .collect()
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
        let mut results: Vec<VectorSearchResult> = self.vectors
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
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

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
    /// check; not a throughput benchmark — see `docs/benchmarks.md`).
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
}
