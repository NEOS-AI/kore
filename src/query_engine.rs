use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use crate::search_index::{SearchIndex, DocumentField, DistanceMetric};
use crate::vector_search::HNSWIndex;

/// Query operators
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOperator {
    And,
    Or,
    Not,
}

/// Query filter types
#[derive(Debug, Clone)]
pub enum QueryFilter {
    /// Text search on a field
    Text {
        field: String,
        terms: Vec<String>,
        operator: QueryOperator,
    },
    /// Numeric range filter
    NumericRange {
        field: String,
        min: f64,
        max: f64,
    },
    /// Tag filter
    Tag {
        field: String,
        tag: String,
    },
    /// Vector similarity search
    Vector {
        field: String,
        vector: Vec<f32>,
        k: usize,
        distance_metric: DistanceMetric,
    },
    /// Combination of filters
    Compound {
        operator: QueryOperator,
        filters: Vec<QueryFilter>,
    },
}

/// Sort order
#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Sort specification
#[derive(Debug, Clone)]
pub struct SortBy {
    pub field: String,
    pub order: SortOrder,
}

/// Query specification
#[derive(Debug, Clone)]
pub struct Query {
    pub filter: Option<QueryFilter>,
    pub sort_by: Option<SortBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl Query {
    pub fn new() -> Self {
        Self {
            filter: None,
            sort_by: None,
            limit: None,
            offset: None,
        }
    }

    pub fn with_filter(mut self, filter: QueryFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_sort(mut self, sort_by: SortBy) -> Self {
        self.sort_by = Some(sort_by);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}

/// Query parser for Redis-like search syntax
pub struct QueryParser;

impl QueryParser {
    /// Parse a simple query string like "hello world" or "@field:value"
    pub fn parse_simple(query: &str) -> Result<Query, String> {
        let terms: Vec<String> = query
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        if terms.is_empty() {
            return Ok(Query::new());
        }

        // Check for field-specific queries (@field:value)
        if let Some(first) = terms.first() {
            if first.starts_with('@') {
                if let Some(colon_pos) = first.find(':') {
                    let field = first[1..colon_pos].to_string();
                    let value = first[colon_pos + 1..].to_string();

                    return Ok(Query::new().with_filter(QueryFilter::Text {
                        field,
                        terms: vec![value],
                        operator: QueryOperator::And,
                    }));
                }
            }
        }

        // Default: full-text search on all text fields
        Ok(Query::new())
    }

    /// Parse numeric range like "[10 20]" or "(10 +inf)"
    pub fn parse_numeric_range(range_str: &str) -> Result<(f64, f64), String> {
        let trimmed = range_str.trim();

        if !((trimmed.starts_with('[') || trimmed.starts_with('(')) &&
             (trimmed.ends_with(']') || trimmed.ends_with(')'))) {
            return Err("Invalid range format".to_string());
        }

        let inner = &trimmed[1..trimmed.len() - 1];
        let parts: Vec<&str> = inner.split_whitespace().collect();

        if parts.len() != 2 {
            return Err("Range must have exactly 2 values".to_string());
        }

        let min = if parts[0] == "-inf" {
            f64::NEG_INFINITY
        } else {
            parts[0].parse().map_err(|_| "Invalid min value")?
        };

        let max = if parts[1] == "+inf" || parts[1] == "inf" {
            f64::INFINITY
        } else {
            parts[1].parse().map_err(|_| "Invalid max value")?
        };

        Ok((min, max))
    }
}

/// Query executor
pub struct QueryExecutor;

impl QueryExecutor {
    /// Execute a query on a search index
    pub fn execute(
        index: &SearchIndex,
        query: &Query,
        document_data: &HashMap<Bytes, HashMap<String, DocumentField>>,
    ) -> Vec<(Bytes, Option<f32>)> {
        // Step 1: Apply filters to get candidate documents
        let candidates = if let Some(ref filter) = query.filter {
            Self::apply_filter(index, filter)
        } else {
            // No filter: return all documents
            index.get_documents().clone()
        };

        // Step 2: Score candidates — vector similarity and/or text TF-IDF (Batch GT).
        let mut scored_results: Vec<(Bytes, Option<f32>)> = if let Some(ref filter) = query.filter {
            let vector_scores = Self::get_vector_scores(index, filter);
            let text_scores = Self::get_text_scores(index, filter, &candidates);
            match (vector_scores, text_scores) {
                (Some(vs), Some(ts)) => {
                    // Prefer vector scores for docs that have them; else TF-IDF.
                    candidates
                        .into_iter()
                        .map(|doc_id| {
                            let score = vs
                                .get(&doc_id)
                                .copied()
                                .or_else(|| ts.get(&doc_id).copied());
                            (doc_id, score)
                        })
                        .collect()
                }
                (Some(vs), None) => candidates
                    .into_iter()
                    .filter_map(|doc_id| {
                        vs.get(&doc_id).map(|&score| (doc_id, Some(score)))
                    })
                    .collect(),
                (None, Some(ts)) => candidates
                    .into_iter()
                    .map(|doc_id| {
                        let score = ts.get(&doc_id).copied();
                        (doc_id, score)
                    })
                    .collect(),
                (None, None) => {
                    candidates.into_iter().map(|doc_id| (doc_id, None)).collect()
                }
            }
        } else {
            candidates.into_iter().map(|doc_id| (doc_id, None)).collect()
        };

        // Step 3: Sort results
        if let Some(ref sort_by) = query.sort_by {
            Self::sort_results(&mut scored_results, sort_by, document_data);
        } else if scored_results.iter().any(|(_, score)| score.is_some()) {
            // Vector or TF-IDF scores: sort by score descending (higher is better).
            scored_results.sort_by(|a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Step 4: Apply offset and limit
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(usize::MAX);

        scored_results
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect()
    }

    /// Apply a filter to get candidate documents
    fn apply_filter(index: &SearchIndex, filter: &QueryFilter) -> HashSet<Bytes> {
        match filter {
            QueryFilter::Text { field, terms, operator } => {
                if let Some(text_index) = index.get_text_index(field) {
                    match operator {
                        QueryOperator::And => text_index.search_and(terms),
                        QueryOperator::Or => text_index.search_or(terms),
                        QueryOperator::Not => {
                            let matches = text_index.search_or(terms);
                            index.get_documents()
                                .iter()
                                .filter(|doc| !matches.contains(*doc))
                                .cloned()
                                .collect()
                        }
                    }
                } else {
                    HashSet::new()
                }
            }
            QueryFilter::NumericRange { field, min, max } => {
                if let Some(numeric_index) = index.get_numeric_index(field) {
                    numeric_index.range(*min, *max)
                } else {
                    HashSet::new()
                }
            }
            QueryFilter::Tag { field, tag } => {
                if let Some(tag_index) = index.get_tag_index(field) {
                    tag_index.search(tag)
                } else {
                    HashSet::new()
                }
            }
            QueryFilter::Vector { field, vector, k, distance_metric } => {
                // Batch FW: prefer dual-written HNSW ANN when the graph has data.
                // Flat algorithm / empty HNSW fall back to exact flat-map scan.
                if let Some(hnsw) = index.get_hnsw_index(field) {
                    Self::knn_search_hnsw(hnsw, vector, *k)
                } else if let Some(vector_index) = index.get_vector_index(field) {
                    Self::knn_search(vector_index, vector, *k, distance_metric)
                } else {
                    HashSet::new()
                }
            }
            QueryFilter::Compound { operator, filters } => {
                if filters.is_empty() {
                    return HashSet::new();
                }

                let mut result = Self::apply_filter(index, &filters[0]);

                for filter in &filters[1..] {
                    let filter_result = Self::apply_filter(index, filter);

                    result = match operator {
                        QueryOperator::And => {
                            result.intersection(&filter_result).cloned().collect()
                        }
                        QueryOperator::Or => {
                            result.union(&filter_result).cloned().collect()
                        }
                        QueryOperator::Not => {
                            result.difference(&filter_result).cloned().collect()
                        }
                    };
                }

                result
            }
        }
    }

    /// Perform exact K-Nearest Neighbor search over a flat vector map.
    fn knn_search(
        vector_index: &HashMap<Bytes, Vec<f32>>,
        query_vector: &[f32],
        k: usize,
        distance_metric: &DistanceMetric,
    ) -> HashSet<Bytes> {
        let mut scored_docs: Vec<(Bytes, f32)> = vector_index
            .iter()
            .map(|(doc_id, doc_vector)| {
                let score = Self::compute_similarity(query_vector, doc_vector, distance_metric);
                (doc_id.clone(), score)
            })
            .collect();

        // Sort by score (higher is better for similarity)
        scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top k
        scored_docs
            .into_iter()
            .take(k)
            .map(|(doc_id, _)| doc_id)
            .collect()
    }

    /// Approximate KNN via dual-written HNSW graph (Batch FW).
    ///
    /// Walks graph edges only — disconnected nodes are not returned even when
    /// closer in the flat map. Scores use the same Cosine/L2/IP mapping as
    /// [`HNSWIndex::search`] / [`Self::compute_similarity`].
    fn knn_search_hnsw(
        hnsw: &HNSWIndex,
        query_vector: &[f32],
        k: usize,
    ) -> HashSet<Bytes> {
        hnsw.search(query_vector, k)
            .into_iter()
            .map(|r| r.doc_id)
            .collect()
    }

    /// Get vector similarity scores for results
    fn get_vector_scores(
        index: &SearchIndex,
        filter: &QueryFilter,
    ) -> Option<HashMap<Bytes, f32>> {
        match filter {
            QueryFilter::Vector { field, vector, k, distance_metric } => {
                // Prefer HNSW ANN scores when the dual-written graph has data
                // (Batch FW). `k` bounds the ANN result set; flat path scores
                // the whole map (exact) for FLAT / empty-HNSW fallback.
                if let Some(hnsw) = index.get_hnsw_index(field) {
                    let scores: HashMap<Bytes, f32> = hnsw
                        .search(vector, *k)
                        .into_iter()
                        .map(|r| (r.doc_id, r.score))
                        .collect();
                    Some(scores)
                } else if let Some(vector_index) = index.get_vector_index(field) {
                    let scores: HashMap<Bytes, f32> = vector_index
                        .iter()
                        .map(|(doc_id, doc_vector)| {
                            let score =
                                Self::compute_similarity(vector, doc_vector, distance_metric);
                            (doc_id.clone(), score)
                        })
                        .collect();
                    Some(scores)
                } else {
                    None
                }
            }
            QueryFilter::Compound { filters, .. } => {
                // Check if any sub-filter is a vector query
                for filter in filters {
                    if let Some(scores) = Self::get_vector_scores(index, filter) {
                        return Some(scores);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Field-weighted TF-IDF scores for text filters (Batch GT).
    ///
    /// Compound OR of text fields sums per-field scores; AND takes the
    /// intersection candidates already computed and sums field contributions.
    fn get_text_scores(
        index: &SearchIndex,
        filter: &QueryFilter,
        candidates: &HashSet<Bytes>,
    ) -> Option<HashMap<Bytes, f32>> {
        match filter {
            QueryFilter::Text { field, terms, operator } => {
                // NOT queries: no meaningful TF-IDF ranking of non-matches.
                if matches!(operator, QueryOperator::Not) {
                    return None;
                }
                let text_index = index.get_text_index(field)?;
                let scores = text_index.score_tf_idf(terms, candidates);
                if scores.is_empty() {
                    None
                } else {
                    Some(scores)
                }
            }
            QueryFilter::Compound { operator, filters } => {
                let mut any = false;
                let mut combined: HashMap<Bytes, f32> = HashMap::new();
                for sub in filters {
                    if let Some(scores) = Self::get_text_scores(index, sub, candidates) {
                        any = true;
                        match operator {
                            QueryOperator::Or | QueryOperator::And => {
                                for (doc, s) in scores {
                                    *combined.entry(doc).or_insert(0.0) += s;
                                }
                            }
                            QueryOperator::Not => {}
                        }
                    }
                }
                if any {
                    Some(combined)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Compute similarity between two vectors
    pub fn compute_similarity(
        vec1: &[f32],
        vec2: &[f32],
        metric: &DistanceMetric,
    ) -> f32 {
        if vec1.len() != vec2.len() {
            return 0.0;
        }

        match metric {
            DistanceMetric::Cosine => Self::cosine_similarity(vec1, vec2),
            DistanceMetric::L2 => {
                // Convert L2 distance to similarity (higher is better)
                let distance = Self::l2_distance(vec1, vec2);
                1.0 / (1.0 + distance)
            }
            DistanceMetric::IP => Self::inner_product(vec1, vec2),
        }
    }

    /// Cosine similarity
    fn cosine_similarity(vec1: &[f32], vec2: &[f32]) -> f32 {
        let dot_product: f32 = vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum();
        let norm1: f32 = vec1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm2: f32 = vec2.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm1 == 0.0 || norm2 == 0.0 {
            return 0.0;
        }

        dot_product / (norm1 * norm2)
    }

    /// L2 (Euclidean) distance
    fn l2_distance(vec1: &[f32], vec2: &[f32]) -> f32 {
        vec1.iter()
            .zip(vec2.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    /// Inner product
    fn inner_product(vec1: &[f32], vec2: &[f32]) -> f32 {
        vec1.iter().zip(vec2.iter()).map(|(a, b)| a * b).sum()
    }

    /// Sort results
    fn sort_results(
        results: &mut [(Bytes, Option<f32>)],
        sort_by: &SortBy,
        document_data: &HashMap<Bytes, HashMap<String, DocumentField>>,
    ) {
        results.sort_by(|a, b| {
            let a_value = document_data
                .get(&a.0)
                .and_then(|fields| fields.get(&sort_by.field));
            let b_value = document_data
                .get(&b.0)
                .and_then(|fields| fields.get(&sort_by.field));

            let cmp = match (a_value, b_value) {
                (Some(DocumentField::Numeric(a_num)), Some(DocumentField::Numeric(b_num))) => {
                    a_num.partial_cmp(b_num).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Some(DocumentField::Text(a_text)), Some(DocumentField::Text(b_text))) => {
                    a_text.cmp(b_text)
                }
                _ => std::cmp::Ordering::Equal,
            };

            match sort_by.order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_index::{
        FieldDefinition, FieldType, IndexDefinition, VectorAlgorithm,
    };
    use crate::vector_search::HnswGraphSnapshot;
    use std::collections::HashMap;

    #[test]
    fn test_query_parser() {
        let query = QueryParser::parse_simple("hello world").unwrap();
        assert!(query.filter.is_none());

        let query = QueryParser::parse_simple("@title:hello").unwrap();
        assert!(query.filter.is_some());
    }

    #[test]
    fn test_numeric_range_parser() {
        let (min, max) = QueryParser::parse_numeric_range("[10 20]").unwrap();
        assert_eq!(min, 10.0);
        assert_eq!(max, 20.0);

        let (min, max) = QueryParser::parse_numeric_range("(-inf +inf)").unwrap();
        assert_eq!(min, f64::NEG_INFINITY);
        assert_eq!(max, f64::INFINITY);
    }

    #[test]
    fn test_cosine_similarity() {
        let vec1 = vec![1.0, 0.0, 0.0];
        let vec2 = vec![1.0, 0.0, 0.0];
        let sim = QueryExecutor::cosine_similarity(&vec1, &vec2);
        assert!((sim - 1.0).abs() < 0.001);

        let vec3 = vec![0.0, 1.0, 0.0];
        let sim = QueryExecutor::cosine_similarity(&vec1, &vec3);
        assert!((sim - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_l2_distance() {
        let vec1 = vec![0.0, 0.0];
        let vec2 = vec![3.0, 4.0];
        let dist = QueryExecutor::l2_distance(&vec1, &vec2);
        assert!((dist - 5.0).abs() < 0.001);
    }

    fn hnsw_index_def(name: &str) -> IndexDefinition {
        IndexDefinition::new(
            name.to_string(),
            vec![],
            vec![FieldDefinition {
                name: "emb".to_string(),
                field_type: FieldType::Vector {
                    algorithm: VectorAlgorithm::HNSW {
                        m: 8,
                        ef_construction: 64,
                    },
                    dimensions: 2,
                    distance_metric: DistanceMetric::L2,
                },
            }],
        )
    }

    fn flat_index_def(name: &str) -> IndexDefinition {
        IndexDefinition::new(
            name.to_string(),
            vec![],
            vec![FieldDefinition {
                name: "emb".to_string(),
                field_type: FieldType::Vector {
                    algorithm: VectorAlgorithm::Flat,
                    dimensions: 2,
                    distance_metric: DistanceMetric::L2,
                },
            }],
        )
    }

    /// Batch FW: crafted connectivity where flat exact top-1 differs from HNSW
    /// edge walk — query engine must use HNSW (not full scan).
    #[test]
    fn knn_uses_hnsw_not_flat_scan_when_graph_present() {
        let mut index = SearchIndex::new(hnsw_index_def("vec"));

        // Index four points; dual-write builds an HNSW graph.
        let docs = [
            ("entry", vec![0.0f32, 0.0]),
            ("near", vec![1.0, 0.0]),
            ("mid", vec![10.0, 0.0]),
            ("far_isolated", vec![0.1, 0.0]),
        ];
        for (id, v) in &docs {
            let mut fields = HashMap::new();
            fields.insert("emb".to_string(), DocumentField::Vector(v.clone()));
            index.index_document(Bytes::from(*id), fields);
        }

        // Rewrite graph: entry -- mid -- near; leave far_isolated disconnected.
        // Flat KNN would rank far_isolated #1 for query [0.1, 0]; HNSW must not.
        let snap = HnswGraphSnapshot {
            entry_point: Some(Bytes::from("entry")),
            levels: vec![
                (Bytes::from("entry"), 0),
                (Bytes::from("far_isolated"), 0),
                (Bytes::from("mid"), 0),
                (Bytes::from("near"), 0),
            ],
            layers: vec![vec![
                (Bytes::from("entry"), vec![Bytes::from("mid")]),
                (Bytes::from("far_isolated"), vec![]),
                (Bytes::from("mid"), vec![Bytes::from("entry"), Bytes::from("near")]),
                (Bytes::from("near"), vec![Bytes::from("mid")]),
            ]],
        };
        index.apply_hnsw_graph("emb", &snap).expect("apply crafted graph");

        // Sanity: flat map still prefers isolated closer point.
        let flat = index.get_vector_index("emb").expect("flat map");
        let flat_top = QueryExecutor::knn_search(flat, &[0.1f32, 0.0], 1, &DistanceMetric::L2);
        assert!(
            flat_top.contains(&Bytes::from("far_isolated")),
            "flat exact path must prefer far_isolated"
        );

        // HNSW API: same craft excludes isolated.
        let hnsw = index.get_hnsw_index("emb").expect("non-empty HNSW");
        let hnsw_top = hnsw.search(&[0.1f32, 0.0], 1);
        assert_eq!(hnsw_top.len(), 1);
        assert_ne!(hnsw_top[0].doc_id, Bytes::from("far_isolated"));
        assert_eq!(hnsw_top[0].doc_id, Bytes::from("entry"));

        // Query engine VECTOR filter must follow HNSW, not flat.
        let query = Query::new()
            .with_filter(QueryFilter::Vector {
                field: "emb".to_string(),
                vector: vec![0.1, 0.0],
                k: 1,
                distance_metric: DistanceMetric::L2,
            })
            .with_limit(1);
        let doc_data = HashMap::new();
        let results = QueryExecutor::execute(&index, &query, &doc_data);
        assert_eq!(results.len(), 1, "expected single KNN hit: {:?}", results);
        assert_eq!(
            results[0].0,
            Bytes::from("entry"),
            "query engine must use HNSW ANN (got {:?})",
            results[0].0
        );
        // Score path: L2 similarity 1/(1+d); entry at [0,0] → d=0.1.
        let score = results[0].1.expect("vector score");
        let expected = 1.0 / (1.0 + 0.1);
        assert!(
            (score - expected).abs() < 1e-5,
            "score {} vs expected {} (HNSW L2 distance_to_score)",
            score,
            expected
        );
    }

    /// FLAT VECTOR fields never get an HNSW graph; knn stays exact flat.
    #[test]
    fn knn_flat_algorithm_stays_exact() {
        let mut index = SearchIndex::new(flat_index_def("flat_vec"));
        assert!(index.get_hnsw_index("emb").is_none());

        for (id, v) in [
            ("a", vec![0.0f32, 0.0]),
            ("b", vec![1.0, 0.0]),
            ("c", vec![0.05, 0.0]),
        ] {
            let mut fields = HashMap::new();
            fields.insert("emb".to_string(), DocumentField::Vector(v));
            index.index_document(Bytes::from(id), fields);
        }

        let query = Query::new()
            .with_filter(QueryFilter::Vector {
                field: "emb".to_string(),
                vector: vec![0.0, 0.0],
                k: 1,
                distance_metric: DistanceMetric::L2,
            })
            .with_limit(1);
        let results = QueryExecutor::execute(&index, &query, &HashMap::new());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, Bytes::from("a"));
    }

    /// Empty HNSW graph falls back to flat map when present.
    #[test]
    fn knn_falls_back_to_flat_when_hnsw_empty() {
        // HNSW schema but no documents → get_hnsw_index is None; no flat data either.
        let index = SearchIndex::new(hnsw_index_def("empty"));
        assert!(index.get_hnsw_index("emb").is_none());
        assert!(index.get_vector_index("emb").is_none());

        let query = Query::new().with_filter(QueryFilter::Vector {
            field: "emb".to_string(),
            vector: vec![0.0, 0.0],
            k: 3,
            distance_metric: DistanceMetric::L2,
        });
        let results = QueryExecutor::execute(&index, &query, &HashMap::new());
        assert!(results.is_empty());
    }
}
