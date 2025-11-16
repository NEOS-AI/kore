use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use crate::search_index::{SearchIndex, DocumentField, DistanceMetric};

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
        let mut candidates = if let Some(ref filter) = query.filter {
            Self::apply_filter(index, filter)
        } else {
            // No filter: return all documents
            index.get_documents().clone()
        };

        // Step 2: For vector queries, compute scores
        let mut scored_results: Vec<(Bytes, Option<f32>)> = if let Some(ref filter) = query.filter {
            if let Some(scores) = Self::get_vector_scores(index, filter) {
                candidates
                    .into_iter()
                    .filter_map(|doc_id| {
                        scores.get(&doc_id).map(|&score| (doc_id, Some(score)))
                    })
                    .collect()
            } else {
                candidates.into_iter().map(|doc_id| (doc_id, None)).collect()
            }
        } else {
            candidates.into_iter().map(|doc_id| (doc_id, None)).collect()
        };

        // Step 3: Sort results
        if let Some(ref sort_by) = query.sort_by {
            Self::sort_results(&mut scored_results, sort_by, document_data);
        } else if scored_results.iter().any(|(_, score)| score.is_some()) {
            // If we have vector scores, sort by score (descending)
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
                if let Some(vector_index) = index.get_vector_index(field) {
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

    /// Perform K-Nearest Neighbor search
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

    /// Get vector similarity scores for results
    fn get_vector_scores(
        index: &SearchIndex,
        filter: &QueryFilter,
    ) -> Option<HashMap<Bytes, f32>> {
        match filter {
            QueryFilter::Vector { field, vector, k: _, distance_metric } => {
                if let Some(vector_index) = index.get_vector_index(field) {
                    let scores: HashMap<Bytes, f32> = vector_index
                        .iter()
                        .map(|(doc_id, doc_vector)| {
                            let score = Self::compute_similarity(vector, doc_vector, distance_metric);
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
    use crate::search_index::{IndexDefinition, FieldDefinition, FieldType};

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
}
