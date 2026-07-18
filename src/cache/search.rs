use crate::cache::Cache;
use crate::memory::MemoryCategory;
use crate::search_index::{IndexDefinition, DocumentField, SearchIndex};
use crate::query_engine::{QueryParser, QueryExecutor};
use bytes::Bytes;
use std::collections::HashMap;

/// Search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub total: usize,
    pub documents: Vec<SearchDocument>,
}

/// Search document
#[derive(Debug, Clone)]
pub struct SearchDocument {
    pub id: Bytes,
    pub fields: HashMap<String, DocumentField>,
    pub score: Option<f32>,
}

/// Index info
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub num_docs: usize,
    pub num_fields: usize,
    pub fields: Vec<String>,
}

impl Cache {
    /// Create a new search index
    pub fn create_search_index(&self, definition: IndexDefinition) -> Result<(), String> {
        self.search_index_manager.create_index(definition)
    }

    /// Drop a search index and free its tracked memory
    pub fn drop_search_index(&self, name: &str) -> Result<(), String> {
        let size = self
            .search_index_manager
            .get_index(name)
            .map(|idx| idx.read().approx_memory())
            .unwrap_or(0);

        self.search_index_manager.drop_index(name)?;

        if size > 0 {
            self.memory_tracker
                .deallocate(size, MemoryCategory::Search);
        }
        Ok(())
    }

    /// List all search indices
    pub fn list_search_indices(&self) -> Vec<String> {
        self.search_index_manager.list_indices()
    }

    /// Full index definitions for AOF rewrite (sorted by name).
    pub fn list_search_index_definitions(&self) -> Vec<IndexDefinition> {
        self.search_index_manager.list_definitions()
    }

    /// Alias → real index mappings for AOF rewrite (sorted by alias).
    pub fn list_search_aliases(&self) -> Vec<(String, String)> {
        self.search_index_manager.list_aliases()
    }

    /// True when this DB has any search index or alias (rewrite even if keyspace empty).
    pub fn has_search_state(&self) -> bool {
        self.search_index_manager.has_any_state()
    }

    /// Get search index information
    pub fn get_search_index_info(&self, name: &str) -> Option<IndexInfo> {
        let index = self.search_index_manager.get_index(name)?;
        let index_guard = index.read();

        Some(IndexInfo {
            name: index_guard.definition.name.clone(),
            num_docs: index_guard.size(),
            num_fields: index_guard.definition.fields.len(),
            fields: index_guard.definition.fields
                .iter()
                .map(|f| f.name.clone())
                .collect(),
        })
    }

    /// FT.ALIASADD alias index
    pub fn alias_add(&self, alias: &str, index: &str) -> Result<(), String> {
        self.search_index_manager.alias_add(alias, index)
    }

    /// FT.ALIASDEL alias
    pub fn alias_del(&self, alias: &str) -> Result<(), String> {
        self.search_index_manager.alias_del(alias)
    }

    /// FT.ALIASUPDATE alias index
    pub fn alias_update(&self, alias: &str, index: &str) -> Result<(), String> {
        self.search_index_manager.alias_update(alias, index)
    }

    /// Distinct tag values for a TAG field (`FT.TAGVALS index field`).
    ///
    /// Returns `Ok(None)` if the index is missing, `Err` if the field is not a TAG field.
    pub fn get_tag_values(&self, index_name: &str, field: &str) -> Result<Option<Vec<String>>, String> {
        let Some(index) = self.search_index_manager.get_index(index_name) else {
            return Ok(None);
        };
        let guard = index.read();
        let Some(field_def) = guard.definition.get_field(field) else {
            return Err(format!("Unknown field '{}'", field));
        };
        if !matches!(field_def.field_type, crate::search_index::FieldType::Tag { .. }) {
            return Err(format!("Field '{}' is not a TAG field", field));
        }
        let values = guard
            .get_tag_index(field)
            .map(|ti| ti.tag_values())
            .unwrap_or_default();
        Ok(Some(values))
    }

    /// Account search memory for indexing `doc_id` with `fields` into an index
    /// that currently holds `old_fields` (if any). Returns Err on maxmemory.
    fn account_search_index_write(
        &self,
        doc_id: &Bytes,
        old_fields: Option<&HashMap<String, DocumentField>>,
        fields: &HashMap<String, DocumentField>,
    ) -> Result<(), String> {
        let old_size = old_fields
            .map(|f| SearchIndex::document_approx_size(doc_id, f))
            .unwrap_or(0);
        let new_size = SearchIndex::document_approx_size(doc_id, fields);

        if new_size > old_size {
            let delta = new_size - old_size;
            if !self
                .memory_tracker
                .can_allocate(delta, MemoryCategory::Search)
            {
                // Under maxmemory pressure, try reclaiming (including other
                // search docs — never the doc we are about to re-account).
                if self.eviction_allowed() {
                    let _ = self.evict_memory_excluding(delta, Some(doc_id));
                }
                if !self
                    .memory_tracker
                    .can_allocate(delta, MemoryCategory::Search)
                {
                    return Err("OOM: cannot allocate search index memory".into());
                }
            }
        }

        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::Search);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::Search);
        }
        Ok(())
    }

    /// Index a document
    pub fn index_document(
        &self,
        index_name: &str,
        doc_id: Bytes,
        fields: HashMap<String, DocumentField>,
    ) -> Result<(), String> {
        let index = self.search_index_manager
            .get_index(index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?;

        // Snapshot under lock, then drop before accounting (eviction may need
        // the same index write lock to free search docs).
        let old_fields = {
            let index_guard = index.read();
            index_guard.get_document_data(&doc_id).cloned()
        };
        self.account_search_index_write(&doc_id, old_fields.as_ref(), &fields)?;
        index.write().index_document(doc_id, fields);
        Ok(())
    }

    /// Remove a document from an index
    pub fn remove_from_index(&self, index_name: &str, doc_id: &Bytes) -> Result<(), String> {
        let index = self.search_index_manager
            .get_index(index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?;

        let mut index_guard = index.write();
        if let Some(fields) = index_guard.get_document_data(doc_id) {
            let size = SearchIndex::document_approx_size(doc_id, fields);
            self.memory_tracker
                .deallocate(size, MemoryCategory::Search);
        }
        index_guard.remove_document(doc_id);
        Ok(())
    }

    /// Search for documents
    pub fn search(
        &self,
        index_name: &str,
        query_str: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResult, String> {
        let index = self.search_index_manager
            .get_index(index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?;

        let index_guard = index.read();

        // Parse query
        let mut query = QueryParser::parse_simple(query_str)
            .map_err(|e| format!("Query parse error: {}", e))?;

        // If no filter was set (simple text search), create a filter to search all text fields
        if query.filter.is_none() && !query_str.is_empty() {
            let terms: Vec<String> = query_str
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();

            if !terms.is_empty() {
                // Get all text fields from the index
                let text_fields: Vec<String> = index_guard
                    .definition
                    .fields
                    .iter()
                    .filter_map(|f| {
                        if matches!(f.field_type, crate::search_index::FieldType::Text { .. }) {
                            Some(f.name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                // Create OR filter for all text fields
                if !text_fields.is_empty() {
                    let field_filters: Vec<crate::query_engine::QueryFilter> = text_fields
                        .into_iter()
                        .map(|field_name| crate::query_engine::QueryFilter::Text {
                            field: field_name,
                            terms: terms.clone(),
                            operator: crate::query_engine::QueryOperator::Or,
                        })
                        .collect();

                    if field_filters.len() == 1 {
                        query.filter = Some(field_filters.into_iter().next().unwrap());
                    } else if field_filters.len() > 1 {
                        query.filter = Some(crate::query_engine::QueryFilter::Compound {
                            operator: crate::query_engine::QueryOperator::Or,
                            filters: field_filters,
                        });
                    }
                }
            }
        }

        query = query.with_limit(limit).with_offset(offset);

        // Collect document data from the index
        let mut document_data = HashMap::new();
        for doc_id in index_guard.get_documents() {
            if let Some(fields) = index_guard.get_document_data(doc_id) {
                document_data.insert(doc_id.clone(), fields.clone());
            }
        }

        // Execute query
        let results = QueryExecutor::execute(&index_guard, &query, &document_data);

        // Convert to SearchResult
        let documents: Vec<SearchDocument> = results
            .into_iter()
            .filter_map(|(doc_id, score)| {
                document_data.get(&doc_id).map(|fields| SearchDocument {
                    id: doc_id,
                    fields: fields.clone(),
                    score,
                })
            })
            .collect();

        let total = documents.len();

        Ok(SearchResult { total, documents })
    }

    /// Auto-index documents based on key prefix.
    /// Called after successful HSET when the key matches an index PREFIX.
    /// Skips an index (best-effort) when search memory cannot be allocated.
    pub fn auto_index_key(&self, key: &Bytes, fields: HashMap<String, DocumentField>) {
        let key_str = String::from_utf8_lossy(key);
        let matching_indices = self.search_index_manager.find_matching_indices(&key_str);

        for index_arc in matching_indices {
            // Drop the lock before accounting so eviction can remove other
            // (or even the same) search docs without deadlock.
            let old_fields = {
                let index = index_arc.read();
                index.get_document_data(key).cloned()
            };
            if self
                .account_search_index_write(key, old_fields.as_ref(), &fields)
                .is_err()
            {
                // Leave prior index entry in place if update would OOM; for new docs, skip.
                continue;
            }
            index_arc.write().index_document(key.clone(), fields.clone());
        }
    }

    /// Auto-remove document from indices when key is deleted (DEL/UNLINK).
    pub fn auto_remove_from_indices(&self, key: &Bytes) {
        let key_str = String::from_utf8_lossy(key);
        let matching_indices = self.search_index_manager.find_matching_indices(&key_str);

        for index_arc in matching_indices {
            let mut index = index_arc.write();
            if let Some(fields) = index.get_document_data(key) {
                let size = SearchIndex::document_approx_size(key, fields);
                self.memory_tracker
                    .deallocate(size, MemoryCategory::Search);
            }
            index.remove_document(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_index::{FieldDefinition, FieldType};

    #[test]
    fn test_create_and_list_indices() {
        let cache = Cache::new_with_sweep(16, 1024 * 1024, 500 * 1024 * 1024, false);

        let definition = IndexDefinition::new(
            "test_idx".to_string(),
            vec!["doc:".to_string()],
            vec![
                FieldDefinition {
                    name: "title".to_string(),
                    field_type: FieldType::Text {
                        weight: 1.0,
                        sortable: false,
                    },
                },
            ],
        );

        assert!(cache.create_search_index(definition).is_ok());

        let indices = cache.list_search_indices();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0], "test_idx");

        assert!(cache.drop_search_index("test_idx").is_ok());
        assert_eq!(cache.list_search_indices().len(), 0);
    }

    #[test]
    fn test_index_and_search() {
        let cache = Cache::new_with_sweep(16, 1024 * 1024, 500 * 1024 * 1024, false);

        let definition = IndexDefinition::new(
            "test_idx".to_string(),
            vec![],
            vec![
                FieldDefinition {
                    name: "content".to_string(),
                    field_type: FieldType::Text {
                        weight: 1.0,
                        sortable: false,
                    },
                },
            ],
        );

        cache.create_search_index(definition).unwrap();

        // Index a document
        let mut fields = HashMap::new();
        fields.insert(
            "content".to_string(),
            DocumentField::Text("hello world".to_string()),
        );
        cache.index_document("test_idx", Bytes::from("doc1"), fields).unwrap();

        // Search
        let results = cache.search("test_idx", "hello", 10, 0).unwrap();
        assert_eq!(results.total, 1);
    }
}
