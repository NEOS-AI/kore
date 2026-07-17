use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use parking_lot::RwLock;
use std::sync::Arc;
use serde::{Deserialize, Serialize};

/// Field types supported in search indices
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FieldType {
    /// Text field for full-text search
    Text {
        weight: f64,
        sortable: bool,
    },
    /// Numeric field for range queries
    Numeric {
        sortable: bool,
    },
    /// Tag field for exact matching
    Tag {
        separator: String,
        sortable: bool,
    },
    /// Vector field for similarity search
    Vector {
        algorithm: VectorAlgorithm,
        dimensions: usize,
        distance_metric: DistanceMetric,
    },
}

/// Vector search algorithms
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VectorAlgorithm {
    /// Flat (brute-force) search
    Flat,
    /// Hierarchical Navigable Small World
    HNSW {
        m: usize,
        ef_construction: usize,
    },
}

/// Distance metrics for vector similarity
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine distance)
    Cosine,
    /// Euclidean distance (L2)
    L2,
    /// Inner product
    IP,
}

/// Field definition in an index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub field_type: FieldType,
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub prefix: Vec<String>,
    pub fields: Vec<FieldDefinition>,
    pub created_at: u64,
}

impl IndexDefinition {
    pub fn new(name: String, prefix: Vec<String>, fields: Vec<FieldDefinition>) -> Self {
        Self {
            name,
            prefix,
            fields,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Check if a key matches this index's prefix
    pub fn matches_prefix(&self, key: &str) -> bool {
        if self.prefix.is_empty() {
            return true;
        }
        self.prefix.iter().any(|prefix| key.starts_with(prefix))
    }

    /// Get field definition by name
    pub fn get_field(&self, name: &str) -> Option<&FieldDefinition> {
        self.fields.iter().find(|f| f.name == name)
    }
}

/// Inverted index for text search
#[derive(Debug, Clone)]
pub struct InvertedIndex {
    /// Maps term to set of document IDs
    terms: HashMap<String, HashSet<Bytes>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            terms: HashMap::new(),
        }
    }

    /// Add a document to the index
    pub fn add_document(&mut self, doc_id: Bytes, text: &str, weight: f64) {
        let tokens = Self::tokenize(text);
        for token in tokens {
            self.terms
                .entry(token)
                .or_insert_with(HashSet::new)
                .insert(doc_id.clone());
        }
    }

    /// Remove a document from the index
    pub fn remove_document(&mut self, doc_id: &Bytes) {
        for (_, doc_set) in self.terms.iter_mut() {
            doc_set.remove(doc_id);
        }
        // Clean up empty entries
        self.terms.retain(|_, docs| !docs.is_empty());
    }

    /// Search for documents matching a term
    pub fn search(&self, term: &str) -> HashSet<Bytes> {
        let normalized = term.to_lowercase();
        self.terms
            .get(&normalized)
            .cloned()
            .unwrap_or_default()
    }

    /// Search for documents matching all terms (AND)
    pub fn search_and(&self, terms: &[String]) -> HashSet<Bytes> {
        if terms.is_empty() {
            return HashSet::new();
        }

        let mut result = self.search(&terms[0]);
        for term in &terms[1..] {
            let term_results = self.search(term);
            result = result.intersection(&term_results).cloned().collect();
        }
        result
    }

    /// Search for documents matching any term (OR)
    pub fn search_or(&self, terms: &[String]) -> HashSet<Bytes> {
        let mut result = HashSet::new();
        for term in terms {
            result.extend(self.search(term));
        }
        result
    }

    /// Simple tokenization (split by whitespace and punctuation)
    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }
}

/// Numeric index for range queries
#[derive(Debug, Clone)]
pub struct NumericIndex {
    /// Maps document ID to numeric value
    values: HashMap<Bytes, f64>,
}

impl NumericIndex {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn add(&mut self, doc_id: Bytes, value: f64) {
        self.values.insert(doc_id, value);
    }

    pub fn remove(&mut self, doc_id: &Bytes) {
        self.values.remove(doc_id);
    }

    /// Range query: find documents where min <= value <= max
    pub fn range(&self, min: f64, max: f64) -> HashSet<Bytes> {
        self.values
            .iter()
            .filter(|(_, &v)| v >= min && v <= max)
            .map(|(k, _)| k.clone())
            .collect()
    }
}

/// Tag index for exact matching
#[derive(Debug, Clone)]
pub struct TagIndex {
    /// Maps tag to set of document IDs
    tags: HashMap<String, HashSet<Bytes>>,
}

impl TagIndex {
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }

    pub fn add(&mut self, doc_id: Bytes, tags: Vec<String>) {
        for tag in tags {
            self.tags
                .entry(tag)
                .or_insert_with(HashSet::new)
                .insert(doc_id.clone());
        }
    }

    pub fn remove(&mut self, doc_id: &Bytes) {
        for (_, doc_set) in self.tags.iter_mut() {
            doc_set.remove(doc_id);
        }
        self.tags.retain(|_, docs| !docs.is_empty());
    }

    pub fn search(&self, tag: &str) -> HashSet<Bytes> {
        self.tags.get(tag).cloned().unwrap_or_default()
    }

    /// All distinct tag values currently indexed (for FT.TAGVALS).
    pub fn tag_values(&self) -> Vec<String> {
        let mut vals: Vec<String> = self.tags.keys().cloned().collect();
        vals.sort();
        vals
    }
}

/// Complete search index
#[derive(Debug)]
pub struct SearchIndex {
    pub definition: IndexDefinition,
    /// Text field indices
    text_indices: HashMap<String, InvertedIndex>,
    /// Numeric field indices
    numeric_indices: HashMap<String, NumericIndex>,
    /// Tag field indices
    tag_indices: HashMap<String, TagIndex>,
    /// Vector field indices (stored separately)
    /// Maps field name to document ID to vector
    vector_indices: HashMap<String, HashMap<Bytes, Vec<f32>>>,
    /// All document IDs in this index
    documents: HashSet<Bytes>,
    /// Document field data storage (for returning in search results)
    document_data: HashMap<Bytes, HashMap<String, DocumentField>>,
}

impl SearchIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        Self {
            definition,
            text_indices: HashMap::new(),
            numeric_indices: HashMap::new(),
            tag_indices: HashMap::new(),
            vector_indices: HashMap::new(),
            documents: HashSet::new(),
            document_data: HashMap::new(),
        }
    }

    /// Add or update a document in the index
    pub fn index_document(&mut self, doc_id: Bytes, fields: HashMap<String, DocumentField>) {
        // First remove if exists
        if self.documents.contains(&doc_id) {
            self.remove_document(&doc_id);
        }

        self.documents.insert(doc_id.clone());

        // Store document field data
        self.document_data.insert(doc_id.clone(), fields.clone());

        for field_def in &self.definition.fields {
            if let Some(field_value) = fields.get(&field_def.name) {
                match &field_def.field_type {
                    FieldType::Text { weight, .. } => {
                        if let DocumentField::Text(text) = field_value {
                            let index = self.text_indices
                                .entry(field_def.name.clone())
                                .or_insert_with(InvertedIndex::new);
                            index.add_document(doc_id.clone(), text, *weight);
                        }
                    }
                    FieldType::Numeric { .. } => {
                        let value = match field_value {
                            DocumentField::Numeric(v) => Some(*v),
                            // HSET stores raw strings; coerce for NUMERIC schema fields.
                            DocumentField::Text(s) => s.parse::<f64>().ok(),
                            _ => None,
                        };
                        if let Some(value) = value {
                            let index = self.numeric_indices
                                .entry(field_def.name.clone())
                                .or_insert_with(NumericIndex::new);
                            index.add(doc_id.clone(), value);
                        }
                    }
                    FieldType::Tag { separator, .. } => {
                        let tags = match field_value {
                            DocumentField::Tag(tags) => tags.clone(),
                            // HSET stores raw strings; split by TAG SEPARATOR.
                            DocumentField::Text(text) => text
                                .split(separator.as_str())
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect(),
                            _ => Vec::new(),
                        };
                        if !tags.is_empty() {
                            let index = self.tag_indices
                                .entry(field_def.name.clone())
                                .or_insert_with(TagIndex::new);
                            index.add(doc_id.clone(), tags);
                        }
                    }
                    FieldType::Vector { dimensions, .. } => {
                        if let DocumentField::Vector(vec) = field_value {
                            if vec.len() == *dimensions {
                                let index = self.vector_indices
                                    .entry(field_def.name.clone())
                                    .or_insert_with(HashMap::new);
                                index.insert(doc_id.clone(), vec.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Remove a document from the index
    pub fn remove_document(&mut self, doc_id: &Bytes) {
        self.documents.remove(doc_id);
        self.document_data.remove(doc_id);

        for index in self.text_indices.values_mut() {
            index.remove_document(doc_id);
        }

        for index in self.numeric_indices.values_mut() {
            index.remove(doc_id);
        }

        for index in self.tag_indices.values_mut() {
            index.remove(doc_id);
        }

        for index in self.vector_indices.values_mut() {
            index.remove(doc_id);
        }
    }

    /// Get text index for a field
    pub fn get_text_index(&self, field: &str) -> Option<&InvertedIndex> {
        self.text_indices.get(field)
    }

    /// Get numeric index for a field
    pub fn get_numeric_index(&self, field: &str) -> Option<&NumericIndex> {
        self.numeric_indices.get(field)
    }

    /// Get tag index for a field
    pub fn get_tag_index(&self, field: &str) -> Option<&TagIndex> {
        self.tag_indices.get(field)
    }

    /// Get vector index for a field
    pub fn get_vector_index(&self, field: &str) -> Option<&HashMap<Bytes, Vec<f32>>> {
        self.vector_indices.get(field)
    }

    /// Get all documents in the index
    pub fn get_documents(&self) -> &HashSet<Bytes> {
        &self.documents
    }

    /// Get number of documents
    pub fn size(&self) -> usize {
        self.documents.len()
    }

    /// Get document field data
    pub fn get_document_data(&self, doc_id: &Bytes) -> Option<&HashMap<String, DocumentField>> {
        self.document_data.get(doc_id)
    }

    /// Approximate memory for a single document (id + field storage + inverted-index overhead).
    pub fn document_approx_size(doc_id: &Bytes, fields: &HashMap<String, DocumentField>) -> usize {
        use crate::memory::{with_alloc_overhead, BYTES_OVERHEAD, DICT_ENTRY_OVERHEAD};
        let mut size = doc_id.len() + BYTES_OVERHEAD + DICT_ENTRY_OVERHEAD + 64;
        for (name, value) in fields {
            size += name.len() + value.approx_size() + DICT_ENTRY_OVERHEAD + 48;
            // Rough inverted-index / posting overhead per field
            size += 32;
        }
        with_alloc_overhead(size)
    }

    /// Approximate total memory used by all documents in this index.
    pub fn approx_memory(&self) -> usize {
        self.document_data
            .iter()
            .map(|(id, fields)| Self::document_approx_size(id, fields))
            .sum()
    }
}

/// Document field value
#[derive(Debug, Clone)]
pub enum DocumentField {
    Text(String),
    Numeric(f64),
    Tag(Vec<String>),
    Vector(Vec<f32>),
}

impl DocumentField {
    /// Approximate heap size of this field value (for memory accounting).
    pub fn approx_size(&self) -> usize {
        match self {
            DocumentField::Text(s) => s.len() + 24,
            DocumentField::Numeric(_) => 16,
            DocumentField::Tag(tags) => {
                tags.iter().map(|t| t.len() + 8).sum::<usize>() + 24
            }
            DocumentField::Vector(v) => v.len() * std::mem::size_of::<f32>() + 24,
        }
    }
}

/// Manager for all search indices
#[derive(Debug)]
pub struct SearchIndexManager {
    indices: Arc<RwLock<HashMap<String, Arc<RwLock<SearchIndex>>>>>,
    /// Alias name → real index name (RediSearch FT.ALIAS*)
    aliases: Arc<RwLock<HashMap<String, String>>>,
}

impl SearchIndexManager {
    pub fn new() -> Self {
        Self {
            indices: Arc::new(RwLock::new(HashMap::new())),
            aliases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Resolve `name` if it is an alias; otherwise return `name` unchanged.
    pub fn resolve_name(&self, name: &str) -> String {
        let aliases = self.aliases.read();
        Self::resolve_name_locked(&aliases, name)
    }

    /// Resolve under an already-held aliases lock (one hop; aliases always store real names).
    fn resolve_name_locked(aliases: &HashMap<String, String>, name: &str) -> String {
        aliases
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    /// Create a new index
    pub fn create_index(&self, definition: IndexDefinition) -> Result<(), String> {
        // Lock order: aliases then indices (matches drop_index / alias_*).
        // Hold both for the full check-and-insert so create vs alias cannot TOCTOU.
        let aliases = self.aliases.write();
        let mut indices = self.indices.write();

        // Alias names and index names share a namespace.
        if aliases.contains_key(&definition.name) {
            return Err(format!(
                "Index name '{}' clashes with an existing alias",
                definition.name
            ));
        }

        if indices.contains_key(&definition.name) {
            return Err(format!("Index '{}' already exists", definition.name));
        }

        let index = SearchIndex::new(definition.clone());
        indices.insert(definition.name.clone(), Arc::new(RwLock::new(index)));
        Ok(())
    }

    /// Drop an index (resolves aliases). Removes all aliases that pointed at it.
    pub fn drop_index(&self, name: &str) -> Result<(), String> {
        // Lock order: aliases then indices (consistent across alias ops).
        let mut aliases = self.aliases.write();
        let real_name = Self::resolve_name_locked(&aliases, name);

        let mut indices = self.indices.write();
        if indices.remove(&real_name).is_none() {
            return Err(format!("Index '{}' does not exist", name));
        }

        aliases.retain(|_, target| target != &real_name);
        Ok(())
    }

    /// Get an index (resolves aliases)
    pub fn get_index(&self, name: &str) -> Option<Arc<RwLock<SearchIndex>>> {
        let real = self.resolve_name(name);
        let indices = self.indices.read();
        indices.get(&real).cloned()
    }

    /// List all index names (aliases are not listed)
    pub fn list_indices(&self) -> Vec<String> {
        let indices = self.indices.read();
        indices.keys().cloned().collect()
    }

    /// Get index definition (resolves aliases)
    pub fn get_definition(&self, name: &str) -> Option<IndexDefinition> {
        let index = self.get_index(name)?;
        let def = index.read().definition.clone();
        Some(def)
    }

    /// FT.ALIASADD alias index — create a new alias (fails if alias exists).
    ///
    /// If `index` is itself an alias, the real index name is resolved and stored
    /// so DROPINDEX cleanup by real name stays consistent.
    pub fn alias_add(&self, alias: &str, index: &str) -> Result<(), String> {
        // Lock order: aliases then indices (matches create_index / drop_index).
        let mut aliases = self.aliases.write();
        let indices = self.indices.read();

        if indices.contains_key(alias) {
            return Err(format!("Alias clashes with an existing index name '{}'", alias));
        }
        if aliases.contains_key(alias) {
            return Err(format!("Alias '{}' already exists", alias));
        }

        let real_index = Self::resolve_name_locked(&aliases, index);
        if !indices.contains_key(&real_index) {
            return Err(format!("Unknown index name '{}'", index));
        }

        aliases.insert(alias.to_string(), real_index);
        Ok(())
    }

    /// FT.ALIASDEL alias — remove an alias.
    pub fn alias_del(&self, alias: &str) -> Result<(), String> {
        let mut aliases = self.aliases.write();
        if aliases.remove(alias).is_none() {
            return Err(format!("Alias '{}' does not exist", alias));
        }
        Ok(())
    }

    /// FT.ALIASUPDATE alias index — create or retarget an alias.
    ///
    /// If `index` is itself an alias, the real index name is resolved and stored.
    pub fn alias_update(&self, alias: &str, index: &str) -> Result<(), String> {
        // Lock order: aliases then indices (matches create_index / drop_index).
        let mut aliases = self.aliases.write();
        let indices = self.indices.read();

        if indices.contains_key(alias) {
            return Err(format!("Alias clashes with an existing index name '{}'", alias));
        }

        let real_index = Self::resolve_name_locked(&aliases, index);
        if !indices.contains_key(&real_index) {
            return Err(format!("Unknown index name '{}'", index));
        }

        aliases.insert(alias.to_string(), real_index);
        Ok(())
    }

    /// Find indices that match a key prefix
    pub fn find_matching_indices(&self, key: &str) -> Vec<Arc<RwLock<SearchIndex>>> {
        let indices = self.indices.read();
        indices
            .values()
            .filter(|idx| {
                let index = idx.read();
                index.definition.matches_prefix(key)
            })
            .cloned()
            .collect()
    }

    /// Sample up to `n` documents across all indices for maxmemory eviction.
    ///
    /// Returns `(index_name, doc_id, approx_size)` triples. Used so search
    /// category memory can be reclaimed under `allkeys-*` policies without
    /// deleting the underlying hash key.
    ///
    /// `exclude_doc` skips a document currently being re-indexed so accounting
    /// does not double-free it while holding a stale size snapshot.
    pub fn sample_documents_for_eviction(
        &self,
        n: usize,
        exclude_doc: Option<&Bytes>,
    ) -> Vec<(String, Bytes, usize)> {
        if n == 0 {
            return Vec::new();
        }
        use rand::seq::SliceRandom;
        let indices = self.indices.read();
        let mut pool: Vec<(String, Bytes, usize)> = Vec::new();
        for (name, idx) in indices.iter() {
            let guard = idx.read();
            for (doc_id, fields) in guard.document_data.iter() {
                if exclude_doc.is_some_and(|ex| ex == doc_id) {
                    continue;
                }
                let size = SearchIndex::document_approx_size(doc_id, fields);
                if size > 0 {
                    pool.push((name.clone(), doc_id.clone(), size));
                }
            }
        }
        drop(indices);
        if pool.len() <= n {
            return pool;
        }
        let mut rng = rand::thread_rng();
        pool.shuffle(&mut rng);
        pool.truncate(n);
        pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inverted_index() {
        let mut index = InvertedIndex::new();
        let doc1 = Bytes::from("doc1");
        let doc2 = Bytes::from("doc2");

        index.add_document(doc1.clone(), "hello world", 1.0);
        index.add_document(doc2.clone(), "hello rust", 1.0);

        let results = index.search("hello");
        assert_eq!(results.len(), 2);
        assert!(results.contains(&doc1));
        assert!(results.contains(&doc2));

        let results = index.search("world");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&doc1));
    }

    #[test]
    fn test_numeric_index() {
        let mut index = NumericIndex::new();
        let doc1 = Bytes::from("doc1");
        let doc2 = Bytes::from("doc2");
        let doc3 = Bytes::from("doc3");

        index.add(doc1.clone(), 10.0);
        index.add(doc2.clone(), 20.0);
        index.add(doc3.clone(), 30.0);

        let results = index.range(15.0, 25.0);
        assert_eq!(results.len(), 1);
        assert!(results.contains(&doc2));
    }

    #[test]
    fn test_search_index_manager() {
        let manager = SearchIndexManager::new();

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

        assert!(manager.create_index(definition).is_ok());
        assert!(manager.get_index("test_idx").is_some());
        assert_eq!(manager.list_indices().len(), 1);
        assert!(manager.drop_index("test_idx").is_ok());
        assert_eq!(manager.list_indices().len(), 0);
    }

    #[test]
    fn test_search_index_aliases() {
        let manager = SearchIndexManager::new();

        let definition = IndexDefinition::new(
            "idx".to_string(),
            vec![],
            vec![FieldDefinition {
                name: "t".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            }],
        );
        manager.create_index(definition).unwrap();

        assert!(manager.alias_add("a1", "idx").is_ok());
        assert!(manager.get_index("a1").is_some());
        assert_eq!(manager.resolve_name("a1"), "idx");

        // duplicate alias
        assert!(manager.alias_add("a1", "idx").is_err());
        // unknown index
        assert!(manager.alias_add("a2", "nope").is_err());
        // clash with index name
        assert!(manager.alias_add("idx", "idx").is_err());

        // alias → alias target resolves and stores the real index name
        assert!(manager.alias_add("a2", "a1").is_ok());
        assert_eq!(manager.resolve_name("a2"), "idx");

        // retarget via update (create new)
        let def2 = IndexDefinition::new(
            "idx2".to_string(),
            vec![],
            vec![FieldDefinition {
                name: "t".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            }],
        );
        manager.create_index(def2).unwrap();
        assert!(manager.alias_update("a1", "idx2").is_ok());
        assert_eq!(manager.resolve_name("a1"), "idx2");

        // ALIASUPDATE with alias target stores real name
        assert!(manager.alias_update("a2", "a1").is_ok());
        assert_eq!(manager.resolve_name("a2"), "idx2");

        assert!(manager.alias_del("a1").is_ok());
        assert!(manager.get_index("a1").is_none());
        assert!(manager.alias_del("a1").is_err());

        // dropindex cleans aliases that stored the real name (including a2 → idx2)
        manager.alias_add("gone", "idx").unwrap();
        manager.drop_index("idx").unwrap();
        assert!(manager.get_index("gone").is_none());
        assert!(manager.alias_del("gone").is_err());

        // a2 still points at idx2; drop via real name cleans a2
        manager.drop_index("idx2").unwrap();
        assert!(manager.get_index("a2").is_none());
        assert!(manager.alias_del("a2").is_err());
    }
}
