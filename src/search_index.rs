use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use parking_lot::RwLock;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::vector_search::{HnswGraphSnapshot, HNSWIndex};

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    /// Compare **logical schema** only: `name`, `prefix`, and `fields`
    /// (each field's name + type). Ignores `created_at` so two independently
    /// created definitions with the same FT.CREATE shape are equal.
    ///
    /// Used by RDB merge (`DbSnapshot::load_into`) to decide whether a name
    /// clash is an idempotent skip (schemas equal) or a hard error (diverge).
    pub fn schema_eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.prefix == other.prefix
            && self.fields == other.fields
    }

    /// Human-readable summary of logical schema differences (for merge errors).
    ///
    /// Returns `None` when [`schema_eq`] is true.
    pub fn schema_diff_summary(&self, other: &Self) -> Option<String> {
        if self.schema_eq(other) {
            return None;
        }
        let mut parts = Vec::new();
        if self.name != other.name {
            parts.push(format!("name ('{}' vs '{}')", self.name, other.name));
        }
        if self.prefix != other.prefix {
            parts.push(format!(
                "prefix ({:?} vs {:?})",
                self.prefix, other.prefix
            ));
        }
        if self.fields != other.fields {
            parts.push(format!(
                "fields ({:?} vs {:?})",
                self.fields, other.fields
            ));
        }
        Some(parts.join(", "))
    }

    /// Parse an `FT.CREATE` argument list into an index definition.
    ///
    /// `argv` is the full command argv with the command name at `[0]`:
    /// `["FT.CREATE", index, … OPTIONS …, "SCHEMA", field, type, …]`.
    ///
    /// This is the **single** FT.CREATE schema parser used by both the live
    /// command path (`FT.CREATE`) and AOF load, so PREFIX / SCHEMA / field
    /// options (TEXT WEIGHT/SORTABLE, TAG SEPARATOR, VECTOR HNSW M /
    /// EF_CONSTRUCTION, metrics) cannot drift between the two.
    ///
    /// HNSW defaults: `M=16`, `EF_CONSTRUCTION=200` when omitted.
    pub fn from_ft_create_argv(argv: &[Bytes]) -> Result<IndexDefinition, String> {
        // Accept either full argv (`FT.CREATE` first) or args starting at the
        // index name (command-handler slice after the command token).
        let args = if !argv.is_empty()
            && String::from_utf8_lossy(&argv[0]).eq_ignore_ascii_case("FT.CREATE")
        {
            &argv[1..]
        } else {
            argv
        };
        Self::from_ft_create_args(args)
    }

    /// Parse `FT.CREATE` args starting at the index name (no command token).
    ///
    /// Shape: `[index, ON HASH, PREFIX n p…, SCHEMA, field, type, options…]`.
    pub fn from_ft_create_args(args: &[Bytes]) -> Result<IndexDefinition, String> {
        if args.is_empty() {
            return Err("ERR wrong number of arguments for 'FT.CREATE' command".into());
        }

        let index_name = String::from_utf8_lossy(&args[0]).into_owned();
        let mut i = 1;
        let mut prefix_list = Vec::new();
        let mut fields = Vec::new();

        // Optional ON / PREFIX, then SCHEMA.
        while i < args.len() {
            let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
            match arg.as_str() {
                "ON" => {
                    // Skip ON HASH (or ON JSON, etc.) — only HASH is supported.
                    i += 2;
                }
                "PREFIX" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("ERR missing prefix count".into());
                    }
                    let count = parse_ft_usize(&args[i])
                        .ok_or_else(|| "ERR invalid prefix count".to_string())?;
                    i += 1;
                    for _ in 0..count {
                        if i >= args.len() {
                            return Err("ERR missing prefix".into());
                        }
                        prefix_list.push(String::from_utf8_lossy(&args[i]).into_owned());
                        i += 1;
                    }
                }
                "SCHEMA" => {
                    i += 1;
                    break;
                }
                _ => {
                    return Err(format!("ERR unknown option '{}'", arg));
                }
            }
        }

        // SCHEMA fields.
        while i < args.len() {
            if i + 1 >= args.len() {
                return Err("ERR incomplete field definition".into());
            }

            let field_name = String::from_utf8_lossy(&args[i]).into_owned();
            i += 1;

            let field_type_str = String::from_utf8_lossy(&args[i]).to_uppercase();
            i += 1;

            let field_type = match field_type_str.as_str() {
                "TEXT" => {
                    let mut weight = 1.0f64;
                    let mut sortable = false;
                    while i < args.len() {
                        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                        match opt.as_str() {
                            "WEIGHT" => {
                                i += 1;
                                if i >= args.len() {
                                    return Err("ERR missing weight value".into());
                                }
                                weight = std::str::from_utf8(&args[i])
                                    .ok()
                                    .and_then(|s| s.parse().ok())
                                    .ok_or_else(|| "ERR invalid weight".to_string())?;
                                i += 1;
                            }
                            "SORTABLE" => {
                                sortable = true;
                                i += 1;
                            }
                            _ => break,
                        }
                    }
                    FieldType::Text { weight, sortable }
                }
                "NUMERIC" => {
                    let mut sortable = false;
                    if i < args.len()
                        && String::from_utf8_lossy(&args[i]).eq_ignore_ascii_case("SORTABLE")
                    {
                        sortable = true;
                        i += 1;
                    }
                    FieldType::Numeric { sortable }
                }
                "TAG" => {
                    let mut separator = ",".to_string();
                    let mut sortable = false;
                    while i < args.len() {
                        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                        match opt.as_str() {
                            "SEPARATOR" => {
                                i += 1;
                                if i >= args.len() {
                                    return Err("ERR missing separator".into());
                                }
                                separator = String::from_utf8_lossy(&args[i]).into_owned();
                                i += 1;
                            }
                            "SORTABLE" => {
                                sortable = true;
                                i += 1;
                            }
                            _ => break,
                        }
                    }
                    FieldType::Tag { separator, sortable }
                }
                "VECTOR" => {
                    // VECTOR algorithm [HNSW M n] TYPE FLOAT32 DIM n [DISTANCE_METRIC m]
                    if i >= args.len() {
                        return Err("ERR incomplete VECTOR field definition".into());
                    }

                    let algo_str = String::from_utf8_lossy(&args[i]).to_uppercase();
                    i += 1;

                    let algorithm = match algo_str.as_str() {
                        "FLAT" => VectorAlgorithm::Flat,
                        "HNSW" => {
                            let mut m = 16usize;
                            const DEFAULT_EF_CONSTRUCTION: usize = 200;
                            let mut ef_construction = DEFAULT_EF_CONSTRUCTION;
                            // Optional HNSW options: M <n> and/or EF_CONSTRUCTION <n>
                            // (order-independent; stop at TYPE / unknown keyword).
                            loop {
                                if i >= args.len() {
                                    break;
                                }
                                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                                match opt.as_str() {
                                    "M" => {
                                        i += 1;
                                        if i >= args.len() {
                                            return Err("ERR missing HNSW M value".into());
                                        }
                                        m = parse_ft_usize(&args[i]).ok_or_else(|| {
                                            "ERR invalid HNSW M value".to_string()
                                        })?;
                                        i += 1;
                                    }
                                    "EF_CONSTRUCTION" => {
                                        i += 1;
                                        if i >= args.len() {
                                            return Err(
                                                "ERR missing HNSW EF_CONSTRUCTION value".into(),
                                            );
                                        }
                                        ef_construction = parse_ft_usize(&args[i]).ok_or_else(
                                            || "ERR invalid HNSW EF_CONSTRUCTION value".to_string(),
                                        )?;
                                        i += 1;
                                    }
                                    _ => break,
                                }
                            }
                            VectorAlgorithm::HNSW {
                                m,
                                ef_construction,
                            }
                        }
                        _ => {
                            return Err(format!(
                                "ERR unknown vector algorithm '{}'",
                                algo_str
                            ));
                        }
                    };

                    // Optional TYPE keyword, then type value (e.g. FLOAT32).
                    if i < args.len()
                        && String::from_utf8_lossy(&args[i]).eq_ignore_ascii_case("TYPE")
                    {
                        i += 1;
                    }
                    if i >= args.len() {
                        return Err("ERR incomplete VECTOR field definition".into());
                    }
                    i += 1; // skip type value

                    // DIM / DIMENSION
                    let dimensions = if i < args.len() {
                        let tok = String::from_utf8_lossy(&args[i]).to_uppercase();
                        if tok == "DIM" || tok == "DIMENSION" {
                            i += 1;
                            if i >= args.len() {
                                return Err("ERR missing dimension value".into());
                            }
                            let dim = parse_ft_usize(&args[i])
                                .ok_or_else(|| "ERR invalid dimension value".to_string())?;
                            i += 1;
                            dim
                        } else {
                            return Err("ERR missing DIM keyword".into());
                        }
                    } else {
                        return Err("ERR missing dimension".into());
                    };
                    if dimensions == 0 {
                        return Err("ERR invalid dimension value".into());
                    }

                    // DISTANCE_METRIC (default Cosine)
                    let distance_metric = if i < args.len()
                        && String::from_utf8_lossy(&args[i])
                            .eq_ignore_ascii_case("DISTANCE_METRIC")
                    {
                        i += 1;
                        if i >= args.len() {
                            return Err("ERR missing distance metric".into());
                        }
                        let metric_str = String::from_utf8_lossy(&args[i]).to_uppercase();
                        i += 1;
                        match metric_str.as_str() {
                            "COSINE" => DistanceMetric::Cosine,
                            "L2" => DistanceMetric::L2,
                            "IP" => DistanceMetric::IP,
                            _ => {
                                return Err(format!(
                                    "ERR unknown distance metric '{}'",
                                    metric_str
                                ));
                            }
                        }
                    } else {
                        DistanceMetric::Cosine
                    };

                    FieldType::Vector {
                        algorithm,
                        dimensions,
                        distance_metric,
                    }
                }
                _ => {
                    return Err(format!("ERR unknown field type '{}'", field_type_str));
                }
            };

            fields.push(FieldDefinition {
                name: field_name,
                field_type,
            });
        }

        if fields.is_empty() {
            return Err("ERR no fields defined".into());
        }

        Ok(IndexDefinition::new(index_name, prefix_list, fields))
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

/// Parse a decimal usize from a bulk FT.CREATE token.
fn parse_ft_usize(tok: &[u8]) -> Option<usize> {
    std::str::from_utf8(tok).ok()?.parse().ok()
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

    /// Add a document to the index.
    ///
    /// `_weight` is reserved for future TF-IDF / field-weight scoring; presence
    /// is already used at the FT schema layer (`FieldType::Text { weight }`).
    pub fn add_document(&mut self, doc_id: Bytes, text: &str, _weight: f64) {
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
    /// Live HNSW graphs for VECTOR HNSW fields (Batch FV dual-write with
    /// `vector_indices`). Query path (Batch FW) prefers non-empty HNSW ANN via
    /// [`Self::get_hnsw_index`]; flat map remains for FLAT fields and as
    /// fallback when the graph is empty/missing. Graphs are also RDB/AOF durable.
    hnsw_indices: HashMap<String, HNSWIndex>,
    /// All document IDs in this index
    documents: HashSet<Bytes>,
    /// Document field data storage (for returning in search results)
    document_data: HashMap<Bytes, HashMap<String, DocumentField>>,
}

impl SearchIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        let mut hnsw_indices = HashMap::new();
        for field in &definition.fields {
            if let FieldType::Vector {
                algorithm: VectorAlgorithm::HNSW { m, ef_construction },
                distance_metric,
                ..
            } = &field.field_type
            {
                hnsw_indices.insert(
                    field.name.clone(),
                    HNSWIndex::new(*m, *ef_construction, distance_metric.clone()),
                );
            }
        }
        Self {
            definition,
            text_indices: HashMap::new(),
            numeric_indices: HashMap::new(),
            tag_indices: HashMap::new(),
            vector_indices: HashMap::new(),
            hnsw_indices,
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
                        // Accept typed Vector or parse Text as FLOAT32 components
                        // (comma/space-separated, or LE binary of dim * 4 bytes).
                        let parsed = match field_value {
                            DocumentField::Vector(vec) if vec.len() == *dimensions => {
                                Some(vec.clone())
                            }
                            DocumentField::Text(s) => {
                                parse_vector_field_text(s, *dimensions)
                            }
                            _ => None,
                        };
                        if let Some(vec) = parsed {
                            let index = self
                                .vector_indices
                                .entry(field_def.name.clone())
                                .or_insert_with(HashMap::new);
                            index.insert(doc_id.clone(), vec.clone());
                            // Dual-write HNSW graph when this field is HNSW.
                            if let Some(hnsw) = self.hnsw_indices.get_mut(&field_def.name) {
                                hnsw.add(doc_id.clone(), vec);
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

        for hnsw in self.hnsw_indices.values_mut() {
            hnsw.remove(doc_id);
        }
    }

    /// Drop all indexed documents while keeping the `IndexDefinition` (schema).
    ///
    /// Used by FLUSHDB/FLUSHALL so search no longer returns deleted keys but
    /// FT.CREATE schema (and manager-level aliases) survive, matching RediSearch.
    pub fn clear_documents(&mut self) {
        self.text_indices.clear();
        self.numeric_indices.clear();
        self.tag_indices.clear();
        self.vector_indices.clear();
        for hnsw in self.hnsw_indices.values_mut() {
            hnsw.clear();
        }
        self.documents.clear();
        self.document_data.clear();
    }

    /// Export durable HNSW graphs for RDB (Batch FV): `(field_name, snapshot)`.
    ///
    /// Empty graphs (no vectors) are omitted.
    pub fn export_hnsw_graphs(&self) -> Vec<(String, HnswGraphSnapshot)> {
        let mut out = Vec::new();
        for (field, hnsw) in &self.hnsw_indices {
            if hnsw.is_empty() {
                continue;
            }
            out.push((field.clone(), hnsw.snapshot_graph()));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Apply a durable HNSW graph for `field` after vectors are loaded.
    ///
    /// Validates neighbor/level ids against the live HNSW vector map. If the
    /// field has no HNSW index (FLAT / missing), returns an error.
    pub fn apply_hnsw_graph(
        &mut self,
        field: &str,
        snap: &HnswGraphSnapshot,
    ) -> Result<(), String> {
        let hnsw = self
            .hnsw_indices
            .get_mut(field)
            .ok_or_else(|| format!("no HNSW index for field '{}'", field))?;
        // Ensure vectors for every level node exist on the HNSW side. Prefer
        // the flat vector map as source of truth when dual-write drifted.
        if let Some(flat) = self.vector_indices.get(field) {
            for (id, _) in &snap.levels {
                if !hnsw.iter_vectors().any(|(vid, _)| vid == id) {
                    if let Some(v) = flat.get(id) {
                        hnsw.install_vector(id.clone(), v.clone());
                    }
                }
            }
        }
        hnsw.apply_graph_snapshot(snap)
    }

    /// Test/injector: force next insert levels on an HNSW field (FIFO).
    pub fn enqueue_hnsw_levels(
        &mut self,
        field: &str,
        levels: impl IntoIterator<Item = usize>,
    ) -> Result<(), String> {
        let hnsw = self
            .hnsw_indices
            .get_mut(field)
            .ok_or_else(|| format!("no HNSW index for field '{}'", field))?;
        hnsw.enqueue_levels(levels);
        Ok(())
    }

    /// Snapshot of one HNSW field graph (if present and non-empty).
    pub fn hnsw_graph_snapshot(&self, field: &str) -> Option<HnswGraphSnapshot> {
        self.hnsw_indices.get(field).and_then(|h| {
            if h.is_empty() {
                None
            } else {
                Some(h.snapshot_graph())
            }
        })
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

    /// Get vector index for a field (flat map; all VECTOR fields dual-store here).
    pub fn get_vector_index(&self, field: &str) -> Option<&HashMap<Bytes, Vec<f32>>> {
        self.vector_indices.get(field)
    }

    /// Live HNSW graph for a VECTOR HNSW field when it has data (Batch FW).
    ///
    /// Returns `None` for FLAT fields, missing fields, or empty graphs so callers
    /// can fall back to [`Self::get_vector_index`] exact scan.
    pub fn get_hnsw_index(&self, field: &str) -> Option<&HNSWIndex> {
        self.hnsw_indices
            .get(field)
            .filter(|h| !h.is_empty())
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

/// Parse a hash/text payload into a FLOAT32 vector of `dimensions`.
///
/// Accepts:
/// - little-endian binary of exactly `dimensions * 4` bytes
/// - comma- and/or whitespace-separated decimal floats
fn parse_vector_field_text(s: &str, dimensions: usize) -> Option<Vec<f32>> {
    let bytes = s.as_bytes();
    if bytes.len() == dimensions.saturating_mul(4) {
        let mut out = Vec::with_capacity(dimensions);
        for i in 0..dimensions {
            let start = i * 4;
            let arr: [u8; 4] = bytes[start..start + 4].try_into().ok()?;
            out.push(f32::from_le_bytes(arr));
        }
        return Some(out);
    }
    let parts: Vec<&str> = s
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() != dimensions {
        return None;
    }
    let mut out = Vec::with_capacity(dimensions);
    for p in parts {
        out.push(p.parse::<f32>().ok()?);
    }
    Some(out)
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

    /// Drop every index and alias (full wipe — AOF load failure / hard reset).
    ///
    /// Not used by live FLUSHDB/FLUSHALL (those keep schema via `clear_documents`).
    pub fn clear(&self) {
        // Lock order: aliases then indices (matches create/drop/alias_*).
        let mut aliases = self.aliases.write();
        let mut indices = self.indices.write();
        aliases.clear();
        indices.clear();
    }

    /// Take every index + alias, leaving this manager empty.
    ///
    /// Used by scratch-load swap: move search state between caches without
    /// re-creating definitions/documents.
    pub fn take_all(
        &self,
    ) -> (
        HashMap<String, Arc<RwLock<SearchIndex>>>,
        HashMap<String, String>,
    ) {
        // Lock order: aliases then indices (matches create/drop/alias_*).
        let mut aliases = self.aliases.write();
        let mut indices = self.indices.write();
        (std::mem::take(&mut *indices), std::mem::take(&mut *aliases))
    }

    /// Install index + alias maps (replaces any existing state).
    ///
    /// Intended for `take_all` → swap paths. Debug builds assert every alias
    /// target names an installed index (catch take/install pairing bugs).
    pub fn install(
        &self,
        indices: HashMap<String, Arc<RwLock<SearchIndex>>>,
        aliases: HashMap<String, String>,
    ) {
        debug_assert!(
            aliases
                .values()
                .all(|target| indices.contains_key(target)),
            "SearchIndexManager::install: alias target missing from indices map"
        );
        // Lock order: aliases then indices (matches create/drop/alias_*).
        let mut a = self.aliases.write();
        let mut i = self.indices.write();
        *a = aliases;
        *i = indices;
    }

    /// Clear documents from every index while keeping definitions and aliases.
    ///
    /// Used by FLUSHDB/FLUSHALL (RediSearch-style: keys/docs gone, schema remains).
    ///
    /// **Memory:** this does **not** adjust [`crate::memory::MemoryTracker`]
    /// Search bytes. Callers that account search memory must reset or
    /// deallocate separately — e.g. [`crate::cache::Cache::flush`] always
    /// `memory_tracker.reset()` afterward. Safe only when paired that way.
    pub fn clear_documents(&self) {
        let indices = self.indices.read();
        for idx in indices.values() {
            idx.write().clear_documents();
        }
    }

    /// True if any index definition or alias is present (single lock pair).
    pub fn has_any_state(&self) -> bool {
        // Lock order: aliases then indices (matches create/drop/alias_*).
        let aliases = self.aliases.read();
        let indices = self.indices.read();
        !aliases.is_empty() || !indices.is_empty()
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

    /// Get an index (resolves aliases).
    ///
    /// Holds aliases then indices locks for the resolve+lookup (no TOCTOU
    /// between a concurrent alias retarget and the indices map read).
    pub fn get_index(&self, name: &str) -> Option<Arc<RwLock<SearchIndex>>> {
        // Lock order: aliases then indices (matches create/drop/alias_*).
        let aliases = self.aliases.read();
        let real = Self::resolve_name_locked(&aliases, name);
        let indices = self.indices.read();
        indices.get(&real).cloned()
    }

    /// List all index names (aliases are not listed)
    pub fn list_indices(&self) -> Vec<String> {
        let indices = self.indices.read();
        indices.keys().cloned().collect()
    }

    /// List all index definitions (real indices only; sorted by name for stable rewrite).
    pub fn list_definitions(&self) -> Vec<IndexDefinition> {
        let indices = self.indices.read();
        let mut defs: Vec<IndexDefinition> = indices
            .values()
            .map(|idx| idx.read().definition.clone())
            .collect();
        defs.sort_by(|a, b| a.name.cmp(&b.name));
        defs
    }

    /// List all aliases as `(alias, real_index_name)` pairs (sorted by alias for stable rewrite).
    pub fn list_aliases(&self) -> Vec<(String, String)> {
        let aliases = self.aliases.read();
        let mut pairs: Vec<(String, String)> = aliases
            .iter()
            .map(|(a, i)| (a.clone(), i.clone()))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
    }

    /// Export all non-empty HNSW graphs: `(index_name, field_name, snapshot)`.
    ///
    /// Sorted by `(index_name, field_name)` for stable RDB rewrite (Batch FV).
    pub fn export_hnsw_graphs(&self) -> Vec<(String, String, HnswGraphSnapshot)> {
        let indices = self.indices.read();
        let mut out = Vec::new();
        for (name, idx) in indices.iter() {
            let guard = idx.read();
            for (field, snap) in guard.export_hnsw_graphs() {
                out.push((name.clone(), field, snap));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        out
    }

    /// Apply durable HNSW graphs after vectors have been loaded (RDB restore).
    ///
    /// Unknown index/field names are skipped with an error string collected;
    /// first hard apply failure returns `Err`. Empty input is a no-op.
    pub fn apply_hnsw_graphs(
        &self,
        graphs: &[(String, String, HnswGraphSnapshot)],
    ) -> Result<(), String> {
        for (index_name, field, snap) in graphs {
            let Some(idx) = self.get_index(index_name) else {
                return Err(format!(
                    "HNSW graph restore: unknown index '{}'",
                    index_name
                ));
            };
            idx.write().apply_hnsw_graph(field, snap)?;
        }
        Ok(())
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

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn argv(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|p| b(p)).collect()
    }

    #[test]
    fn schema_eq_ignores_created_at() {
        let fields = vec![FieldDefinition {
            name: "title".into(),
            field_type: FieldType::Text {
                weight: 1.0,
                sortable: false,
            },
        }];
        let a = IndexDefinition {
            name: "idx".into(),
            prefix: vec!["doc:".into()],
            fields: fields.clone(),
            created_at: 1,
        };
        let b = IndexDefinition {
            name: "idx".into(),
            prefix: vec!["doc:".into()],
            fields,
            created_at: 999,
        };
        assert!(a.schema_eq(&b));
        assert!(b.schema_eq(&a));
        assert!(a.schema_diff_summary(&b).is_none());
    }

    #[test]
    fn schema_eq_detects_prefix_and_field_diff() {
        let text = FieldDefinition {
            name: "title".into(),
            field_type: FieldType::Text {
                weight: 1.0,
                sortable: false,
            },
        };
        let base = IndexDefinition {
            name: "idx".into(),
            prefix: vec!["doc:".into()],
            fields: vec![text.clone()],
            created_at: 1,
        };
        let other_prefix = IndexDefinition {
            name: "idx".into(),
            prefix: vec!["other:".into()],
            fields: vec![text.clone()],
            created_at: 1,
        };
        assert!(!base.schema_eq(&other_prefix));
        let sum = base.schema_diff_summary(&other_prefix).unwrap();
        assert!(sum.contains("prefix"), "got {sum}");

        let other_field = IndexDefinition {
            name: "idx".into(),
            prefix: vec!["doc:".into()],
            fields: vec![FieldDefinition {
                name: "body".into(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            }],
            created_at: 1,
        };
        assert!(!base.schema_eq(&other_field));
        let sum = base.schema_diff_summary(&other_field).unwrap();
        assert!(sum.contains("fields"), "got {sum}");
    }

    #[test]
    fn from_ft_create_argv_full_schema() {
        let args = argv(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "WEIGHT",
            "2.0",
            "SORTABLE",
            "price",
            "NUMERIC",
            "SORTABLE",
            "tags",
            "TAG",
            "SEPARATOR",
            "|",
            "emb",
            "VECTOR",
            "HNSW",
            "M",
            "32",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "L2",
        ]);
        let def = IndexDefinition::from_ft_create_argv(&args).expect("parse");
        assert_eq!(def.name, "idx");
        assert_eq!(def.prefix, vec!["doc:".to_string()]);
        assert_eq!(def.fields.len(), 4);
        assert_eq!(
            def.fields[0],
            FieldDefinition {
                name: "title".into(),
                field_type: FieldType::Text {
                    weight: 2.0,
                    sortable: true,
                },
            }
        );
        assert_eq!(
            def.fields[1],
            FieldDefinition {
                name: "price".into(),
                field_type: FieldType::Numeric { sortable: true },
            }
        );
        assert_eq!(
            def.fields[2],
            FieldDefinition {
                name: "tags".into(),
                field_type: FieldType::Tag {
                    separator: "|".into(),
                    sortable: false,
                },
            }
        );
        assert_eq!(
            def.fields[3],
            FieldDefinition {
                name: "emb".into(),
                field_type: FieldType::Vector {
                    algorithm: VectorAlgorithm::HNSW {
                        m: 32,
                        ef_construction: 200,
                    },
                    dimensions: 4,
                    distance_metric: DistanceMetric::L2,
                },
            }
        );
    }

    #[test]
    fn from_ft_create_args_without_command_token() {
        let with_cmd = argv(&[
            "FT.CREATE",
            "a",
            "SCHEMA",
            "t",
            "TEXT",
            "v",
            "VECTOR",
            "FLAT",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "IP",
        ]);
        let without_cmd = argv(&[
            "a",
            "SCHEMA",
            "t",
            "TEXT",
            "v",
            "VECTOR",
            "FLAT",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "IP",
        ]);
        let d1 = IndexDefinition::from_ft_create_argv(&with_cmd).unwrap();
        let d2 = IndexDefinition::from_ft_create_args(&without_cmd).unwrap();
        assert_eq!(d1.name, d2.name);
        assert_eq!(d1.prefix, d2.prefix);
        assert_eq!(d1.fields, d2.fields);
    }

    #[test]
    fn from_ft_create_rejects_empty_schema() {
        let args = argv(&["FT.CREATE", "idx", "PREFIX", "1", "x:"]);
        let err = IndexDefinition::from_ft_create_argv(&args).unwrap_err();
        assert!(err.contains("no fields"), "got {err}");
    }

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

    #[test]
    fn has_any_state_indices_or_aliases() {
        let manager = SearchIndexManager::new();
        assert!(!manager.has_any_state());
        manager
            .create_index(IndexDefinition::new(
                "idx".to_string(),
                vec!["doc:".to_string()],
                vec![FieldDefinition {
                    name: "t".to_string(),
                    field_type: FieldType::Text {
                        weight: 1.0,
                        sortable: false,
                    },
                }],
            ))
            .unwrap();
        assert!(manager.has_any_state());
        manager.alias_add("blog", "idx").unwrap();
        assert!(manager.has_any_state());
        manager.drop_index("idx").unwrap(); // drops alias targets too
        assert!(!manager.has_any_state());

        // Alias-only is impossible (alias requires an index); empty after drop.
        manager
            .create_index(IndexDefinition::new(
                "i2".to_string(),
                vec![],
                vec![FieldDefinition {
                    name: "t".to_string(),
                    field_type: FieldType::Text {
                        weight: 1.0,
                        sortable: false,
                    },
                }],
            ))
            .unwrap();
        manager.alias_add("a", "i2").unwrap();
        assert!(manager.has_any_state());
    }

    #[test]
    fn clear_documents_keeps_schema_and_aliases() {
        let manager = SearchIndexManager::new();
        let definition = IndexDefinition::new(
            "idx".to_string(),
            vec!["doc:".to_string()],
            vec![FieldDefinition {
                name: "title".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            }],
        );
        manager.create_index(definition).unwrap();
        manager.alias_add("blog", "idx").unwrap();

        {
            let idx = manager.get_index("idx").unwrap();
            let mut guard = idx.write();
            let mut fields = HashMap::new();
            fields.insert("title".to_string(), DocumentField::Text("hello".into()));
            guard.index_document(Bytes::from("doc:1"), fields);
            assert_eq!(guard.size(), 1);
        }

        manager.clear_documents();

        assert_eq!(manager.list_indices(), vec!["idx".to_string()]);
        assert_eq!(
            manager.list_aliases(),
            vec![("blog".to_string(), "idx".to_string())]
        );
        let idx = manager.get_index("idx").unwrap();
        assert_eq!(idx.read().size(), 0);
        assert_eq!(idx.read().definition.name, "idx");

        // Full clear drops schema + aliases.
        manager.clear();
        assert!(manager.list_indices().is_empty());
        assert!(manager.list_aliases().is_empty());
    }
}
