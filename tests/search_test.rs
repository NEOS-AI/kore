use kore::{Cache, IndexDefinition, FieldDefinition, FieldType, DocumentField, DistanceMetric, VectorAlgorithm};
use bytes::Bytes;
use std::collections::HashMap;

#[test]
fn test_create_text_index() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "articles".to_string(),
        vec!["article:".to_string()],
        vec![
            FieldDefinition {
                name: "title".to_string(),
                field_type: FieldType::Text {
                    weight: 2.0,
                    sortable: false,
                },
            },
            FieldDefinition {
                name: "body".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            },
        ],
    );

    let result = cache.create_search_index(definition);
    assert!(result.is_ok());

    let indices = cache.list_search_indices();
    assert_eq!(indices.len(), 1);
    assert_eq!(indices[0], "articles");
}

#[test]
fn test_index_and_search_documents() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    // Create index
    let definition = IndexDefinition::new(
        "docs".to_string(),
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

    // Index documents
    let mut fields1 = HashMap::new();
    fields1.insert(
        "content".to_string(),
        DocumentField::Text("The quick brown fox jumps over the lazy dog".to_string()),
    );
    cache.index_document("docs", Bytes::from("doc1"), fields1).unwrap();

    let mut fields2 = HashMap::new();
    fields2.insert(
        "content".to_string(),
        DocumentField::Text("A lazy cat sleeps all day".to_string()),
    );
    cache.index_document("docs", Bytes::from("doc2"), fields2).unwrap();

    let mut fields3 = HashMap::new();
    fields3.insert(
        "content".to_string(),
        DocumentField::Text("The fox is very quick and agile".to_string()),
    );
    cache.index_document("docs", Bytes::from("doc3"), fields3).unwrap();

    // Search for "lazy"
    let results = cache.search("docs", "lazy", 10, 0).unwrap();
    assert_eq!(results.total, 2); // doc1 and doc2

    // Search for "fox"
    let results = cache.search("docs", "fox", 10, 0).unwrap();
    assert_eq!(results.total, 2); // doc1 and doc3
}

#[test]
fn test_numeric_index() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "products".to_string(),
        vec![],
        vec![
            FieldDefinition {
                name: "name".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            },
            FieldDefinition {
                name: "price".to_string(),
                field_type: FieldType::Numeric {
                    sortable: true,
                },
            },
        ],
    );

    cache.create_search_index(definition).unwrap();

    // Index products
    let mut fields1 = HashMap::new();
    fields1.insert("name".to_string(), DocumentField::Text("Laptop".to_string()));
    fields1.insert("price".to_string(), DocumentField::Numeric(999.99));
    cache.index_document("products", Bytes::from("prod1"), fields1).unwrap();

    let mut fields2 = HashMap::new();
    fields2.insert("name".to_string(), DocumentField::Text("Mouse".to_string()));
    fields2.insert("price".to_string(), DocumentField::Numeric(29.99));
    cache.index_document("products", Bytes::from("prod2"), fields2).unwrap();

    let mut fields3 = HashMap::new();
    fields3.insert("name".to_string(), DocumentField::Text("Keyboard".to_string()));
    fields3.insert("price".to_string(), DocumentField::Numeric(79.99));
    cache.index_document("products", Bytes::from("prod3"), fields3).unwrap();

    let info = cache.get_search_index_info("products").unwrap();
    assert_eq!(info.num_docs, 3);
}

#[test]
fn test_tag_index() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "posts".to_string(),
        vec![],
        vec![
            FieldDefinition {
                name: "title".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            },
            FieldDefinition {
                name: "tags".to_string(),
                field_type: FieldType::Tag {
                    separator: ",".to_string(),
                    sortable: false,
                },
            },
        ],
    );

    cache.create_search_index(definition).unwrap();

    // Index posts with tags
    let mut fields1 = HashMap::new();
    fields1.insert("title".to_string(), DocumentField::Text("Rust Tutorial".to_string()));
    fields1.insert("tags".to_string(), DocumentField::Tag(vec!["rust".to_string(), "programming".to_string()]));
    cache.index_document("posts", Bytes::from("post1"), fields1).unwrap();

    let mut fields2 = HashMap::new();
    fields2.insert("title".to_string(), DocumentField::Text("Python Guide".to_string()));
    fields2.insert("tags".to_string(), DocumentField::Tag(vec!["python".to_string(), "programming".to_string()]));
    cache.index_document("posts", Bytes::from("post2"), fields2).unwrap();

    let info = cache.get_search_index_info("posts").unwrap();
    assert_eq!(info.num_docs, 2);
}

#[test]
fn test_vector_index() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "embeddings".to_string(),
        vec![],
        vec![
            FieldDefinition {
                name: "text".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            },
            FieldDefinition {
                name: "embedding".to_string(),
                field_type: FieldType::Vector {
                    algorithm: VectorAlgorithm::Flat,
                    dimensions: 3,
                    distance_metric: DistanceMetric::Cosine,
                },
            },
        ],
    );

    cache.create_search_index(definition).unwrap();

    // Index documents with vectors
    let mut fields1 = HashMap::new();
    fields1.insert("text".to_string(), DocumentField::Text("document one".to_string()));
    fields1.insert("embedding".to_string(), DocumentField::Vector(vec![1.0, 0.0, 0.0]));
    cache.index_document("embeddings", Bytes::from("doc1"), fields1).unwrap();

    let mut fields2 = HashMap::new();
    fields2.insert("text".to_string(), DocumentField::Text("document two".to_string()));
    fields2.insert("embedding".to_string(), DocumentField::Vector(vec![0.0, 1.0, 0.0]));
    cache.index_document("embeddings", Bytes::from("doc2"), fields2).unwrap();

    let mut fields3 = HashMap::new();
    fields3.insert("text".to_string(), DocumentField::Text("document three".to_string()));
    fields3.insert("embedding".to_string(), DocumentField::Vector(vec![0.9, 0.1, 0.0]));
    cache.index_document("embeddings", Bytes::from("doc3"), fields3).unwrap();

    let info = cache.get_search_index_info("embeddings").unwrap();
    assert_eq!(info.num_docs, 3);
    assert_eq!(info.num_fields, 2);
}

#[test]
fn test_drop_index() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "temp".to_string(),
        vec![],
        vec![
            FieldDefinition {
                name: "data".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            },
        ],
    );

    cache.create_search_index(definition).unwrap();
    assert_eq!(cache.list_search_indices().len(), 1);

    cache.drop_search_index("temp").unwrap();
    assert_eq!(cache.list_search_indices().len(), 0);
}

#[test]
fn test_index_info() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "test_info".to_string(),
        vec!["test:".to_string()],
        vec![
            FieldDefinition {
                name: "field1".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            },
            FieldDefinition {
                name: "field2".to_string(),
                field_type: FieldType::Numeric {
                    sortable: true,
                },
            },
        ],
    );

    cache.create_search_index(definition).unwrap();

    let info = cache.get_search_index_info("test_info").unwrap();
    assert_eq!(info.name, "test_info");
    assert_eq!(info.num_fields, 2);
    assert_eq!(info.num_docs, 0);
    assert_eq!(info.fields.len(), 2);
    assert!(info.fields.contains(&"field1".to_string()));
    assert!(info.fields.contains(&"field2".to_string()));
}

#[test]
fn test_remove_document() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "removable".to_string(),
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
    fields.insert("content".to_string(), DocumentField::Text("test content".to_string()));
    cache.index_document("removable", Bytes::from("doc1"), fields).unwrap();

    let info = cache.get_search_index_info("removable").unwrap();
    assert_eq!(info.num_docs, 1);

    // Remove the document
    cache.remove_from_index("removable", &Bytes::from("doc1")).unwrap();

    let info = cache.get_search_index_info("removable").unwrap();
    assert_eq!(info.num_docs, 0);
}

#[test]
fn test_search_with_limit_offset() {
    let cache = Cache::new_with_sweep(16, 10 * 1024 * 1024, 500 * 1024 * 1024, false);

    let definition = IndexDefinition::new(
        "pagination".to_string(),
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

    // Index multiple documents
    for i in 1..=10 {
        let mut fields = HashMap::new();
        fields.insert(
            "content".to_string(),
            DocumentField::Text(format!("document number {}", i)),
        );
        cache.index_document("pagination", Bytes::from(format!("doc{}", i)), fields).unwrap();
    }

    // Test with limit
    let results = cache.search("pagination", "document", 5, 0).unwrap();
    assert!(results.total <= 10);

    // Test with offset
    let results = cache.search("pagination", "document", 5, 5).unwrap();
    assert!(results.total <= 10);
}
