use crate::commands::CommandHandler;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::search_index::{
    FieldDefinition, FieldType, IndexDefinition, DocumentField, DistanceMetric, VectorAlgorithm,
};
use crate::query_engine::{Query, QueryFilter, QueryOperator, SortBy, SortOrder};
use bytes::Bytes;
use std::collections::HashMap;

impl CommandHandler {
    /// FT.CREATE index [ON HASH] [PREFIX count prefix [prefix ...]] SCHEMA field_name field_type [options ...]
    pub fn handle_ft_create(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'FT.CREATE' command"));
        }

        let index_name = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid index name")),
        };

        let mut i = 1;
        let mut prefix_list = Vec::new();
        let mut fields = Vec::new();

        // Parse optional PREFIX
        while i < args.len() {
            let arg = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => break,
            };

            match arg.as_str() {
                "ON" => {
                    // Skip ON HASH for now
                    i += 2;
                }
                "PREFIX" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::error("ERR missing prefix count"));
                    }

                    let count = self.parse_integer(&args[i])? as usize;
                    i += 1;

                    for _ in 0..count {
                        if i >= args.len() {
                            return Ok(RespValue::error("ERR missing prefix"));
                        }
                        if let Some(prefix) = args[i].as_bulk_string() {
                            prefix_list.push(String::from_utf8_lossy(prefix).to_string());
                        }
                        i += 1;
                    }
                }
                "SCHEMA" => {
                    i += 1;
                    break;
                }
                _ => {
                    return Ok(RespValue::error(format!("ERR unknown option '{}'", arg)));
                }
            }
        }

        // Parse SCHEMA
        while i < args.len() {
            if i + 1 >= args.len() {
                return Ok(RespValue::error("ERR incomplete field definition"));
            }

            let field_name = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_string(),
                None => return Ok(RespValue::error("ERR invalid field name")),
            };
            i += 1;

            let field_type_str = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Ok(RespValue::error("ERR invalid field type")),
            };
            i += 1;

            let field_type = match field_type_str.as_str() {
                "TEXT" => {
                    let mut weight = 1.0;
                    let mut sortable = false;

                    // Parse TEXT options
                    while i < args.len() {
                        if let Some(opt) = args[i].as_bulk_string() {
                            let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                            match opt_str.as_str() {
                                "WEIGHT" => {
                                    i += 1;
                                    if i >= args.len() {
                                        return Ok(RespValue::error("ERR missing weight value"));
                                    }
                                    weight = match args[i].as_bulk_string() {
                                        Some(s) => String::from_utf8_lossy(s).parse()
                                            .map_err(|_| Error::InvalidArgument("invalid weight".into()))?,
                                        None => return Ok(RespValue::error("ERR invalid weight")),
                                    };
                                    i += 1;
                                }
                                "SORTABLE" => {
                                    sortable = true;
                                    i += 1;
                                }
                                _ => break,
                            }
                        } else {
                            break;
                        }
                    }

                    FieldType::Text { weight, sortable }
                }
                "NUMERIC" => {
                    let mut sortable = false;

                    if i < args.len() {
                        if let Some(opt) = args[i].as_bulk_string() {
                            let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                            if opt_str == "SORTABLE" {
                                sortable = true;
                                i += 1;
                            }
                        }
                    }

                    FieldType::Numeric { sortable }
                }
                "TAG" => {
                    let mut separator = ",".to_string();
                    let mut sortable = false;

                    while i < args.len() {
                        if let Some(opt) = args[i].as_bulk_string() {
                            let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                            match opt_str.as_str() {
                                "SEPARATOR" => {
                                    i += 1;
                                    if i >= args.len() {
                                        return Ok(RespValue::error("ERR missing separator"));
                                    }
                                    separator = match args[i].as_bulk_string() {
                                        Some(s) => String::from_utf8_lossy(s).to_string(),
                                        None => return Ok(RespValue::error("ERR invalid separator")),
                                    };
                                    i += 1;
                                }
                                "SORTABLE" => {
                                    sortable = true;
                                    i += 1;
                                }
                                _ => break,
                            }
                        } else {
                            break;
                        }
                    }

                    FieldType::Tag { separator, sortable }
                }
                "VECTOR" => {
                    // VECTOR algorithm TYPE DIMENSION metric [options]
                    if i + 5 >= args.len() {
                        return Ok(RespValue::error("ERR incomplete VECTOR field definition"));
                    }

                    let algo_str = match args[i].as_bulk_string() {
                        Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                        None => return Ok(RespValue::error("ERR invalid algorithm")),
                    };
                    i += 1;

                    let algorithm = match algo_str.as_str() {
                        "FLAT" => VectorAlgorithm::Flat,
                        "HNSW" => {
                            let mut m = 16;
                            let mut ef_construction = 200;

                            // Parse HNSW options (simplified)
                            if i < args.len() {
                                if let Some(opt) = args[i].as_bulk_string() {
                                    let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                                    if opt_str == "M" {
                                        i += 1;
                                        if i < args.len() {
                                            m = self.parse_integer(&args[i])? as usize;
                                            i += 1;
                                        }
                                    }
                                }
                            }

                            VectorAlgorithm::HNSW { m, ef_construction }
                        }
                        _ => return Ok(RespValue::error(format!("ERR unknown vector algorithm '{}'", algo_str))),
                    };

                    // Skip "TYPE" keyword
                    if i < args.len() {
                        if let Some(s) = args[i].as_bulk_string() {
                            let s_str = String::from_utf8_lossy(s).to_uppercase();
                            if s_str == "TYPE" {
                                i += 1;
                            }
                        }
                    }

                    // Skip type (e.g., "FLOAT32")
                    i += 1;

                    // Parse DIM or DIMENSION
                    let dimensions = if i < args.len() {
                        if let Some(s) = args[i].as_bulk_string() {
                            let s_str = String::from_utf8_lossy(s).to_uppercase();
                            if s_str == "DIM" || s_str == "DIMENSION" {
                                i += 1;
                                if i >= args.len() {
                                    return Ok(RespValue::error("ERR missing dimension value"));
                                }
                                let dim = self.parse_integer(&args[i])? as usize;
                                i += 1;
                                dim
                            } else {
                                return Ok(RespValue::error("ERR missing DIM keyword"));
                            }
                        } else {
                            return Ok(RespValue::error("ERR missing DIM keyword"));
                        }
                    } else {
                        return Ok(RespValue::error("ERR missing dimension"));
                    };

                    // Parse DISTANCE_METRIC
                    let distance_metric = if i < args.len() {
                        if let Some(s) = args[i].as_bulk_string() {
                            let s_str = String::from_utf8_lossy(s).to_uppercase();
                            if s_str == "DISTANCE_METRIC" {
                                i += 1;
                                if i >= args.len() {
                                    return Ok(RespValue::error("ERR missing distance metric"));
                                }
                                let metric_str = match args[i].as_bulk_string() {
                                    Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                                    None => return Ok(RespValue::error("ERR invalid distance metric")),
                                };
                                i += 1;

                                match metric_str.as_str() {
                                    "COSINE" => DistanceMetric::Cosine,
                                    "L2" => DistanceMetric::L2,
                                    "IP" => DistanceMetric::IP,
                                    _ => return Ok(RespValue::error(format!("ERR unknown distance metric '{}'", metric_str))),
                                }
                            } else {
                                DistanceMetric::Cosine // Default
                            }
                        } else {
                            DistanceMetric::Cosine // Default
                        }
                    } else {
                        DistanceMetric::Cosine // Default
                    };

                    FieldType::Vector {
                        algorithm,
                        dimensions,
                        distance_metric,
                    }
                }
                _ => {
                    return Ok(RespValue::error(format!("ERR unknown field type '{}'", field_type_str)));
                }
            };

            fields.push(FieldDefinition {
                name: field_name,
                field_type,
            });
        }

        if fields.is_empty() {
            return Ok(RespValue::error("ERR no fields defined"));
        }

        let definition = IndexDefinition::new(index_name, prefix_list, fields);

        match self.cache.create_search_index(definition) {
            Ok(_) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    /// FT.DROPINDEX index [DD]
    pub fn handle_ft_dropindex(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'FT.DROPINDEX' command"));
        }

        let index_name = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid index name")),
        };

        match self.cache.drop_search_index(&index_name) {
            Ok(_) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    /// FT._LIST
    pub fn handle_ft_list(&self, _args: &[RespValue]) -> Result<RespValue> {
        let indices = self.cache.list_search_indices();
        let resp_indices: Vec<RespValue> = indices
            .into_iter()
            .map(|name| RespValue::BulkString(Some(Bytes::from(name))))
            .collect();
        Ok(RespValue::Array(resp_indices))
    }

    /// FT.INFO index
    pub fn handle_ft_info(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'FT.INFO' command"));
        }

        let index_name = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid index name")),
        };

        match self.cache.get_search_index_info(&index_name) {
            Some(info) => {
                let mut result = Vec::new();

                result.push(RespValue::BulkString(Some(Bytes::from_static(b"index_name"))));
                result.push(RespValue::BulkString(Some(Bytes::from(info.name))));

                result.push(RespValue::BulkString(Some(Bytes::from_static(b"num_docs"))));
                result.push(RespValue::Integer(info.num_docs as i64));

                result.push(RespValue::BulkString(Some(Bytes::from_static(b"num_fields"))));
                result.push(RespValue::Integer(info.num_fields as i64));

                result.push(RespValue::BulkString(Some(Bytes::from_static(b"fields"))));
                let fields: Vec<RespValue> = info.fields
                    .iter()
                    .map(|f| RespValue::BulkString(Some(Bytes::from(f.clone()))))
                    .collect();
                result.push(RespValue::Array(fields));

                Ok(RespValue::Array(result))
            }
            None => Ok(RespValue::error(format!("ERR index '{}' not found", index_name))),
        }
    }

    /// FT.TAGVALS index fieldName — distinct tag values for a TAG field.
    pub fn handle_ft_tagvals(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'FT.TAGVALS' command",
            ));
        }
        let index_name = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid index name")),
        };
        let field_name = match args[1].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid field name")),
        };

        match self.cache.get_tag_values(&index_name, &field_name) {
            Ok(Some(values)) => {
                let arr = values
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(Bytes::from(v))))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Ok(None) => Ok(RespValue::error(format!(
                "ERR Unknown index name '{}'",
                index_name
            ))),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    /// FT.SEARCH index query [options...]
    pub fn handle_ft_search(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'FT.SEARCH' command"));
        }

        let index_name = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid index name")),
        };

        let query_str = match args[1].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid query")),
        };

        // Parse options
        let mut limit = 10;
        let mut offset = 0;
        let mut return_fields: Option<Vec<String>> = None;
        let mut i = 2;

        while i < args.len() {
            if let Some(opt) = args[i].as_bulk_string() {
                let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                match opt_str.as_str() {
                    "LIMIT" => {
                        if i + 2 >= args.len() {
                            return Ok(RespValue::error("ERR LIMIT requires offset and count"));
                        }
                        i += 1;
                        offset = self.parse_integer(&args[i])? as usize;
                        i += 1;
                        limit = self.parse_integer(&args[i])? as usize;
                        i += 1;
                    }
                    "RETURN" => {
                        if i + 1 >= args.len() {
                            return Ok(RespValue::error("ERR RETURN requires field count"));
                        }
                        i += 1;
                        let count = self.parse_integer(&args[i])? as usize;
                        i += 1;

                        let mut fields = Vec::new();
                        for _ in 0..count {
                            if i >= args.len() {
                                return Ok(RespValue::error("ERR not enough RETURN fields"));
                            }
                            if let Some(field) = args[i].as_bulk_string() {
                                fields.push(String::from_utf8_lossy(field).to_string());
                            }
                            i += 1;
                        }
                        return_fields = Some(fields);
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        // Execute search
        match self.cache.search(&index_name, &query_str, limit, offset) {
            Ok(results) => {
                let mut resp = Vec::new();
                resp.push(RespValue::Integer(results.total as i64));

                for result in results.documents {
                    resp.push(RespValue::BulkString(Some(result.id.clone())));

                    let mut fields_resp = Vec::new();
                    for (field_name, field_value) in result.fields {
                        // Filter by RETURN fields if specified
                        if let Some(ref return_f) = return_fields {
                            if !return_f.contains(&field_name) {
                                continue;
                            }
                        }

                        fields_resp.push(RespValue::BulkString(Some(Bytes::from(field_name))));

                        let value_resp = match field_value {
                            DocumentField::Text(s) => RespValue::BulkString(Some(Bytes::from(s))),
                            DocumentField::Numeric(n) => RespValue::BulkString(Some(Bytes::from(n.to_string()))),
                            DocumentField::Tag(tags) => RespValue::BulkString(Some(Bytes::from(tags.join(",")))),
                            DocumentField::Vector(vec) => {
                                let vec_str = format!("{:?}", vec);
                                RespValue::BulkString(Some(Bytes::from(vec_str)))
                            }
                        };
                        fields_resp.push(value_resp);
                    }

                    resp.push(RespValue::Array(fields_resp));
                }

                Ok(RespValue::Array(resp))
            }
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }
}
