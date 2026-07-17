use crate::commands::CommandHandler;
use crate::error::Result;
use crate::protocol::RespValue;
use crate::search_index::{DocumentField, IndexDefinition};
use bytes::Bytes;

impl CommandHandler {
    /// FT.CREATE index [ON HASH] [PREFIX count prefix [prefix ...]] SCHEMA field_name field_type [options ...]
    ///
    /// Schema parsing is shared with AOF load via
    /// [`IndexDefinition::from_ft_create_argv`].
    pub fn handle_ft_create(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'FT.CREATE' command",
            ));
        }

        // Convert RESP args to bulk tokens for the shared parser.
        let mut tokens: Vec<Bytes> = Vec::with_capacity(args.len());
        for a in args {
            match a.as_bulk_string() {
                Some(b) => tokens.push(Bytes::copy_from_slice(b)),
                None => return Ok(RespValue::error("ERR invalid argument")),
            }
        }

        let definition = match IndexDefinition::from_ft_create_args(&tokens) {
            Ok(def) => def,
            Err(e) => return Ok(RespValue::error(e)),
        };

        match self.cache.create_search_index(definition) {
            Ok(_) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    /// FT.DROPINDEX index [DD]
    ///
    /// `index` may be a real index name or an alias; on success all aliases that
    /// pointed at the dropped index are removed.
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

    /// FT.ALIASADD alias index — bind a new alias to an existing index.
    pub fn handle_ft_aliasadd(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'FT.ALIASADD' command",
            ));
        }
        let alias = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid alias name")),
        };
        let index = match args[1].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid index name")),
        };
        match self.cache.alias_add(&alias, &index) {
            Ok(_) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    /// FT.ALIASDEL alias — remove an alias.
    pub fn handle_ft_aliasdel(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'FT.ALIASDEL' command",
            ));
        }
        let alias = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid alias name")),
        };
        match self.cache.alias_del(&alias) {
            Ok(_) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    /// FT.ALIASUPDATE alias index — create or retarget an alias.
    pub fn handle_ft_aliasupdate(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'FT.ALIASUPDATE' command",
            ));
        }
        let alias = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid alias name")),
        };
        let index = match args[1].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_string(),
            None => return Ok(RespValue::error("ERR invalid index name")),
        };
        match self.cache.alias_update(&alias, &index) {
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
