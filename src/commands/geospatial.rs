use crate::error::Result;
use crate::protocol::RespValue;
use crate::geospatial::{DistanceUnit, GeoSearchResult};
use bytes::Bytes;
use super::CommandHandler;

impl CommandHandler {
    /// Handle GEOADD command
    /// GEOADD key longitude latitude member [longitude latitude member ...]
    pub(super) fn handle_geoadd(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geoadd' command",
            ));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Parse longitude, latitude, member triplets
        let mut triplets = Vec::new();
        let mut i = 1;

        while i + 2 < args.len() {
            let longitude = match self.parse_float(&args[i]) {
                Ok(lon) => lon,
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not a valid float",
                    ))
                }
            };

            let latitude = match self.parse_float(&args[i + 1]) {
                Ok(lat) => lat,
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not a valid float",
                    ))
                }
            };

            let member = match args[i + 2].as_bulk_string() {
                Some(m) => m.clone(),
                None => return Ok(RespValue::error("ERR invalid member")),
            };

            triplets.push((longitude, latitude, member));
            i += 3;
        }

        if triplets.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geoadd' command",
            ));
        }

        // Update stats
        self.cache.stats.incr(&self.cache.stats.cmd_geoadd);

        // Get or create geospatial set
        let geoset = self.cache.get_or_create_geo_set(&key);
        let mut set = geoset.write().unwrap();

        let mut added = 0;
        for (lon, lat, member) in triplets {
            match set.add(member, lon, lat) {
                Some(true) => added += 1,  // New member added
                Some(false) => {},  // Existing member updated
                None => {
                    // Invalid coordinates
                    return Ok(RespValue::error(
                        "ERR invalid longitude,latitude pair",
                    ));
                }
            }
        }

        Ok(RespValue::Integer(added as i64))
    }

    /// Handle GEOSEARCH command
    /// GEOSEARCH key FROMMEMBER member | FROMLONLAT longitude latitude
    ///   BYRADIUS radius M|KM|FT|MI [WITHDIST] [WITHCOORD] [COUNT count]
    pub(super) fn handle_geosearch(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 5 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geosearch' command",
            ));
        }

        // Update stats
        self.cache.stats.incr(&self.cache.stats.cmd_geosearch);

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Parse FROM clause
        let from_clause = match args[1].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };

        let (center_lon, center_lat, from_idx) = match from_clause.as_str() {
            "FROMMEMBER" => {
                if args.len() < 3 {
                    return Ok(RespValue::error("ERR syntax error"));
                }
                let member = match args[2].as_bulk_string() {
                    Some(m) => m,
                    None => return Ok(RespValue::error("ERR invalid member")),
                };

                let geoset = match self.cache.get_geo_set(key) {
                    Some(g) => g,
                    None => return Ok(RespValue::Array(vec![])),
                };

                let set = geoset.read().unwrap();
                let (lon, lat) = match set.get_position(member) {
                    Some(pos) => pos,
                    None => return Ok(RespValue::error("ERR could not decode requested member")),
                };

                (lon, lat, 3)
            }
            "FROMLONLAT" => {
                if args.len() < 4 {
                    return Ok(RespValue::error("ERR syntax error"));
                }
                let lon = match self.parse_float(&args[2]) {
                    Ok(l) => l,
                    Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
                };
                let lat = match self.parse_float(&args[3]) {
                    Ok(l) => l,
                    Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
                };
                (lon, lat, 4)
            }
            _ => return Ok(RespValue::error("ERR syntax error")),
        };

        // Parse BYRADIUS clause
        if from_idx >= args.len() {
            return Ok(RespValue::error("ERR syntax error"));
        }

        let shape_clause = match args[from_idx].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };

        if shape_clause != "BYRADIUS" {
            return Ok(RespValue::error("ERR syntax error"));
        }

        if from_idx + 2 >= args.len() {
            return Ok(RespValue::error("ERR syntax error"));
        }

        let radius = match self.parse_float(&args[from_idx + 1]) {
            Ok(r) => r,
            Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
        };

        let unit_str = match args[from_idx + 2].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s),
            None => return Ok(RespValue::error("ERR syntax error")),
        };

        let unit = match DistanceUnit::from_str(&unit_str) {
            Some(u) => u,
            None => return Ok(RespValue::error("ERR unsupported unit provided. please use M, KM, FT, MI")),
        };

        // Parse optional flags
        let mut with_dist = false;
        let mut with_coord = false;
        let mut count: Option<usize> = None;
        let mut idx = from_idx + 3;

        while idx < args.len() {
            let opt = match args[idx].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => break,
            };

            match opt.as_str() {
                "WITHDIST" => {
                    with_dist = true;
                    idx += 1;
                }
                "WITHCOORD" => {
                    with_coord = true;
                    idx += 1;
                }
                "COUNT" => {
                    if idx + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    count = Some(match self.parse_integer(&args[idx + 1]) {
                        Ok(c) => c as usize,
                        Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
                    });
                    idx += 2;
                }
                _ => break,
            }
        }

        // Perform search
        let geoset = match self.cache.get_geo_set(key) {
            Some(g) => g,
            None => return Ok(RespValue::Array(vec![])),
        };

        let set = geoset.read().unwrap();
        let mut results = set.search_radius(center_lon, center_lat, radius, unit);

        // Apply count limit
        if let Some(c) = count {
            results.truncate(c);
        }

        // Format results
        let resp_results: Vec<RespValue> = results
            .iter()
            .map(|result| self.format_geosearch_result(result, with_dist, with_coord, &unit))
            .collect();

        Ok(RespValue::Array(resp_results))
    }

    /// Format a single geosearch result based on options
    fn format_geosearch_result(
        &self,
        result: &GeoSearchResult,
        with_dist: bool,
        with_coord: bool,
        unit: &DistanceUnit,
    ) -> RespValue {
        if !with_dist && !with_coord {
            // Just return the member name
            RespValue::BulkString(Some(result.member.clone()))
        } else {
            // Return an array with member and optional info
            let mut items = vec![RespValue::BulkString(Some(result.member.clone()))];

            if with_dist {
                let distance = unit.from_meters(result.distance);
                items.push(RespValue::BulkString(Some(Bytes::from(format!("{:.4}", distance)))));
            }

            if with_coord {
                let coords = vec![
                    RespValue::BulkString(Some(Bytes::from(format!("{:.6}", result.longitude)))),
                    RespValue::BulkString(Some(Bytes::from(format!("{:.6}", result.latitude)))),
                ];
                items.push(RespValue::Array(coords));
            }

            RespValue::Array(items)
        }
    }
}
