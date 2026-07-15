use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::geospatial::{DistanceUnit, GeoAddOptions, GeoAddResult, GeoSearchResult, SortOrder, geohash_encode, geohash_to_string};
use bytes::Bytes;
use super::CommandHandler;
use std::str;

impl CommandHandler {
    /// Return WRONGTYPE if key exists but is not a geo set.
    fn ensure_geo_key(&self, key: &Bytes) -> Result<Option<()>> {
        match self.cache.key_type(key) {
            KeyType::None => Ok(None),
            KeyType::Geo => Ok(Some(())),
            _ => Err(Error::WrongType),
        }
    }

    /// Handle GEOADD command
    /// GEOADD key [NX|XX] [GET] [CH] longitude latitude member [longitude latitude member ...]
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

        // Parse optional flags before the triplets
        let mut opts = GeoAddOptions::default();
        let mut i = 1usize;

        loop {
            let flag = match args.get(i).and_then(|v| v.as_bulk_string()) {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => break,
            };
            match flag.as_str() {
                "NX" => { opts.nx = true; i += 1; }
                "XX" => { opts.xx = true; i += 1; }
                "GET" => { opts.get = true; i += 1; }
                "CH" => { opts.ch = true; i += 1; }
                _ => break,
            }
        }

        if opts.nx && opts.xx {
            return Ok(RespValue::error("ERR XX and NX options at the same time are not compatible"));
        }

        // Parse longitude, latitude, member triplets
        let mut triplets = Vec::new();
        while i + 2 < args.len() {
            let longitude = match self.parse_float(&args[i]) {
                Ok(lon) => lon,
                Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
            };
            let latitude = match self.parse_float(&args[i + 1]) {
                Ok(lat) => lat,
                Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
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

        self.cache.stats.incr(&self.cache.stats.cmd_geoadd);

        let est_growth: usize = triplets.iter().map(|(_, _, m)| m.len() + 64).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est_growth) {
            return Ok(RespValue::error(e.to_resp_string()));
        }

        let geoset = match self.cache.get_or_create_geo_set(&key) {
            Ok(g) => g,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut set = geoset.write();
        let before = key.len() + set.memory_usage();

        let mut count = 0i64;
        let mut old_positions: Vec<RespValue> = Vec::new();

        for (lon, lat, member) in triplets {
            let result = set.add_with_opts(member, lon, lat, &opts);
            match result {
                GeoAddResult::Added => {
                    count += 1;
                    if opts.get {
                        old_positions.push(RespValue::null());
                    }
                }
                GeoAddResult::Updated(old_lon, old_lat) => {
                    if opts.ch { count += 1; }
                    if opts.get {
                        old_positions.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(format!("{:.17}", old_lon)))),
                            RespValue::BulkString(Some(Bytes::from(format!("{:.17}", old_lat)))),
                        ]));
                    }
                }
                GeoAddResult::Skipped => {
                    if opts.get {
                        old_positions.push(RespValue::null());
                    }
                }
                GeoAddResult::InvalidCoords => {
                    return Ok(RespValue::error("ERR invalid longitude,latitude pair"));
                }
            }
        }

        let after = key.len() + set.memory_usage();
        drop(set);
        self.cache.account_geo_set_delta(before, after);

        if opts.get {
            Ok(RespValue::Array(old_positions))
        } else {
            Ok(RespValue::Integer(count))
        }
    }

    /// Handle GEODIST command
    /// GEODIST key member1 member2 [M|KM|FT|MI]
    pub(super) fn handle_geodist(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geodist' command",
            ));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_geo_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let member1 = match args[1].as_bulk_string() {
            Some(m) => m.clone(),
            None => return Ok(RespValue::error("ERR invalid member")),
        };
        let member2 = match args[2].as_bulk_string() {
            Some(m) => m.clone(),
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let unit = if let Some(u_arg) = args.get(3) {
            match u_arg.as_bulk_string() {
                Some(s) => match DistanceUnit::from_str(&String::from_utf8_lossy(s)) {
                    Some(u) => u,
                    None => return Ok(RespValue::error(
                        "ERR unsupported unit provided. please use M, KM, FT, MI",
                    )),
                },
                None => return Ok(RespValue::error("ERR syntax error")),
            }
        } else {
            DistanceUnit::Meters
        };

        let geoset = match self.cache.get_geo_set(key) {
            Some(g) => g,
            None => return Ok(RespValue::null()),
        };

        let set = geoset.read();
        match set.distance_between(&member1, &member2) {
            Some(dist_m) => {
                let dist = unit.from_meters(dist_m);
                Ok(RespValue::BulkString(Some(Bytes::from(format!("{:.4}", dist)))))
            }
            None => Ok(RespValue::null()),
        }
    }

    /// Handle GEOPOS command
    /// GEOPOS key member [member ...]
    pub(super) fn handle_geopos(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geopos' command",
            ));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_geo_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let geoset_opt = self.cache.get_geo_set(key);

        let results: Vec<RespValue> = args[1..]
            .iter()
            .map(|arg| {
                let member = match arg.as_bulk_string() {
                    Some(m) => m.clone(),
                    None => return RespValue::null(),
                };
                match geoset_opt.as_ref().and_then(|g| g.read().get_position(&member)) {
                    Some((lon, lat)) => RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(format!("{:.17}", lon)))),
                        RespValue::BulkString(Some(Bytes::from(format!("{:.17}", lat)))),
                    ]),
                    None => RespValue::null(),
                }
            })
            .collect();

        Ok(RespValue::Array(results))
    }

    /// Handle GEOHASH command
    /// GEOHASH key member [member ...]
    pub(super) fn handle_geohash(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geohash' command",
            ));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_geo_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let geoset_opt = self.cache.get_geo_set(key);

        let results: Vec<RespValue> = args[1..]
            .iter()
            .map(|arg| {
                let member = match arg.as_bulk_string() {
                    Some(m) => m.clone(),
                    None => return RespValue::null(),
                };
                match geoset_opt.as_ref().and_then(|g| {
                    let set = g.read();
                    set.get_position(&member).map(|(lon, lat)| (lon, lat))
                }) {
                    Some((lon, lat)) => {
                        let hash = geohash_encode(lon, lat);
                        let s = geohash_to_string(hash);
                        RespValue::BulkString(Some(Bytes::copy_from_slice(&s)))
                    }
                    None => RespValue::null(),
                }
            })
            .collect();

        Ok(RespValue::Array(results))
    }

    /// Handle GEOSEARCH command
    /// GEOSEARCH key FROMMEMBER member | FROMLONLAT longitude latitude
    ///   BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
    ///   [ASC|DESC] [COUNT count [ANY]] [WITHDIST] [WITHCOORD]
    pub(super) fn handle_geosearch(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 5 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geosearch' command",
            ));
        }

        self.cache.stats.incr(&self.cache.stats.cmd_geosearch);

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_geo_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

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
                let set = geoset.read();
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

        if from_idx >= args.len() {
            return Ok(RespValue::error("ERR syntax error"));
        }

        let shape_clause = match args[from_idx].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };

        // Parse BY clause
        let (mut results, opts_idx, shape_unit) = match shape_clause.as_str() {
            "BYRADIUS" => {
                if from_idx + 2 >= args.len() {
                    return Ok(RespValue::error("ERR syntax error"));
                }
                let radius = match self.parse_float(&args[from_idx + 1]) {
                    Ok(r) => r,
                    Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
                };
                let unit_str = match args[from_idx + 2].as_bulk_string() {
                    Some(s) => String::from_utf8_lossy(s).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                let unit = match DistanceUnit::from_str(&unit_str) {
                    Some(u) => u,
                    None => return Ok(RespValue::error(
                        "ERR unsupported unit provided. please use M, KM, FT, MI",
                    )),
                };
                let geoset = match self.cache.get_geo_set(key) {
                    Some(g) => g,
                    None => return Ok(RespValue::Array(vec![])),
                };
                let set = geoset.read();
                let res = set.search_radius_sorted(center_lon, center_lat, radius, unit, SortOrder::None);
                (res, from_idx + 3, unit)
            }
            "BYBOX" => {
                if from_idx + 3 >= args.len() {
                    return Ok(RespValue::error("ERR syntax error"));
                }
                let width = match self.parse_float(&args[from_idx + 1]) {
                    Ok(w) => w,
                    Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
                };
                let height = match self.parse_float(&args[from_idx + 2]) {
                    Ok(h) => h,
                    Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
                };
                let unit_str = match args[from_idx + 3].as_bulk_string() {
                    Some(s) => String::from_utf8_lossy(s).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                let unit = match DistanceUnit::from_str(&unit_str) {
                    Some(u) => u,
                    None => return Ok(RespValue::error(
                        "ERR unsupported unit provided. please use M, KM, FT, MI",
                    )),
                };
                let width_m = unit.to_meters(width);
                let height_m = unit.to_meters(height);
                let geoset = match self.cache.get_geo_set(key) {
                    Some(g) => g,
                    None => return Ok(RespValue::Array(vec![])),
                };
                let set = geoset.read();
                let res = set.search_box(center_lon, center_lat, width_m, height_m, SortOrder::None);
                (res, from_idx + 4, unit)
            }
            _ => return Ok(RespValue::error("ERR syntax error")),
        };

        // Parse optional flags
        let mut with_dist = false;
        let mut with_coord = false;
        let mut count: Option<usize> = None;
        let mut sort = SortOrder::Asc;
        let mut idx = opts_idx;

        while idx < args.len() {
            let opt = match args[idx].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => break,
            };
            match opt.as_str() {
                "WITHDIST" => { with_dist = true; idx += 1; }
                "WITHCOORD" => { with_coord = true; idx += 1; }
                "ASC" => { sort = SortOrder::Asc; idx += 1; }
                "DESC" => { sort = SortOrder::Desc; idx += 1; }
                "COUNT" => {
                    if idx + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    count = Some(match self.parse_integer(&args[idx + 1]) {
                        Ok(c) if c > 0 => c as usize,
                        _ => return Ok(RespValue::error(
                            "ERR value is not an integer or out of range",
                        )),
                    });
                    idx += 2;
                    // Consume optional ANY flag
                    if let Some(next) = args.get(idx).and_then(|v| v.as_bulk_string()) {
                        if next.eq_ignore_ascii_case(b"ANY") {
                            idx += 1;
                        }
                    }
                }
                _ => break,
            }
        }

        // Apply sort
        match sort {
            SortOrder::Asc | SortOrder::None => {
                results.sort_by(|a, b| {
                    a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
                });
            }
            SortOrder::Desc => {
                results.sort_by(|a, b| {
                    b.distance.partial_cmp(&a.distance).unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }

        // Apply count limit
        if let Some(c) = count {
            results.truncate(c);
        }

        let resp_results: Vec<RespValue> = results
            .iter()
            .map(|r| self.format_geosearch_result(r, with_dist, with_coord, &shape_unit))
            .collect();

        Ok(RespValue::Array(resp_results))
    }

    /// Handle GEOSEARCHSTORE command
    /// GEOSEARCHSTORE destination source FROMMEMBER member | FROMLONLAT longitude latitude
    ///   BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
    ///   [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
    pub(super) fn handle_geosearchstore(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 6 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'geosearchstore' command",
            ));
        }

        let dest_key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid destination key")),
        };

        // Detect STOREDIST flag and build cleaned search args (without STOREDIST)
        let mut storedist = false;
        let mut search_args: Vec<RespValue> = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            if arg.as_bulk_string()
                .map(|s| s.eq_ignore_ascii_case(b"STOREDIST"))
                .unwrap_or(false)
            {
                storedist = true;
                continue;
            }
            search_args.push(arg.clone());
        }

        if storedist {
            // Add WITHDIST so handle_geosearch returns distances in the response
            search_args.push(RespValue::BulkString(Some(Bytes::from_static(b"WITHDIST"))));
            let search_result = self.handle_geosearch(&search_args)?;

            // Store results in a sorted set with distance as score
            let source_key = match args[1].as_bulk_string() {
                Some(k) => k.clone(),
                None => return Ok(RespValue::error("ERR invalid source key")),
            };
            let dest_key_for_store = dest_key.clone();
            // Ensure we don't hold the source geoset lock while writing to dest
            // (source and dest may be the same key)
            let dest_sorted = match self.cache.get_or_create_sorted_set(&dest_key_for_store) {
                Ok(z) => z,
                Err(Error::WrongType) => {
                    return Ok(RespValue::error(Error::WrongType.to_resp_string()));
                }
                Err(e) => return Ok(RespValue::error(e.to_resp_string())),
            };
            let mut zset = dest_sorted.write();
            let _ = source_key; // consumed above

            let mut count = 0i64;
            if let RespValue::Array(arr) = &search_result {
                for item in arr {
                    if let RespValue::Array(sub) = item {
                        // WITHDIST format: [member, dist_string, ...]
                        if let (Some(name_val), Some(dist_val)) =
                            (sub.get(0), sub.get(1))
                        {
                            if let (Some(name), Some(dist_bytes)) = (
                                name_val.as_bulk_string(),
                                dist_val.as_bulk_string(),
                            ) {
                                if let Some(dist) = str::from_utf8(dist_bytes)
                                    .ok()
                                    .and_then(|s| s.parse::<f64>().ok())
                                {
                                    zset.add(name.clone(), dist);
                                    count += 1;
                                }
                            }
                        }
                    }
                }
            }

            return Ok(RespValue::Integer(count));
        }

        // Without STOREDIST: store results as a geospatial set (copy lon/lat from source)
        let search_result = self.handle_geosearch(&search_args)?;

        let source_key = match args[1].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid source key")),
        };

        let source_geoset = match self.cache.get_geo_set(source_key) {
            Some(g) => g,
            None => return Ok(RespValue::Integer(0)),
        };

        // Collect member names from GEOSEARCH result
        let mut member_names: Vec<Bytes> = Vec::new();
        if let RespValue::Array(arr) = &search_result {
            for item in arr {
                match item {
                    RespValue::BulkString(Some(name)) => member_names.push(name.clone()),
                    RespValue::Array(sub) => {
                        if let Some(RespValue::BulkString(Some(name))) = sub.first() {
                            member_names.push(name.clone());
                        }
                    }
                    _ => {}
                }
            }
        }

        let count = member_names.len();
        if count == 0 {
            return Ok(RespValue::Integer(0));
        }

        // Write results to destination geoset
        let dest_geoset = match self.cache.get_or_create_geo_set(&dest_key) {
            Ok(g) => g,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut dest = dest_geoset.write();
        let src = source_geoset.read();

        for name in &member_names {
            if let Some((lon, lat)) = src.get_position(name) {
                dest.add(name.clone(), lon, lat);
            }
        }

        Ok(RespValue::Integer(count as i64))
    }

    /// Handle GEORADIUS command (legacy / deprecated)
    /// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]
    pub(super) fn handle_georadius(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 5 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'georadius' command",
            ));
        }

        // Rewrite as GEOSEARCH key FROMLONLAT lon lat BYRADIUS radius unit [opts...]
        let mut rewritten = vec![
            args[0].clone(),                      // key
            RespValue::BulkString(Some(Bytes::from_static(b"FROMLONLAT"))),
            args[1].clone(),                      // longitude
            args[2].clone(),                      // latitude
            RespValue::BulkString(Some(Bytes::from_static(b"BYRADIUS"))),
            args[3].clone(),                      // radius
            args[4].clone(),                      // unit
        ];
        rewritten.extend_from_slice(&args[5..]);
        self.handle_geosearch(&rewritten)
    }

    /// Handle GEORADIUSBYMEMBER command (legacy / deprecated)
    /// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]
    pub(super) fn handle_georadiusbymember(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'georadiusbymember' command",
            ));
        }

        // Rewrite as GEOSEARCH key FROMMEMBER member BYRADIUS radius unit [opts...]
        let mut rewritten = vec![
            args[0].clone(),                      // key
            RespValue::BulkString(Some(Bytes::from_static(b"FROMMEMBER"))),
            args[1].clone(),                      // member
            RespValue::BulkString(Some(Bytes::from_static(b"BYRADIUS"))),
            args[2].clone(),                      // radius
            args[3].clone(),                      // unit
        ];
        rewritten.extend_from_slice(&args[4..]);
        self.handle_geosearch(&rewritten)
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
            RespValue::BulkString(Some(result.member.clone()))
        } else {
            let mut items = vec![RespValue::BulkString(Some(result.member.clone()))];

            if with_dist {
                let distance = unit.from_meters(result.distance);
                items.push(RespValue::BulkString(Some(Bytes::from(format!("{:.4}", distance)))));
            }

            if with_coord {
                let coords = vec![
                    RespValue::BulkString(Some(Bytes::from(format!("{:.17}", result.longitude)))),
                    RespValue::BulkString(Some(Bytes::from(format!("{:.17}", result.latitude)))),
                ];
                items.push(RespValue::Array(coords));
            }

            RespValue::Array(items)
        }
    }
}
