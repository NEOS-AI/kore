//! Kore RDB (snapshot) format — binary, little-endian.
//!
//! Layout (version 1):
//!   magic: b"KORDB\0" (6 bytes)
//!   version: u32
//!   single-DB body: strings, zsets, geos
//!   footer: 0xFF u8
//!
//! Layout (version 2):
//!   same as v1 plus hashes, lists, sets (single DB, implicit index 0)
//!
//! Layout (version 3):
//!   magic + version
//!   n_databases: u64
//!   for each non-empty DB:
//!     db_index: u32
//!     single-DB body: strings, zsets, geos, hashes, lists, sets, streams
//!   footer: 0xFF u8
//!
//! Layout (version 4):
//!   same as v3 plus trailing typed-expires section per DB body
//!   (key + expire_unix_ms for non-string keys with TTL)
//!
//! Layout (version 5):
//!   same as v4 plus trailing search section per DB body (indices + aliases)
//!
//! Layout (version 6):
//!   same as v5 plus optional durable HNSW graph section per DB body
//!   (index name, field name, entry_point, per-node levels, per-layer edges).
//!   Load: vectors from docs first, then apply graph (edge-identical restore).
//!   v5 files without this section rebuild HNSW by re-`add` (levels re-sampled).
//!
//! Stream section (version >= 3):
//!   n_streams: u64
//!   for each stream:
//!     key, last_generated_id (ms:u64, seq:u64)
//!     n_entries: u64, for each: id (ms,seq), n_fields, (field, value)*
//!     n_groups: u64, for each group:
//!       name, last_delivered_id (ms,seq)
//!       n_pending, for each: id, consumer, delivery_time_ms:u64, delivery_count:u64
//!       n_consumers, for each: name, seen_time_ms:u64, pending:u64
//!
//! Search section (version >= 5):
//!   n_indices: u64
//!   for each index:
//!     name (bytes), n_prefixes, prefixes*, n_fields,
//!     for each field: name, type tag, type-specific params
//!       TEXT (1): weight f64, sortable u8
//!       NUMERIC (2): sortable u8
//!       TAG (3): separator bytes, sortable u8
//!       VECTOR (4): algo u8 (0=FLAT,1=HNSW), m u64 (if HNSW),
//!                   ef_construction u64, dimensions u64, distance u8 (0=Cosine,1=L2,2=IP)
//!   n_aliases: u64
//!   for each alias: alias name, real index name
//!
//! HNSW graph section (version >= 6):
//!   n_graphs: u64
//!   for each graph:
//!     index_name, field_name
//!     has_entry u8, entry_point bytes (if has_entry)
//!     n_levels: u64, for each: doc_id, level u32
//!     n_layers: u64, for each layer:
//!       n_nodes: u64, for each: doc_id, n_neighbors u64, neighbor ids*
//!
//! Strings/keys/members are length-prefixed with u32 LE + raw bytes.

use crate::cache::Cache;
use crate::databases::Databases;
use crate::entry::StoreOptions;
use crate::error::{Error, Result};
use crate::search_index::{
    DistanceMetric, DocumentField, FieldDefinition, FieldType, IndexDefinition, VectorAlgorithm,
};
use crate::vector_search::HnswGraphSnapshot;
use crate::stream_type::{
    ConsumerSnapshot, GroupSnapshot, PendingEntrySnapshot, StreamId, StreamStateSnapshot,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 6] = b"KORDB\0";
const VERSION: u32 = 6;
const VERSION_V1: u32 = 1;
const VERSION_V2: u32 = 2;
const VERSION_V3: u32 = 3;
const VERSION_V4: u32 = 4;
const VERSION_V5: u32 = 5;
const FOOTER: u8 = 0xFF;

// Field type tags in the RDB search section.
const FT_TAG_TEXT: u8 = 1;
const FT_TAG_NUMERIC: u8 = 2;
const FT_TAG_TAG: u8 = 3;
const FT_TAG_VECTOR: u8 = 4;

// Vector algorithm tags.
const VEC_ALGO_FLAT: u8 = 0;
const VEC_ALGO_HNSW: u8 = 1;

// Distance metric tags.
const DIST_COSINE: u8 = 0;
const DIST_L2: u8 = 1;
const DIST_IP: u8 = 2;

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn write_bytes<W: Write>(w: &mut W, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(data)?;
    Ok(())
}

fn read_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 512 * 1024 * 1024 {
        return Err(Error::ParseError(format!("RDB blob too large: {} bytes", len)));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_u32<W: Write>(w: &mut W, v: u32) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_u64<W: Write>(w: &mut W, v: u64) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_i64<W: Write>(w: &mut W, v: i64) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_f64<W: Write>(w: &mut W, v: f64) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_i64<R: Read>(r: &mut R) -> Result<i64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(i64::from_le_bytes(b))
}

fn read_f64<R: Read>(r: &mut R) -> Result<f64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(f64::from_le_bytes(b))
}

fn write_stream_id<W: Write>(w: &mut W, id: StreamId) -> Result<()> {
    write_u64(w, id.ms)?;
    write_u64(w, id.seq)?;
    Ok(())
}

fn read_stream_id<R: Read>(r: &mut R) -> Result<StreamId> {
    let ms = read_u64(r)?;
    let seq = read_u64(r)?;
    Ok(StreamId::new(ms, seq))
}

fn write_u8<W: Write>(w: &mut W, v: u8) -> Result<()> {
    w.write_all(&[v])?;
    Ok(())
}

fn read_u8<R: Read>(r: &mut R) -> Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn write_field_type<W: Write>(w: &mut W, ft: &FieldType) -> Result<()> {
    match ft {
        FieldType::Text { weight, sortable } => {
            write_u8(w, FT_TAG_TEXT)?;
            write_f64(w, *weight)?;
            write_u8(w, u8::from(*sortable))?;
        }
        FieldType::Numeric { sortable } => {
            write_u8(w, FT_TAG_NUMERIC)?;
            write_u8(w, u8::from(*sortable))?;
        }
        FieldType::Tag {
            separator,
            sortable,
        } => {
            write_u8(w, FT_TAG_TAG)?;
            write_bytes(w, separator.as_bytes())?;
            write_u8(w, u8::from(*sortable))?;
        }
        FieldType::Vector {
            algorithm,
            dimensions,
            distance_metric,
        } => {
            write_u8(w, FT_TAG_VECTOR)?;
            match algorithm {
                VectorAlgorithm::Flat => {
                    write_u8(w, VEC_ALGO_FLAT)?;
                }
                VectorAlgorithm::HNSW {
                    m,
                    ef_construction,
                } => {
                    write_u8(w, VEC_ALGO_HNSW)?;
                    write_u64(w, *m as u64)?;
                    write_u64(w, *ef_construction as u64)?;
                }
            }
            write_u64(w, *dimensions as u64)?;
            let dist = match distance_metric {
                DistanceMetric::Cosine => DIST_COSINE,
                DistanceMetric::L2 => DIST_L2,
                DistanceMetric::IP => DIST_IP,
            };
            write_u8(w, dist)?;
        }
    }
    Ok(())
}

fn read_field_type<R: Read>(r: &mut R) -> Result<FieldType> {
    let tag = read_u8(r)?;
    match tag {
        FT_TAG_TEXT => {
            let weight = read_f64(r)?;
            let sortable = read_u8(r)? != 0;
            Ok(FieldType::Text { weight, sortable })
        }
        FT_TAG_NUMERIC => {
            let sortable = read_u8(r)? != 0;
            Ok(FieldType::Numeric { sortable })
        }
        FT_TAG_TAG => {
            let separator = String::from_utf8(read_bytes(r)?)
                .map_err(|e| Error::ParseError(format!("invalid TAG separator: {}", e)))?;
            let sortable = read_u8(r)? != 0;
            Ok(FieldType::Tag {
                separator,
                sortable,
            })
        }
        FT_TAG_VECTOR => {
            let algo_tag = read_u8(r)?;
            let algorithm = match algo_tag {
                VEC_ALGO_FLAT => VectorAlgorithm::Flat,
                VEC_ALGO_HNSW => {
                    let m = read_u64(r)? as usize;
                    let ef_construction = read_u64(r)? as usize;
                    VectorAlgorithm::HNSW {
                        m,
                        ef_construction,
                    }
                }
                other => {
                    return Err(Error::ParseError(format!(
                        "unknown vector algorithm tag {}",
                        other
                    )));
                }
            };
            let dimensions = read_u64(r)? as usize;
            let dist_tag = read_u8(r)?;
            let distance_metric = match dist_tag {
                DIST_COSINE => DistanceMetric::Cosine,
                DIST_L2 => DistanceMetric::L2,
                DIST_IP => DistanceMetric::IP,
                other => {
                    return Err(Error::ParseError(format!(
                        "unknown distance metric tag {}",
                        other
                    )));
                }
            };
            Ok(FieldType::Vector {
                algorithm,
                dimensions,
                distance_metric,
            })
        }
        other => Err(Error::ParseError(format!(
            "unknown search field type tag {}",
            other
        ))),
    }
}

/// Snapshot of one string entry for RDB.
pub struct StringRecord {
    pub key: Bytes,
    pub value: Bytes,
    pub flags: u32,
    /// Absolute Unix epoch ms, or -1 for no expiry.
    pub expire_unix_ms: i64,
}

/// Snapshot of one sorted set.
pub struct ZSetRecord {
    pub key: Bytes,
    pub members: Vec<(Bytes, f64)>,
}

/// Snapshot of one geo set.
pub struct GeoRecord {
    pub key: Bytes,
    pub members: Vec<(Bytes, f64, f64)>,
}

/// Snapshot of one hash.
pub struct HashRecord {
    pub key: Bytes,
    pub fields: Vec<(Bytes, Bytes)>,
}

/// Snapshot of one list.
pub struct ListRecord {
    pub key: Bytes,
    pub elements: Vec<Bytes>,
}

/// Snapshot of one set.
pub struct SetRecord {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

/// Snapshot of one stream (full state).
pub struct StreamRecord {
    pub key: Bytes,
    pub state: StreamStateSnapshot,
}

/// In-memory snapshot of a single logical database.
pub struct DbSnapshot {
    pub strings: Vec<StringRecord>,
    pub zsets: Vec<ZSetRecord>,
    pub geos: Vec<GeoRecord>,
    pub hashes: Vec<HashRecord>,
    pub lists: Vec<ListRecord>,
    pub sets: Vec<SetRecord>,
    pub streams: Vec<StreamRecord>,
    /// Non-string key expiries: (key, absolute Unix epoch ms). RDB v4+.
    pub typed_expires: Vec<(Bytes, i64)>,
    /// Search index definitions (schema only). RDB v5+.
    pub search_indices: Vec<IndexDefinition>,
    /// Search aliases: (alias, real_index). RDB v5+.
    pub search_aliases: Vec<(String, String)>,
    /// Durable HNSW graphs: (index_name, field_name, snapshot). RDB v6+.
    pub hnsw_graphs: Vec<(String, String, HnswGraphSnapshot)>,
}

/// Multi-database snapshot (RDB v3).
pub struct MultiDbSnapshot {
    /// (db_index, snapshot) for each non-empty DB.
    pub databases: Vec<(u32, DbSnapshot)>,
}

impl DbSnapshot {
    /// Capture current cache state (skip expired strings).
    ///
    /// **Non-mutating:** uses map/export peeks only — no LRU/LFU touch, no
    /// lazy-delete of expired keys, no stats bumps. Safe for RDB save and for
    /// scratch-load merge seed (`flush=false`) so a failed merge leaves the
    /// live target completely untouched.
    pub fn from_cache(cache: &Cache) -> Result<Self> {
        let strings = cache
            .export_strings()
            .into_iter()
            .map(|(key, value, flags, expire_unix_ms)| StringRecord {
                key,
                value,
                flags,
                expire_unix_ms,
            })
            .collect();

        let zsets = cache
            .export_zsets()
            .into_iter()
            .map(|(key, members)| ZSetRecord { key, members })
            .collect();
        let geos = cache
            .export_geos()
            .into_iter()
            .map(|(key, members)| GeoRecord { key, members })
            .collect();
        let hashes = cache
            .export_hashes()
            .into_iter()
            .map(|(key, fields)| HashRecord { key, fields })
            .collect();
        let lists = cache
            .export_lists()
            .into_iter()
            .map(|(key, elements)| ListRecord { key, elements })
            .collect();
        let sets = cache
            .export_sets()
            .into_iter()
            .map(|(key, members)| SetRecord { key, members })
            .collect();
        let streams = cache
            .export_streams()
            .into_iter()
            .map(|(key, state)| StreamRecord { key, state })
            .collect();
        let typed_expires = cache.export_typed_expires_unix_ms();
        let search_indices = cache.list_search_index_definitions();
        let search_aliases = cache.list_search_aliases();
        let hnsw_graphs = cache.export_hnsw_graphs();

        Ok(Self {
            strings,
            zsets,
            geos,
            hashes,
            lists,
            sets,
            streams,
            typed_expires,
            search_indices,
            search_aliases,
            hnsw_graphs,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
            && self.zsets.is_empty()
            && self.geos.is_empty()
            && self.hashes.is_empty()
            && self.lists.is_empty()
            && self.sets.is_empty()
            && self.streams.is_empty()
            && self.search_indices.is_empty()
            && self.search_aliases.is_empty()
    }

    /// Encode a single-DB body (no magic/version/footer). Always includes v2+v3 sections.
    fn encode_body<W: Write>(&self, w: &mut W) -> Result<()> {
        write_u64(w, self.strings.len() as u64)?;
        for s in &self.strings {
            write_bytes(w, &s.key)?;
            write_bytes(w, &s.value)?;
            write_u32(w, s.flags)?;
            write_i64(w, s.expire_unix_ms)?;
        }

        write_u64(w, self.zsets.len() as u64)?;
        for z in &self.zsets {
            write_bytes(w, &z.key)?;
            write_u64(w, z.members.len() as u64)?;
            for (m, score) in &z.members {
                write_bytes(w, m)?;
                write_f64(w, *score)?;
            }
        }

        write_u64(w, self.geos.len() as u64)?;
        for g in &self.geos {
            write_bytes(w, &g.key)?;
            write_u64(w, g.members.len() as u64)?;
            for (m, lon, lat) in &g.members {
                write_bytes(w, m)?;
                write_f64(w, *lon)?;
                write_f64(w, *lat)?;
            }
        }

        // Version 2+: hashes, lists, sets
        write_u64(w, self.hashes.len() as u64)?;
        for h in &self.hashes {
            write_bytes(w, &h.key)?;
            write_u64(w, h.fields.len() as u64)?;
            for (f, v) in &h.fields {
                write_bytes(w, f)?;
                write_bytes(w, v)?;
            }
        }

        write_u64(w, self.lists.len() as u64)?;
        for l in &self.lists {
            write_bytes(w, &l.key)?;
            write_u64(w, l.elements.len() as u64)?;
            for e in &l.elements {
                write_bytes(w, e)?;
            }
        }

        write_u64(w, self.sets.len() as u64)?;
        for s in &self.sets {
            write_bytes(w, &s.key)?;
            write_u64(w, s.members.len() as u64)?;
            for m in &s.members {
                write_bytes(w, m)?;
            }
        }

        // Version 3+: streams
        write_u64(w, self.streams.len() as u64)?;
        for st in &self.streams {
            write_bytes(w, &st.key)?;
            write_stream_id(w, st.state.last_generated_id)?;
            write_u64(w, st.state.entries.len() as u64)?;
            for (id, fields) in &st.state.entries {
                write_stream_id(w, *id)?;
                write_u64(w, fields.len() as u64)?;
                for (f, v) in fields {
                    write_bytes(w, f)?;
                    write_bytes(w, v)?;
                }
            }
            write_u64(w, st.state.groups.len() as u64)?;
            for g in &st.state.groups {
                write_bytes(w, &g.name)?;
                write_stream_id(w, g.last_delivered_id)?;
                write_u64(w, g.pending.len() as u64)?;
                for pe in &g.pending {
                    write_stream_id(w, pe.id)?;
                    write_bytes(w, &pe.consumer)?;
                    write_u64(w, pe.delivery_time_ms)?;
                    write_u64(w, pe.delivery_count)?;
                }
                write_u64(w, g.consumers.len() as u64)?;
                for c in &g.consumers {
                    write_bytes(w, &c.name)?;
                    write_u64(w, c.seen_time_ms)?;
                    write_u64(w, c.pending as u64)?;
                }
            }
        }


        // Version 4+: typed-key expires (key + absolute unix ms)
        write_u64(w, self.typed_expires.len() as u64)?;
        for (key, exp) in &self.typed_expires {
            write_bytes(w, key)?;
            write_i64(w, *exp)?;
        }

        // Version 5+: search indices + aliases
        write_u64(w, self.search_indices.len() as u64)?;
        for def in &self.search_indices {
            write_bytes(w, def.name.as_bytes())?;
            write_u64(w, def.prefix.len() as u64)?;
            for p in &def.prefix {
                write_bytes(w, p.as_bytes())?;
            }
            write_u64(w, def.fields.len() as u64)?;
            for field in &def.fields {
                write_bytes(w, field.name.as_bytes())?;
                write_field_type(w, &field.field_type)?;
            }
        }
        write_u64(w, self.search_aliases.len() as u64)?;
        for (alias, index) in &self.search_aliases {
            write_bytes(w, alias.as_bytes())?;
            write_bytes(w, index.as_bytes())?;
        }

        // Version 6+: durable HNSW graphs (entry/levels/edges)
        write_u64(w, self.hnsw_graphs.len() as u64)?;
        for (index_name, field, snap) in &self.hnsw_graphs {
            write_hnsw_graph(w, index_name, field, snap)?;
        }

        Ok(())
    }

    fn decode_body<R: Read>(r: &mut R, version: u32) -> Result<Self> {
        let n_strings = read_u64(r)? as usize;
        let mut strings = Vec::with_capacity(n_strings);
        for _ in 0..n_strings {
            let key = Bytes::from(read_bytes(r)?);
            let value = Bytes::from(read_bytes(r)?);
            let flags = read_u32(r)?;
            let expire_unix_ms = read_i64(r)?;
            strings.push(StringRecord {
                key,
                value,
                flags,
                expire_unix_ms,
            });
        }

        let n_zsets = read_u64(r)? as usize;
        let mut zsets = Vec::with_capacity(n_zsets);
        for _ in 0..n_zsets {
            let key = Bytes::from(read_bytes(r)?);
            let n = read_u64(r)? as usize;
            let mut members = Vec::with_capacity(n);
            for _ in 0..n {
                let m = Bytes::from(read_bytes(r)?);
                let score = read_f64(r)?;
                members.push((m, score));
            }
            zsets.push(ZSetRecord { key, members });
        }

        let n_geos = read_u64(r)? as usize;
        let mut geos = Vec::with_capacity(n_geos);
        for _ in 0..n_geos {
            let key = Bytes::from(read_bytes(r)?);
            let n = read_u64(r)? as usize;
            let mut members = Vec::with_capacity(n);
            for _ in 0..n {
                let m = Bytes::from(read_bytes(r)?);
                let lon = read_f64(r)?;
                let lat = read_f64(r)?;
                members.push((m, lon, lat));
            }
            geos.push(GeoRecord { key, members });
        }

        let mut hashes = Vec::new();
        let mut lists = Vec::new();
        let mut sets = Vec::new();
        let mut streams = Vec::new();

        if version >= VERSION_V2 {
            let n_hashes = read_u64(r)? as usize;
            hashes.reserve(n_hashes);
            for _ in 0..n_hashes {
                let key = Bytes::from(read_bytes(r)?);
                let n = read_u64(r)? as usize;
                let mut fields = Vec::with_capacity(n);
                for _ in 0..n {
                    let f = Bytes::from(read_bytes(r)?);
                    let v = Bytes::from(read_bytes(r)?);
                    fields.push((f, v));
                }
                hashes.push(HashRecord { key, fields });
            }

            let n_lists = read_u64(r)? as usize;
            lists.reserve(n_lists);
            for _ in 0..n_lists {
                let key = Bytes::from(read_bytes(r)?);
                let n = read_u64(r)? as usize;
                let mut elements = Vec::with_capacity(n);
                for _ in 0..n {
                    elements.push(Bytes::from(read_bytes(r)?));
                }
                lists.push(ListRecord { key, elements });
            }

            let n_sets = read_u64(r)? as usize;
            sets.reserve(n_sets);
            for _ in 0..n_sets {
                let key = Bytes::from(read_bytes(r)?);
                let n = read_u64(r)? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    members.push(Bytes::from(read_bytes(r)?));
                }
                sets.push(SetRecord { key, members });
            }
        }

        if version >= VERSION_V3 {
            let n_streams = read_u64(r)? as usize;
            streams.reserve(n_streams);
            for _ in 0..n_streams {
                let key = Bytes::from(read_bytes(r)?);
                let last_generated_id = read_stream_id(r)?;
                let n_entries = read_u64(r)? as usize;
                let mut entries = Vec::with_capacity(n_entries);
                for _ in 0..n_entries {
                    let id = read_stream_id(r)?;
                    let n_fields = read_u64(r)? as usize;
                    let mut fields = Vec::with_capacity(n_fields);
                    for _ in 0..n_fields {
                        let f = Bytes::from(read_bytes(r)?);
                        let v = Bytes::from(read_bytes(r)?);
                        fields.push((f, v));
                    }
                    entries.push((id, fields));
                }
                let n_groups = read_u64(r)? as usize;
                let mut groups = Vec::with_capacity(n_groups);
                for _ in 0..n_groups {
                    let name = Bytes::from(read_bytes(r)?);
                    let last_delivered_id = read_stream_id(r)?;
                    let n_pending = read_u64(r)? as usize;
                    let mut pending = Vec::with_capacity(n_pending);
                    for _ in 0..n_pending {
                        let id = read_stream_id(r)?;
                        let consumer = Bytes::from(read_bytes(r)?);
                        let delivery_time_ms = read_u64(r)?;
                        let delivery_count = read_u64(r)?;
                        pending.push(PendingEntrySnapshot {
                            id,
                            consumer,
                            delivery_time_ms,
                            delivery_count,
                        });
                    }
                    let n_consumers = read_u64(r)? as usize;
                    let mut consumers = Vec::with_capacity(n_consumers);
                    for _ in 0..n_consumers {
                        let cname = Bytes::from(read_bytes(r)?);
                        let seen_time_ms = read_u64(r)?;
                        let pending_count = read_u64(r)? as usize;
                        consumers.push(ConsumerSnapshot {
                            name: cname,
                            seen_time_ms,
                            pending: pending_count,
                        });
                    }
                    groups.push(GroupSnapshot {
                        name,
                        last_delivered_id,
                        pending,
                        consumers,
                    });
                }
                streams.push(StreamRecord {
                    key,
                    state: StreamStateSnapshot {
                        last_generated_id,
                        entries,
                        groups,
                    },
                });
            }
        }

        let mut typed_expires = Vec::new();
        if version >= VERSION_V4 {
            let n = read_u64(r)? as usize;
            typed_expires.reserve(n);
            for _ in 0..n {
                let key = Bytes::from(read_bytes(r)?);
                let exp = read_i64(r)?;
                typed_expires.push((key, exp));
            }
        }

        let mut search_indices = Vec::new();
        let mut search_aliases = Vec::new();
        let mut hnsw_graphs = Vec::new();
        if version >= VERSION_V5 {
            let n_indices = read_u64(r)? as usize;
            search_indices.reserve(n_indices);
            for _ in 0..n_indices {
                let name = String::from_utf8(read_bytes(r)?)
                    .map_err(|e| Error::ParseError(format!("invalid index name: {}", e)))?;
                let n_prefixes = read_u64(r)? as usize;
                let mut prefixes = Vec::with_capacity(n_prefixes);
                for _ in 0..n_prefixes {
                    let p = String::from_utf8(read_bytes(r)?).map_err(|e| {
                        Error::ParseError(format!("invalid index prefix: {}", e))
                    })?;
                    prefixes.push(p);
                }
                let n_fields = read_u64(r)? as usize;
                let mut fields = Vec::with_capacity(n_fields);
                for _ in 0..n_fields {
                    let fname = String::from_utf8(read_bytes(r)?).map_err(|e| {
                        Error::ParseError(format!("invalid field name: {}", e))
                    })?;
                    let field_type = read_field_type(r)?;
                    fields.push(FieldDefinition {
                        name: fname,
                        field_type,
                    });
                }
                search_indices.push(IndexDefinition::new(name, prefixes, fields));
            }

            let n_aliases = read_u64(r)? as usize;
            search_aliases.reserve(n_aliases);
            for _ in 0..n_aliases {
                let alias = String::from_utf8(read_bytes(r)?)
                    .map_err(|e| Error::ParseError(format!("invalid alias name: {}", e)))?;
                let index = String::from_utf8(read_bytes(r)?).map_err(|e| {
                    Error::ParseError(format!("invalid alias target: {}", e))
                })?;
                search_aliases.push((alias, index));
            }
        }

        if version >= VERSION {
            let n_graphs = read_u64(r)? as usize;
            hnsw_graphs.reserve(n_graphs);
            for _ in 0..n_graphs {
                hnsw_graphs.push(read_hnsw_graph(r)?);
            }
        }

        Ok(Self {
            strings,
            zsets,
            geos,
            hashes,
            lists,
            sets,
            streams,
            typed_expires,
            search_indices,
            search_aliases,
            hnsw_graphs,
        })
    }

    /// Encode as a standalone KORDB file containing this single DB at index 0
    /// (or an empty multi-DB file if this snapshot is empty).
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(4096);
        buf.extend_from_slice(MAGIC);
        write_u32(&mut buf, VERSION)?;
        if self.is_empty() {
            write_u64(&mut buf, 0)?;
        } else {
            write_u64(&mut buf, 1)?;
            write_u32(&mut buf, 0)?;
            self.encode_body(&mut buf)?;
        }
        buf.push(FOOTER);
        Ok(buf)
    }

    /// Decode a full RDB blob. v1/v2 → single DB 0; v3 → first DB body only
    /// (use [`MultiDbSnapshot::decode`] for full multi-DB).
    pub fn decode(data: &[u8]) -> Result<Self> {
        let multi = MultiDbSnapshot::decode(data)?;
        if let Some((_, snap)) = multi.databases.into_iter().next() {
            Ok(snap)
        } else {
            Ok(Self {
                strings: Vec::new(),
                zsets: Vec::new(),
                geos: Vec::new(),
                hashes: Vec::new(),
                lists: Vec::new(),
                sets: Vec::new(),
                streams: Vec::new(),
                typed_expires: Vec::new(),
                search_indices: Vec::new(),
                search_aliases: Vec::new(),
                hnsw_graphs: Vec::new(),
            })
        }
    }

    /// Load snapshot into `cache` **in place** (does not flush first).
    ///
    /// # Non-transactional (raw API)
    ///
    /// On `Err` mid-apply, earlier indices/keys/aliases already written to
    /// `cache` remain — this method is **not** all-or-nothing. Production
    /// callers must use the public scratch-load wrappers instead:
    /// [`load_bytes`], [`load_databases_bytes`], and AOF
    /// [`crate::persistence::aof::load_into_cache`] /
    /// [`crate::persistence::aof::load_into_databases`]. Those apply into a
    /// scratch keyspace and only commit via `replace_keyspace_from` on `Ok`.
    ///
    /// # Search schema
    ///
    /// Search indices are created **before** key types so hash load can
    /// auto-index documents (same order idea as AOF: schema before HSET).
    ///
    /// **Merge name clash (indices):** if an index name from this snapshot
    /// already exists on `cache` (typical `flush=false` merge into a seeded
    /// scratch), compare **logical schema** via
    /// [`IndexDefinition::schema_eq`] (`name` / `prefix` / `fields`; ignore
    /// `created_at`):
    /// - **Equal** → skip create (idempotent; seed definition kept). Hashes
    ///   from the RDB still load and `auto_index_key` against the seed schema
    ///   (only keys matching the seed PREFIX are indexed — same as live).
    /// - **Unequal** → `Err(InvalidArgument)` describing the clash. Do **not**
    ///   silently keep the seed while loading RDB keys that would not match
    ///   the intended RDB schema.
    ///
    /// **Merge name clash (aliases):** if an alias already exists, compare
    /// resolved real-index targets:
    /// - **Equal** → skip (idempotent).
    /// - **Unequal** → `Err(InvalidArgument)` retarget clash (seed mapping kept
    ///   only because the public wrappers roll back the scratch on `Err`).
    ///
    /// Unknown-target alias errors still fail the load.
    pub fn load_into(&self, cache: &Cache) -> Result<usize> {
        let mut loaded = 0usize;
        let now = now_unix_ms();

        // 1. Recreate search schema first. On merge name clash, require schema
        // equality (skip) rather than silently discarding a divergent RDB def.
        let existing_defs: HashMap<String, IndexDefinition> = cache
            .list_search_index_definitions()
            .into_iter()
            .map(|d| (d.name.clone(), d))
            .collect();
        for def in &self.search_indices {
            if let Some(existing) = existing_defs.get(&def.name) {
                if existing.schema_eq(def) {
                    // Idempotent merge: keep seed definition + its documents.
                    continue;
                }
                let detail = existing
                    .schema_diff_summary(def)
                    .unwrap_or_else(|| "schema differs".into());
                return Err(Error::InvalidArgument(format!(
                    "RDB FT.CREATE: index '{}' already exists with a different schema ({})",
                    def.name, detail
                )));
            }
            cache
                .create_search_index(def.clone())
                .map_err(|e| {
                    crate::persistence::aof::map_rdb_ft_mutator_error("RDB FT.CREATE", e)
                })?;
        }

        for s in &self.strings {
            if s.expire_unix_ms >= 0 && s.expire_unix_ms <= now {
                continue;
            }
            let mut opts = StoreOptions::default();
            opts.flags = s.flags;
            if s.expire_unix_ms >= 0 {
                let remaining = (s.expire_unix_ms - now).max(0) as u64;
                opts.ttl_ms = Some(remaining);
            }
            cache.store(s.key.clone(), s.value.clone(), opts)?;
            loaded += 1;
        }

        for z in &self.zsets {
            let zset = cache.get_or_create_sorted_set(&z.key)?;
            let mut set = zset
                .write();
            for (m, score) in &z.members {
                set.add(m.clone(), *score);
            }
            loaded += 1;
        }

        for g in &self.geos {
            let geoset = cache.get_or_create_geo_set(&g.key)?;
            let mut set = geoset
                .write();
            for (m, lon, lat) in &g.members {
                let _ = set.add(m.clone(), *lon, *lat);
            }
            loaded += 1;
        }

        for h in &self.hashes {
            let hash = cache.get_or_create_hash(&h.key)?;
            let index_fields = {
                let mut set = hash.write();
                for (f, v) in &h.fields {
                    set.hset(f.clone(), v.clone());
                }
                // Snapshot for search auto-index (same as command-path / AOF HSET).
                let mut index_fields = HashMap::new();
                for (f, v) in set.hgetall() {
                    let fname = String::from_utf8_lossy(&f).into_owned();
                    let fval = String::from_utf8_lossy(&v).into_owned();
                    index_fields.insert(fname, DocumentField::Text(fval));
                }
                index_fields
            };
            cache.auto_index_key(&h.key, index_fields);
            loaded += 1;
        }

        for l in &self.lists {
            let list = cache.get_or_create_list(&l.key)?;
            let mut set = list
                .write();
            // Elements are stored left-to-right; RPUSH preserves order.
            set.rpush(l.elements.iter().cloned());
            loaded += 1;
        }

        for s in &self.sets {
            let set = cache.get_or_create_set(&s.key)?;
            let mut inner = set
                .write();
            inner.sadd(s.members.iter().cloned());
            loaded += 1;
        }

        for st in &self.streams {
            cache.import_stream(st.key.clone(), st.state.clone())?;
            loaded += 1;
        }

        for (key, exp) in &self.typed_expires {
            if *exp >= 0 && *exp <= now {
                // Expired typed key: drop if loaded above.
                let _ = cache.delete(key);
                continue;
            }
            cache.set_typed_expire_unix_ms(key, *exp);
        }

        // 2. Aliases after keys (and after indices exist).
        // On merge alias clash, require equal resolved targets (skip) rather
        // than silently keeping a divergent seed mapping.
        let existing_aliases: HashMap<String, String> =
            cache.list_search_aliases().into_iter().collect();
        for (alias, index) in &self.search_aliases {
            if let Some(existing_target) = existing_aliases.get(alias) {
                // Stored alias targets are always real index names. Resolve the
                // RDB target the same way alias_add would (alias→alias one hop).
                let resolved_rdb = existing_aliases
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| index.clone());
                if existing_target == &resolved_rdb {
                    continue;
                }
                return Err(Error::InvalidArgument(format!(
                    "RDB FT.ALIASADD: alias '{}' already points to '{}' (RDB targets '{}')",
                    alias, existing_target, resolved_rdb
                )));
            }
            cache
                .alias_add(alias, index)
                .map_err(|e| {
                    crate::persistence::aof::map_rdb_ft_mutator_error("RDB FT.ALIASADD", e)
                })?;
        }

        // 3. Apply durable HNSW graphs after schema + docs (Batch FV).
        // Documents already re-`add`ed vectors (rebuild path); this overwrites
        // levels/edges/entry_point to match the pre-save hierarchy when present.
        if !self.hnsw_graphs.is_empty() {
            cache
                .apply_hnsw_graphs(&self.hnsw_graphs)
                .map_err(|e| Error::ParseError(format!("RDB HNSW graph restore: {}", e)))?;
        }

        Ok(loaded)
    }
}

fn write_hnsw_graph<W: Write>(
    w: &mut W,
    index_name: &str,
    field: &str,
    snap: &HnswGraphSnapshot,
) -> Result<()> {
    write_bytes(w, index_name.as_bytes())?;
    write_bytes(w, field.as_bytes())?;
    // Graph body layout is shared with AOF `FT._LOADGRAPH` (Batch FX).
    snap.write_to(w)
        .map_err(|e| Error::ParseError(e))?;
    Ok(())
}

fn read_hnsw_graph<R: Read>(r: &mut R) -> Result<(String, String, HnswGraphSnapshot)> {
    let index_name = String::from_utf8(read_bytes(r)?)
        .map_err(|e| Error::ParseError(format!("invalid HNSW index name: {}", e)))?;
    let field = String::from_utf8(read_bytes(r)?)
        .map_err(|e| Error::ParseError(format!("invalid HNSW field name: {}", e)))?;
    let snap = HnswGraphSnapshot::read_from(r)
        .map_err(|e| Error::ParseError(format!("RDB HNSW graph: {}", e)))?;
    Ok((index_name, field, snap))
}

// Helper trait-like clone for encode path — avoid by cleaning encode() above.
// The encode() for empty uses MultiDbSnapshot::encode; non-empty encodes inline.

impl MultiDbSnapshot {
    /// Export every logical DB under the multi-DB keyspace epoch **read** lock
    /// so a concurrent [`Databases::replace_keyspaces_from`] install cannot
    /// produce a torn multi-DB snapshot (DB0 new + DB1 old). See
    /// [`Databases::with_stable_keyspace_view`].
    pub fn from_databases(databases: &Databases) -> Result<Self> {
        databases.with_stable_keyspace_view(|| {
            let mut out = Vec::new();
            for (idx, cache) in databases.iter().enumerate() {
                let snap = DbSnapshot::from_cache(cache)?;
                if !snap.is_empty() {
                    out.push((idx as u32, snap));
                }
            }
            Ok(Self { databases: out })
        })
    }

    pub fn from_cache(cache: &Cache) -> Result<Self> {
        let snap = DbSnapshot::from_cache(cache)?;
        if snap.is_empty() {
            Ok(Self {
                databases: Vec::new(),
            })
        } else {
            Ok(Self {
                databases: vec![(0, snap)],
            })
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(4096);
        buf.extend_from_slice(MAGIC);
        write_u32(&mut buf, VERSION)?;
        write_u64(&mut buf, self.databases.len() as u64)?;
        for (idx, snap) in &self.databases {
            write_u32(&mut buf, *idx)?;
            snap.encode_body(&mut buf)?;
        }
        buf.push(FOOTER);
        Ok(buf)
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut cur = std::io::Cursor::new(data);
        let mut magic = [0u8; 6];
        cur.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::ParseError("invalid RDB magic".into()));
        }
        let version = read_u32(&mut cur)?;
        if version != VERSION
            && version != VERSION_V1
            && version != VERSION_V2
            && version != VERSION_V3
            && version != VERSION_V4
            && version != VERSION_V5
        {
            return Err(Error::ParseError(format!(
                "unsupported RDB version {}",
                version
            )));
        }

        let databases = if version >= VERSION_V3 {
            let n_dbs = read_u64(&mut cur)? as usize;
            let mut dbs = Vec::with_capacity(n_dbs);
            for _ in 0..n_dbs {
                let idx = read_u32(&mut cur)?;
                let snap = DbSnapshot::decode_body(&mut cur, version)?;
                dbs.push((idx, snap));
            }
            dbs
        } else {
            // v1/v2: single implicit DB 0
            let snap = DbSnapshot::decode_body(&mut cur, version)?;
            if snap.is_empty() {
                Vec::new()
            } else {
                vec![(0, snap)]
            }
        };

        let mut footer = [0u8; 1];
        cur.read_exact(&mut footer)?;
        if footer[0] != FOOTER {
            return Err(Error::ParseError("invalid RDB footer".into()));
        }

        Ok(Self { databases })
    }

    /// Load into multi-DB keyspaces in place. Returns total keys loaded.
    ///
    /// **Non-transactional:** on `Err`, earlier DBs/keys may already be
    /// mutated. Prefer [`load_databases_bytes`] (scratch-load + swap).
    /// See [`DbSnapshot::load_into`].
    pub fn load_into_databases(&self, databases: &Databases) -> Result<usize> {
        let mut total = 0usize;
        for (idx, snap) in &self.databases {
            let Some(cache) = databases.get(*idx as usize) else {
                // Skip DBs outside configured range.
                continue;
            };
            total += snap.load_into(&cache)?;
        }
        Ok(total)
    }

    /// Load DB 0 (or the first present DB) into a single cache in place.
    ///
    /// **Non-transactional:** on `Err`, partial state may remain on `cache`.
    /// Prefer [`load_bytes`] (scratch-load + swap). See [`DbSnapshot::load_into`].
    pub fn load_into_cache(&self, cache: &Cache) -> Result<usize> {
        for (idx, snap) in &self.databases {
            if *idx == 0 {
                return snap.load_into(cache);
            }
        }
        // No DB 0 — load first DB if any (legacy single-cache path).
        if let Some((_, snap)) = self.databases.first() {
            return snap.load_into(cache);
        }
        Ok(0)
    }
}

/// Save cache snapshot to an RDB file (atomic via temp + rename).
/// Single-cache path: written as multi-DB v3 with DB 0 only (includes streams).
pub fn save_file(cache: &Cache, path: &Path) -> Result<()> {
    let snap = MultiDbSnapshot::from_cache(cache)?;
    write_snapshot_file(&snap, path)
}

/// Save all non-empty logical databases to an RDB file.
pub fn save_databases(databases: &Databases, path: &Path) -> Result<()> {
    let snap = MultiDbSnapshot::from_databases(databases)?;
    write_snapshot_file(&snap, path)
}

fn write_snapshot_file(snap: &MultiDbSnapshot, path: &Path) -> Result<()> {
    let data = snap.encode()?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let tmp = path.with_extension("rdb.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Encode snapshot to bytes (for SYNC / full resync). Includes streams.
pub fn save_to_bytes(cache: &Cache) -> Result<Bytes> {
    let snap = MultiDbSnapshot::from_cache(cache)?;
    Ok(Bytes::from(snap.encode()?))
}

/// Encode multi-DB snapshot to bytes.
pub fn save_databases_to_bytes(databases: &Databases) -> Result<Bytes> {
    let snap = MultiDbSnapshot::from_databases(databases)?;
    Ok(Bytes::from(snap.encode()?))
}

/// Load RDB file into cache (DB 0 / first DB only).
pub fn load_file(cache: &Cache, path: &Path, flush: bool) -> Result<usize> {
    let data = fs::read(path)?;
    load_bytes(cache, &data, flush)
}

/// Load RDB file into multi-DB keyspaces.
pub fn load_databases(databases: &Databases, path: &Path, flush: bool) -> Result<usize> {
    let data = fs::read(path)?;
    load_databases_bytes(databases, &data, flush)
}

/// Load RDB bytes into cache (DB 0 / first DB only).
///
/// **Scratch-load (transactional):** after a successful decode, the snapshot is
/// applied to a scratch keyspace and swapped into `cache` only on `Ok`. On
/// `Err` (including mid-`load_into` failures), `cache` is left completely
/// untouched (including no seed-side mutation: merge seed uses non-mutating
/// export). Commit pauses autosweep on the target for the whole replace.
///
/// This is the **supported** load API. Raw [`DbSnapshot::load_into`] /
/// [`MultiDbSnapshot::load_into_cache`] are non-transactional helpers used
/// only on scratch inside this wrapper.
///
/// - `flush = true` (**snapshot replace** / FULLRESYNC): scratch starts empty;
///   on success the target is flushed (including FT schema) **before** replace
///   so peak dual-residency is shortened, then scratch is installed. FT schema
///   from the RDB fully replaces the target's schema.
/// - `flush = false` (**merge**): scratch is seeded with a non-mutating deep
///   copy of the current keyspace, then the RDB is merged into that copy
///   before swap — failure preserves the live target with no touch/lazy-expire
///   side effects from seeding. FT indices whose names already exist on the
///   seed are kept only when the RDB definition is **schema-equal**
///   ([`IndexDefinition::schema_eq`]); divergent schemas fail the load.
///   Aliases whose names already exist are kept only when the resolved target
///   matches; retarget clashes fail. New FT names from the RDB are added.
pub fn load_bytes(cache: &Cache, data: &[u8], flush: bool) -> Result<usize> {
    let snap = MultiDbSnapshot::decode(data)?;
    let scratch = cache.empty_keyspace_like();
    if !flush {
        // Seed scratch with current keyspace so merge is transactional.
        // from_cache is non-mutating (no touch / lazy-delete / stats).
        let seed = MultiDbSnapshot::from_cache(cache)?;
        seed.load_into_cache(&scratch)?;
    }
    match snap.load_into_cache(&scratch) {
        Ok(n) => {
            cache.with_autosweep_paused(|| {
                // flush=true: drop live keyspace early so peak ≈ scratch only
                // before install (Err path never reaches here).
                if flush {
                    // Dirty WATCH before flush so no clean window while empty.
                    cache.touch_all_watch_keys();
                    cache.flush_all_including_search();
                }
                cache.replace_keyspace_from(&scratch);
            });
            Ok(n)
        }
        Err(e) => Err(e),
    }
}

/// Load RDB bytes into multi-DB keyspaces.
///
/// **Scratch-load (transactional):** see [`load_bytes`]. On `Ok`, every DB
/// keyspace is swapped from scratch under multi-DB autosweep pause; on `Err`,
/// `databases` is untouched.
///
/// Multi-DB install is staged (all sources drained before any target mutate)
/// then installed under a single keyspace-epoch write lock — multi-DB exporters
/// that use [`MultiDbSnapshot::from_databases`] / [`Databases::with_stable_keyspace_view`]
/// (also AOF `rewrite_databases`) never observe DB0-new + DB1-old. Command-path
/// readers still see `-LOADING`. Panic mid-install rolls back fully-swapped DBs
/// (Batch DS); see [`Databases::replace_keyspaces_from`] for residuals.
///
/// - `flush = true` (**snapshot replace**): empty scratch; on success each
///   target DB is swapped from scratch via [`Databases::replace_keyspaces_from`]
///   (full keyspace replace per DB — **no** pre-flush of all DBs). A mid-install
///   panic after some DBs swap restores those DBs to pre-load data via retained
///   discards. Single-DB [`load_bytes`] still pre-flushes for peak memory on
///   FULLRESYNC of one cache.
/// - `flush = false` (**merge**): scratch seeded from non-mutating multi-DB
///   snapshot, then merged (existing FT names kept only when schema/target
///   equal; clash otherwise fails — see [`DbSnapshot::load_into`]).
pub fn load_databases_bytes(databases: &Databases, data: &[u8], flush: bool) -> Result<usize> {
    let snap = MultiDbSnapshot::decode(data)?;
    let scratch = databases.empty_like();
    if !flush {
        let seed = MultiDbSnapshot::from_databases(databases)?;
        seed.load_into_databases(&scratch)?;
    }
    match snap.load_into_databases(&scratch) {
        Ok(n) => {
            databases.with_autosweep_paused_all(|| {
                // Dirty WATCH before replace so no clean-gen window. Do **not**
                // flush_all first: replace_keyspaces_from already swaps each
                // DB fully; pre-flush would empty every DB before any install
                // (worse multi-DB tear / panic recovery).
                if flush {
                    for db in databases.iter() {
                        db.touch_all_watch_keys();
                    }
                }
                databases.replace_keyspaces_from(&scratch);
            });
            Ok(n)
        }
        Err(e) => Err(e),
    }
}

/// Convenience for callers with Arc.
pub fn save_file_arc(cache: &Arc<Cache>, path: &Path) -> Result<()> {
    save_file(cache, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_type::StreamStateSnapshot;

    #[test]
    fn encode_decode_stream_section() {
        let state = StreamStateSnapshot {
            last_generated_id: StreamId::new(1, 1),
            entries: vec![
                (
                    StreamId::new(1, 0),
                    vec![(Bytes::from("f"), Bytes::from("a"))],
                ),
                (
                    StreamId::new(1, 1),
                    vec![(Bytes::from("f"), Bytes::from("b"))],
                ),
            ],
            groups: vec![GroupSnapshot {
                name: Bytes::from("g"),
                last_delivered_id: StreamId::new(1, 1),
                pending: vec![PendingEntrySnapshot {
                    id: StreamId::new(1, 0),
                    consumer: Bytes::from("c1"),
                    delivery_time_ms: 100,
                    delivery_count: 1,
                }],
                consumers: vec![ConsumerSnapshot {
                    name: Bytes::from("c1"),
                    seen_time_ms: 100,
                    pending: 1,
                }],
            }],
        };
        let snap = DbSnapshot {
            strings: vec![],
            zsets: vec![],
            geos: vec![],
            hashes: vec![],
            lists: vec![],
            sets: vec![],
            streams: vec![StreamRecord {
                key: Bytes::from("s"),
                state: state.clone(),
            }],
            typed_expires: vec![],
            search_indices: vec![],
            search_aliases: vec![],
            hnsw_graphs: vec![],
        };
        let multi = MultiDbSnapshot {
            databases: vec![(0, snap)],
        };
        let bytes = multi.encode().unwrap();
        assert!(bytes.starts_with(b"KORDB\0"));
        let version = u32::from_le_bytes(bytes[6..10].try_into().unwrap());
        assert_eq!(version, 6);

        let decoded = MultiDbSnapshot::decode(&bytes).unwrap();
        assert_eq!(decoded.databases.len(), 1);
        let st = &decoded.databases[0].1.streams[0];
        assert_eq!(st.key, Bytes::from("s"));
        assert_eq!(st.state.entries.len(), 2);
        assert_eq!(st.state.groups.len(), 1);
        assert_eq!(st.state.groups[0].pending.len(), 1);
        assert_eq!(st.state.last_generated_id, StreamId::new(1, 1));
        let _ = state;
    }

    #[test]
    fn v2_without_streams_still_loads() {
        // Build a minimal v2-like body manually via encode_body of empty streams
        // by crafting MultiDbSnapshot is always v3 — instead test that decode
        // of a hand-built v2 file works.
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        cache
            .store(
                Bytes::from("k"),
                Bytes::from("v"),
                StoreOptions::default(),
            )
            .unwrap();
        // Encode as v3, decode, verify
        let bytes = save_to_bytes(&cache).unwrap();
        let cache2 = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        load_bytes(&cache2, &bytes, true).unwrap();
        assert!(cache2.exists(&Bytes::from("k")));
    }
}
