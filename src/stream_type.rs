//! Redis Stream data type: append-only log of field/value entries with IDs.
//!
//! Supports basic stream ops and a minimal consumer-group model
//! (XGROUP CREATE/DESTROY, XREADGROUP, XACK, XPENDING summary).

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Shared stream handle stored in the cache keyspace.
pub type SharedStream = Arc<RwLock<RedisStream>>;

/// Stream entry ID: `<millisecondsTime>-<sequenceNumber>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    /// Exclusive upper bound sentinel for ranges (larger than any real ID).
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    pub fn to_string_id(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(self.to_string_id())
    }

    /// Parse `ms-seq`, `ms`, `-`, `+`, `$`, or `0-0`.
    /// Returns None on malformed input.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        if s == "-" {
            return Some(Self::MIN);
        }
        if s == "+" {
            return Some(Self::MAX);
        }
        // `$` handled by caller (means last ID in stream)
        if s == "$" || s == "*" {
            return None;
        }
        if let Some((ms_s, seq_s)) = s.split_once('-') {
            let ms = ms_s.parse::<u64>().ok()?;
            let seq = seq_s.parse::<u64>().ok()?;
            return Some(Self { ms, seq });
        }
        // bare milliseconds → sequence 0
        let ms = s.parse::<u64>().ok()?;
        Some(Self { ms, seq: 0 })
    }

    /// Parse for XADD explicit ID (must be ms-seq, not specials except handled elsewhere).
    pub fn parse_explicit(s: &str) -> Option<Self> {
        let s = s.trim();
        let (ms_s, seq_s) = s.split_once('-')?;
        let ms = ms_s.parse::<u64>().ok()?;
        let seq = seq_s.parse::<u64>().ok()?;
        Some(Self { ms, seq })
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// One stream message.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(Bytes, Bytes)>,
}

impl StreamEntry {
    pub fn memory_size(&self) -> usize {
        use crate::memory::{with_alloc_overhead, BYTES_OVERHEAD, DICT_ENTRY_OVERHEAD};
        let mut n = std::mem::size_of::<Self>() + 16; // ID overhead
        for (k, v) in &self.fields {
            n += k.len() + v.len() + BYTES_OVERHEAD * 2 + DICT_ENTRY_OVERHEAD;
        }
        with_alloc_overhead(n)
    }
}

/// XINFO STREAM summary fields.
#[derive(Debug, Clone)]
pub struct XInfoStream {
    pub length: usize,
    pub last_generated_id: StreamId,
    pub entries_added: u64,
    pub max_deleted_entry_id: StreamId,
    pub groups: usize,
    pub first_entry: Option<StreamEntry>,
    pub last_entry: Option<StreamEntry>,
}

/// One group row for XINFO GROUPS.
#[derive(Debug, Clone)]
pub struct XInfoGroup {
    pub name: Bytes,
    pub consumers: usize,
    pub pending: usize,
    pub last_delivered_id: StreamId,
    /// Logical entries-read counter (None → null in XINFO).
    pub entries_read: Option<u64>,
    /// Entries with ID greater than last-delivered-id (approximate lag).
    pub lag: usize,
}

/// One consumer row for XINFO CONSUMERS.
#[derive(Debug, Clone)]
pub struct XInfoConsumer {
    pub name: Bytes,
    pub pending: usize,
    pub idle_ms: u64,
    pub inactive_ms: u64,
}

/// Nested group detail for XINFO STREAM FULL.
#[derive(Debug, Clone)]
pub struct XInfoStreamFullGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,
    pub lag: usize,
    pub pel_count: usize,
    pub pending: Vec<PendingEntry>,
    pub consumers: Vec<XInfoStreamFullConsumer>,
}

/// Nested consumer detail for XINFO STREAM FULL.
#[derive(Debug, Clone)]
pub struct XInfoStreamFullConsumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub pel_count: usize,
    pub pending: Vec<PendingEntry>,
}

/// Pending entry in a consumer group's PEL.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: StreamId,
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: u64,
}

/// Options for XCLAIM / FORCE path.
#[derive(Debug, Clone)]
pub struct XClaimOpts {
    /// Minimum idle time in ms required to claim (0 = always if pending/FORCE).
    pub min_idle_ms: u64,
    /// Create PEL entry even if not pending (message must exist).
    pub force: bool,
    /// Absolute delivery timestamp; else derived from IDLE or now.
    pub time_ms: Option<u64>,
    /// Relative idle ms → delivery_time = now - idle.
    pub idle_ms: Option<u64>,
    /// Override delivery count; if None and not just_id, increment by 1.
    pub retrycount: Option<u64>,
    /// JUSTID: do not increment delivery count; return IDs only at command layer.
    pub just_id: bool,
}

/// Per-consumer state inside a group.
#[derive(Debug, Clone)]
pub struct Consumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub pending: usize,
}

/// Consumer group.
#[derive(Debug)]
pub struct ConsumerGroup {
    pub name: Bytes,
    /// Last ID delivered to the group (for `>` reads).
    pub last_delivered_id: StreamId,
    /// Logical entries-read counter for lag accounting (XGROUP CREATE/SETID ENTRIESREAD).
    pub entries_read: Option<u64>,
    pub consumers: HashMap<Bytes, Consumer>,
    /// Pending entries list (PEL), keyed by stream ID.
    pub pending: BTreeMap<StreamId, PendingEntry>,
}

impl ConsumerGroup {
    pub fn new(name: Bytes, last_delivered_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id,
            entries_read: None,
            consumers: HashMap::new(),
            pending: BTreeMap::new(),
        }
    }
}

/// Redis Stream.
#[derive(Debug)]
pub struct RedisStream {
    /// Entries ordered by ID.
    entries: BTreeMap<StreamId, StreamEntry>,
    /// Highest ID ever assigned (for auto-ID generation).
    last_generated_id: StreamId,
    /// Logical entries-added counter (XADD increments; XSETID ENTRIESADDED can set).
    entries_added: u64,
    /// Highest ID deleted via XDEL/trim (XSETID MAXDELETEDID can set).
    max_deleted_entry_id: StreamId,
    /// Consumer groups by name.
    groups: HashMap<Bytes, ConsumerGroup>,
}

impl RedisStream {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_generated_id: StreamId::ZERO,
            entries_added: 0,
            max_deleted_entry_id: StreamId::ZERO,
            groups: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn last_id(&self) -> StreamId {
        self.entries
            .keys()
            .next_back()
            .copied()
            .unwrap_or(StreamId::ZERO)
    }

    pub fn last_generated_id(&self) -> StreamId {
        self.last_generated_id
    }

    /// Lookup a single entry by ID.
    pub fn get_entry(&self, id: &StreamId) -> Option<&StreamEntry> {
        self.entries.get(id)
    }

    pub fn memory_size(&self) -> usize {
        use crate::memory::{with_alloc_overhead, BYTES_OVERHEAD, DICT_ENTRY_OVERHEAD};
        let mut n = std::mem::size_of::<Self>();
        // BTree / map structural overhead (rough)
        n += self.entries.len().saturating_mul(48);
        n += self.groups.capacity().saturating_mul(8);
        for e in self.entries.values() {
            n += e.memory_size();
        }
        for g in self.groups.values() {
            n += g.name.len() + BYTES_OVERHEAD + DICT_ENTRY_OVERHEAD + 64;
            n += g.pending.len() * (64 + DICT_ENTRY_OVERHEAD);
            for c in g.consumers.values() {
                n += c.name.len() + 32;
            }
        }
        with_alloc_overhead(n)
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Generate next auto ID (`*`), strictly greater than `last_generated_id`.
    fn next_auto_id(&self) -> StreamId {
        let now = Self::now_ms();
        let last = self.last_generated_id;
        if now > last.ms {
            StreamId::new(now, 0)
        } else if now == last.ms {
            StreamId::new(last.ms, last.seq.saturating_add(1))
        } else {
            // Clock went backwards — bump sequence on last ms
            StreamId::new(last.ms, last.seq.saturating_add(1))
        }
    }

    /// XADD. `id_spec` is `*` or explicit `ms-seq`.
    /// Returns the assigned ID, or error string.
    pub fn xadd(
        &mut self,
        id_spec: &str,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<StreamId, String> {
        if fields.is_empty() {
            return Err("ERR wrong number of arguments for 'xadd' command".into());
        }
        if fields.len() % 2 != 0 {
            // fields come as pairs already; caller should pair them
        }
        // Ensure even number of field/value at call site

        let id = if id_spec == "*" {
            self.next_auto_id()
        } else {
            let explicit = StreamId::parse_explicit(id_spec)
                .ok_or_else(|| "ERR Invalid stream ID specified as stream command argument".to_string())?;
            if explicit == StreamId::ZERO {
                return Err("ERR The ID specified in XADD must be greater than 0-0".into());
            }
            if explicit <= self.last_generated_id {
                return Err(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                        .into(),
                );
            }
            explicit
        };

        let entry = StreamEntry {
            id,
            fields,
        };
        self.entries.insert(id, entry);
        self.last_generated_id = id;
        self.entries_added = self.entries_added.saturating_add(1);
        Ok(id)
    }

    /// XADD with optional MAXLEN trim after insert. `maxlen` exact trim when Some.
    pub fn xadd_maxlen(
        &mut self,
        id_spec: &str,
        fields: Vec<(Bytes, Bytes)>,
        maxlen: Option<usize>,
    ) -> Result<StreamId, String> {
        self.xadd_with_trim(id_spec, fields, maxlen, None, None)
    }

    /// XADD with optional MAXLEN or MINID trim after insert.
    /// `limit` caps how many entries are deleted in this trim (Redis LIMIT).
    pub fn xadd_with_trim(
        &mut self,
        id_spec: &str,
        fields: Vec<(Bytes, Bytes)>,
        maxlen: Option<usize>,
        minid: Option<StreamId>,
        limit: Option<usize>,
    ) -> Result<StreamId, String> {
        let id = self.xadd(id_spec, fields)?;
        if let Some(max) = maxlen {
            self.trim_maxlen_limit(max, limit);
        } else if let Some(min) = minid {
            self.trim_minid_limit(min, limit);
        }
        Ok(id)
    }

    /// Drop entry `id` from the stream and consumer-group PELs.
    fn remove_entry_and_pel(&mut self, id: StreamId) {
        if self.entries.remove(&id).is_some() {
            if id > self.max_deleted_entry_id {
                self.max_deleted_entry_id = id;
            }
        }
        for g in self.groups.values_mut() {
            if g.pending.remove(&id).is_some() {
                for c in g.consumers.values_mut() {
                    if c.pending > 0 {
                        c.pending -= 1;
                        break;
                    }
                }
            }
        }
    }

    /// Keep only the newest `max` entries.
    pub fn trim_maxlen(&mut self, max: usize) -> usize {
        self.trim_maxlen_limit(max, None)
    }

    /// Keep newest `max` entries; if `limit` is set, delete at most that many.
    pub fn trim_maxlen_limit(&mut self, max: usize, limit: Option<usize>) -> usize {
        if self.entries.len() <= max {
            return 0;
        }
        let mut remove_count = self.entries.len() - max;
        if let Some(lim) = limit {
            remove_count = remove_count.min(lim);
        }
        let mut removed = 0;
        let ids: Vec<StreamId> = self.entries.keys().copied().take(remove_count).collect();
        for id in ids {
            self.remove_entry_and_pel(id);
            removed += 1;
        }
        removed
    }

    /// Evict entries with IDs strictly lower than `minid` (Redis MINID).
    pub fn trim_minid(&mut self, minid: StreamId) -> usize {
        self.trim_minid_limit(minid, None)
    }

    /// Evict entries with IDs strictly lower than `minid`; cap deletions with `limit`.
    pub fn trim_minid_limit(&mut self, minid: StreamId, limit: Option<usize>) -> usize {
        let mut ids: Vec<StreamId> = self
            .entries
            .range(..minid)
            .map(|(id, _)| *id)
            .collect();
        if let Some(lim) = limit {
            if ids.len() > lim {
                ids.truncate(lim);
            }
        }
        let mut removed = 0;
        for id in ids {
            self.remove_entry_and_pel(id);
            removed += 1;
        }
        removed
    }

    /// XRANGE [start, end] inclusive. Specials: `-` `+`.
    pub fn xrange(
        &self,
        start: StreamId,
        end: StreamId,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut out: Vec<&StreamEntry> = self
            .entries
            .range(start..=end)
            .map(|(_, e)| e)
            .collect();
        if let Some(c) = count {
            if out.len() > c {
                out.truncate(c);
            }
        }
        out
    }

    /// XREVRANGE: reverse order from end down to start.
    pub fn xrevrange(
        &self,
        end: StreamId,
        start: StreamId,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut out: Vec<&StreamEntry> = self
            .entries
            .range(start..=end)
            .rev()
            .map(|(_, e)| e)
            .collect();
        if let Some(c) = count {
            if out.len() > c {
                out.truncate(c);
            }
        }
        out
    }

    /// XDEL — returns number of deleted entries.
    pub fn xdel(&mut self, ids: &[StreamId]) -> usize {
        let mut n = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                n += 1;
                if *id > self.max_deleted_entry_id {
                    self.max_deleted_entry_id = *id;
                }
                for g in self.groups.values_mut() {
                    if let Some(pe) = g.pending.remove(id) {
                        if let Some(c) = g.consumers.get_mut(&pe.consumer) {
                            c.pending = c.pending.saturating_sub(1);
                        }
                    }
                }
            }
        }
        n
    }

    /// Entries with ID strictly greater than `after`, up to `count`.
    pub fn xread_after(&self, after: StreamId, count: Option<usize>) -> Vec<&StreamEntry> {
        let start = next_id(after);
        let mut out: Vec<&StreamEntry> = self
            .entries
            .range(start..)
            .map(|(_, e)| e)
            .collect();
        if let Some(c) = count {
            if out.len() > c {
                out.truncate(c);
            }
        }
        out
    }

    // ── Consumer groups ──────────────────────────────────────────────────

    pub fn group_create(
        &mut self,
        name: Bytes,
        id: StreamId,
        mkstream_ok: bool,
        entries_read: Option<u64>,
    ) -> Result<(), String> {
        let _ = mkstream_ok; // stream existence checked by caller
        if self.groups.contains_key(&name) {
            return Err("BUSYGROUP Consumer Group name already exists".into());
        }
        let mut group = ConsumerGroup::new(name.clone(), id);
        group.entries_read = entries_read;
        self.groups.insert(name, group);
        Ok(())
    }

    pub fn group_destroy(&mut self, name: &Bytes) -> bool {
        self.groups.remove(name).is_some()
    }

    /// XGROUP CREATECONSUMER — create consumer if missing.
    /// Returns `true` if created, `false` if already present.
    pub fn group_create_consumer(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
    ) -> Result<bool, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| "NOGROUP No such key '' or consumer group".to_string())?;
        if group.consumers.contains_key(consumer_name) {
            return Ok(false);
        }
        let now = Self::now_ms();
        group.consumers.insert(
            consumer_name.clone(),
            Consumer {
                name: consumer_name.clone(),
                seen_time_ms: now,
                pending: 0,
            },
        );
        Ok(true)
    }

    /// XGROUP DELCONSUMER — remove consumer and drop their PEL entries.
    /// Returns number of pending messages that belonged to the consumer.
    pub fn group_del_consumer(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
    ) -> Result<usize, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| "NOGROUP No such key '' or consumer group".to_string())?;
        if !group.consumers.contains_key(consumer_name) {
            return Ok(0);
        }
        let pending_ids: Vec<StreamId> = group
            .pending
            .iter()
            .filter(|(_, pe)| &pe.consumer == consumer_name)
            .map(|(id, _)| *id)
            .collect();
        let n = pending_ids.len();
        for id in pending_ids {
            group.pending.remove(&id);
        }
        group.consumers.remove(consumer_name);
        Ok(n)
    }

    /// XGROUP SETID — set the group's last_delivered_id cursor.
    /// Update last-delivered-id; when `entries_read` is `Some`, also set the counter.
    pub fn group_setid(
        &mut self,
        name: &Bytes,
        id: StreamId,
        entries_read: Option<u64>,
    ) -> Result<(), String> {
        let group = self
            .groups
            .get_mut(name)
            .ok_or_else(|| "NOGROUP No such key '' or consumer group".to_string())?;
        group.last_delivered_id = id;
        if let Some(v) = entries_read {
            group.entries_read = Some(v);
        }
        Ok(())
    }

    /// XSETID — set stream last_generated_id (must be ≥ max entry id).
    /// Optional ENTRIESADDED / MAXDELETEDID update logical counters.
    pub fn xsetid(
        &mut self,
        id: StreamId,
        entries_added: Option<u64>,
        max_deleted_entry_id: Option<StreamId>,
    ) -> Result<(), String> {
        if let Some(max_id) = self.entries.keys().next_back().copied() {
            if id < max_id {
                return Err(
                    "ERR The ID specified in XSETID is smaller than the current top ID".into(),
                );
            }
        }
        self.last_generated_id = id;
        if let Some(n) = entries_added {
            self.entries_added = n;
        }
        if let Some(mid) = max_deleted_entry_id {
            self.max_deleted_entry_id = mid;
        }
        Ok(())
    }

    /// XCLAIM — transfer ownership of pending messages.
    ///
    /// Returns claimed entry IDs (in request order). Entries missing from the stream
    /// are skipped (and removed from PEL if present — orphan cleanup).
    pub fn xclaim(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        ids: &[StreamId],
        opts: &XClaimOpts,
    ) -> Result<Vec<StreamId>, String> {
        if !self.groups.contains_key(group_name) {
            return Err("NOGROUP No such key '' or consumer group".into());
        }
        let now = Self::now_ms();
        let delivery_time = if let Some(t) = opts.time_ms {
            t
        } else if let Some(idle) = opts.idle_ms {
            now.saturating_sub(idle)
        } else {
            now
        };

        let mut claimed = Vec::new();
        for &id in ids {
            // Orphan PEL entry (stream message deleted): drop from PEL, skip.
            if !self.entries.contains_key(&id) {
                let group = self.groups.get_mut(group_name).unwrap();
                if let Some(pe) = group.pending.remove(&id) {
                    if let Some(c) = group.consumers.get_mut(&pe.consumer) {
                        c.pending = c.pending.saturating_sub(1);
                    }
                }
                continue;
            }

            let group = self.groups.get_mut(group_name).unwrap();
            let in_pel = group.pending.contains_key(&id);
            if !in_pel && !opts.force {
                continue;
            }
            if in_pel {
                let pe = group.pending.get(&id).unwrap();
                let idle = now.saturating_sub(pe.delivery_time_ms);
                if idle < opts.min_idle_ms {
                    continue;
                }
            }

            // Ensure consumer exists.
            group
                .consumers
                .entry(consumer_name.clone())
                .or_insert_with(|| Consumer {
                    name: consumer_name.clone(),
                    seen_time_ms: now,
                    pending: 0,
                });
            if let Some(c) = group.consumers.get_mut(consumer_name) {
                c.seen_time_ms = now;
            }

            let is_new = !group.pending.contains_key(&id);
            let pe = group.pending.entry(id).or_insert_with(|| PendingEntry {
                id,
                consumer: consumer_name.clone(),
                delivery_time_ms: delivery_time,
                delivery_count: 0,
            });
            if pe.consumer != *consumer_name {
                if let Some(old) = group.consumers.get_mut(&pe.consumer) {
                    old.pending = old.pending.saturating_sub(1);
                }
                pe.consumer = consumer_name.clone();
                if let Some(c) = group.consumers.get_mut(consumer_name) {
                    c.pending += 1;
                }
            } else if is_new {
                if let Some(c) = group.consumers.get_mut(consumer_name) {
                    c.pending += 1;
                }
            }
            pe.delivery_time_ms = delivery_time;
            if let Some(dc) = opts.retrycount {
                pe.delivery_count = dc;
            } else if !opts.just_id {
                pe.delivery_count = pe.delivery_count.saturating_add(1).max(1);
            } else if pe.delivery_count == 0 {
                pe.delivery_count = 1;
            }
            claimed.push(id);
        }
        Ok(claimed)
    }

    /// Force a message into the PEL for AOF rewrite / recovery (XCLAIM … FORCE).
    pub fn xclaim_force(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        ids: &[StreamId],
        delivery_time_ms: Option<u64>,
        delivery_count: Option<u64>,
    ) -> Result<Vec<StreamId>, String> {
        self.xclaim(
            group_name,
            consumer_name,
            ids,
            &XClaimOpts {
                min_idle_ms: 0,
                force: true,
                time_ms: delivery_time_ms,
                idle_ms: None,
                retrycount: delivery_count,
                just_id: false,
            },
        )
    }

    /// XAUTOCLAIM — scan PEL from `start`, claim idle entries up to `count`.
    ///
    /// Returns `(next_start_id, claimed_ids, deleted_ids)` where `deleted_ids` are
    /// PEL orphans removed because the stream entry no longer exists.
    pub fn xautoclaim(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        min_idle_ms: u64,
        start: StreamId,
        count: usize,
        just_id: bool,
    ) -> Result<(StreamId, Vec<StreamId>, Vec<StreamId>), String> {
        if !self.groups.contains_key(group_name) {
            return Err("NOGROUP No such key '' or consumer group".into());
        }
        let now = Self::now_ms();

        // Collect candidates: IDs in PEL >= start that are idle enough (or orphans).
        let candidates: Vec<StreamId> = {
            let group = self.groups.get(group_name).unwrap();
            group
                .pending
                .range(start..)
                .map(|(id, pe)| (*id, pe.delivery_time_ms))
                .filter(|(id, dt)| {
                    !self.entries.contains_key(id) || now.saturating_sub(*dt) >= min_idle_ms
                })
                .map(|(id, _)| id)
                .collect()
        };

        let mut claimed = Vec::new();
        let mut deleted = Vec::new();
        let mut last_examined: Option<StreamId> = None;

        for id in candidates {
            last_examined = Some(id);
            if !self.entries.contains_key(&id) {
                let group = self.groups.get_mut(group_name).unwrap();
                if let Some(pe) = group.pending.remove(&id) {
                    if let Some(c) = group.consumers.get_mut(&pe.consumer) {
                        c.pending = c.pending.saturating_sub(1);
                    }
                    deleted.push(id);
                }
                continue;
            }
            if claimed.len() >= count {
                // Still scan? Redis stops once COUNT claimed; next cursor is last examined.
                // Continue only if we already hit count — break after setting cursor.
                break;
            }
            let ids = [id];
            let got = self.xclaim(
                group_name,
                consumer_name,
                &ids,
                &XClaimOpts {
                    min_idle_ms,
                    force: false,
                    time_ms: None,
                    idle_ms: None,
                    retrycount: None,
                    just_id,
                },
            )?;
            claimed.extend(got);
            if claimed.len() >= count {
                break;
            }
        }

        // Next cursor: exclusive next after last claimed/examined; "0-0" if end of PEL.
        let next = if let Some(last) = last_examined {
            let group = self.groups.get(group_name).unwrap();
            group
                .pending
                .range(next_id(last)..)
                .next()
                .map(|(id, _)| *id)
                .unwrap_or(StreamId::ZERO)
        } else {
            StreamId::ZERO
        };

        Ok((next, claimed, deleted))
    }

    /// XPENDING range form: list pending entries in [start, end], up to `count`.
    /// Optional consumer filter and minimum idle time.
    pub fn xpending_range(
        &self,
        group_name: &Bytes,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<&Bytes>,
        min_idle_ms: u64,
    ) -> Result<Vec<PendingEntry>, String> {
        let group = self
            .groups
            .get(group_name)
            .ok_or_else(|| "NOGROUP No such key '' or consumer group".to_string())?;
        let now = Self::now_ms();
        let mut out = Vec::new();
        for (id, pe) in group.pending.range(start..=end) {
            let _ = id;
            if let Some(c) = consumer {
                if &pe.consumer != c {
                    continue;
                }
            }
            let idle = now.saturating_sub(pe.delivery_time_ms);
            if idle < min_idle_ms {
                continue;
            }
            out.push(pe.clone());
            if out.len() >= count {
                break;
            }
        }
        Ok(out)
    }

    /// Snapshot for XINFO STREAM (summary form).
    pub fn xinfo_stream(&self) -> XInfoStream {
        let first = self.entries.values().next().cloned();
        let last = self.entries.values().next_back().cloned();
        XInfoStream {
            length: self.entries.len(),
            last_generated_id: self.last_generated_id,
            entries_added: self.entries_added,
            max_deleted_entry_id: self.max_deleted_entry_id,
            groups: self.groups.len(),
            first_entry: first,
            last_entry: last,
        }
    }

    /// Entries for XINFO STREAM FULL, optionally limited by `count`.
    pub fn xinfo_stream_entries(&self, count: Option<usize>) -> Vec<StreamEntry> {
        let mut out: Vec<StreamEntry> = self.entries.values().cloned().collect();
        if let Some(n) = count {
            if out.len() > n {
                out.truncate(n);
            }
        }
        out
    }

    /// Group detail for XINFO STREAM FULL (PEL/consumer pending limited by `count`).
    pub fn xinfo_stream_full_groups(&self, count: Option<usize>) -> Vec<XInfoStreamFullGroup> {
        let mut groups: Vec<XInfoStreamFullGroup> = self
            .groups
            .values()
            .map(|g| {
                let lag = self
                    .entries
                    .range(next_id(g.last_delivered_id)..)
                    .count();
                let mut pending: Vec<PendingEntry> = g.pending.values().cloned().collect();
                if let Some(n) = count {
                    if pending.len() > n {
                        pending.truncate(n);
                    }
                }
                let mut consumers: Vec<XInfoStreamFullConsumer> = g
                    .consumers
                    .values()
                    .map(|c| {
                        let mut c_pending: Vec<PendingEntry> = g
                            .pending
                            .values()
                            .filter(|pe| pe.consumer == c.name)
                            .cloned()
                            .collect();
                        if let Some(n) = count {
                            if c_pending.len() > n {
                                c_pending.truncate(n);
                            }
                        }
                        XInfoStreamFullConsumer {
                            name: c.name.clone(),
                            seen_time_ms: c.seen_time_ms,
                            pel_count: g
                                .pending
                                .values()
                                .filter(|pe| pe.consumer == c.name)
                                .count(),
                            pending: c_pending,
                        }
                    })
                    .collect();
                consumers.sort_by(|a, b| a.name.cmp(&b.name));
                XInfoStreamFullGroup {
                    name: g.name.clone(),
                    last_delivered_id: g.last_delivered_id,
                    entries_read: g.entries_read,
                    lag,
                    pel_count: g.pending.len(),
                    pending,
                    consumers,
                }
            })
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        groups
    }

    /// Snapshot for XINFO GROUPS.
    pub fn xinfo_groups(&self) -> Vec<XInfoGroup> {
        let mut groups: Vec<XInfoGroup> = self
            .groups
            .values()
            .map(|g| {
                let lag = self
                    .entries
                    .range(next_id(g.last_delivered_id)..)
                    .count();
                XInfoGroup {
                    name: g.name.clone(),
                    consumers: g.consumers.len(),
                    pending: g.pending.len(),
                    last_delivered_id: g.last_delivered_id,
                    entries_read: g.entries_read,
                    lag,
                }
            })
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        groups
    }

    /// Snapshot for XINFO CONSUMERS key group.
    pub fn xinfo_consumers(&self, group_name: &Bytes) -> Result<Vec<XInfoConsumer>, String> {
        let group = self
            .groups
            .get(group_name)
            .ok_or_else(|| "NOGROUP No such key '' or consumer group".to_string())?;
        let now = Self::now_ms();
        let mut consumers: Vec<XInfoConsumer> = group
            .consumers
            .values()
            .map(|c| {
                let pending = group
                    .pending
                    .values()
                    .filter(|pe| &pe.consumer == &c.name)
                    .count();
                let idle = now.saturating_sub(c.seen_time_ms);
                XInfoConsumer {
                    name: c.name.clone(),
                    pending,
                    idle_ms: idle,
                    inactive_ms: idle,
                }
            })
            .collect();
        consumers.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(consumers)
    }

    pub fn group_exists(&self, name: &Bytes) -> bool {
        self.groups.contains_key(name)
    }

    pub fn group_names(&self) -> Vec<Bytes> {
        self.groups.keys().cloned().collect()
    }

    /// XREADGROUP: read new messages (`>`) or reclaim from history starting after `id`.
    /// Returns entries and updates PEL / last_delivered_id.
    /// When `noack` is true, new (`>`) messages skip the PEL (still advance last_delivered_id).
    pub fn xreadgroup(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        id_spec: &str,
        count: Option<usize>,
    ) -> Result<Vec<StreamEntry>, String> {
        self.xreadgroup_opts(group_name, consumer_name, id_spec, count, false)
    }

    /// XREADGROUP with optional NOACK.
    pub fn xreadgroup_opts(
        &mut self,
        group_name: &Bytes,
        consumer_name: &Bytes,
        id_spec: &str,
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<StreamEntry>, String> {
        if !self.groups.contains_key(group_name) {
            return Err("NOGROUP No such key '' or consumer group".into());
        }

        let now = Self::now_ms();

        // Snapshot last_delivered / pending IDs under a short mutable borrow, then
        // read entries without holding the group lock against self.entries.
        let entries: Vec<StreamEntry> = if id_spec == ">" {
            let after = self.groups.get(group_name).unwrap().last_delivered_id;
            let start = next_id(after);
            let mut collected = Vec::new();
            for (_, entry) in self.entries.range(start..) {
                collected.push(entry.clone());
                if let Some(c) = count {
                    if collected.len() >= c {
                        break;
                    }
                }
            }
            collected
        } else {
            // Redis: non-`>` ID returns pending messages for this consumer with ID > id
            let after = if id_spec == "0" || id_spec == "0-0" {
                StreamId::ZERO
            } else {
                StreamId::parse_explicit(id_spec)
                    .or_else(|| StreamId::parse(id_spec))
                    .ok_or_else(|| {
                        "ERR Invalid stream ID specified as stream command argument".to_string()
                    })?
            };
            let start = next_id(after);
            let pending_ids: Vec<StreamId> = {
                let group = self.groups.get(group_name).unwrap();
                group
                    .pending
                    .range(start..)
                    .filter(|(_, pe)| &pe.consumer == consumer_name)
                    .map(|(id, _)| *id)
                    .collect()
            };
            let mut collected = Vec::new();
            for id in pending_ids {
                if let Some(entry) = self.entries.get(&id) {
                    collected.push(entry.clone());
                    if let Some(c) = count {
                        if collected.len() >= c {
                            break;
                        }
                    }
                }
            }
            collected
        };

        // Ensure consumer + update PEL
        let group = self.groups.get_mut(group_name).unwrap();
        group
            .consumers
            .entry(consumer_name.clone())
            .or_insert_with(|| Consumer {
                name: consumer_name.clone(),
                seen_time_ms: now,
                pending: 0,
            });
        if let Some(c) = group.consumers.get_mut(consumer_name) {
            c.seen_time_ms = now;
        }

        if id_spec == ">" {
            for entry in &entries {
                group.last_delivered_id = entry.id;
                if noack {
                    // NOACK: deliver without adding to the PEL.
                    continue;
                }
                let pe = group.pending.entry(entry.id).or_insert_with(|| PendingEntry {
                    id: entry.id,
                    consumer: consumer_name.clone(),
                    delivery_time_ms: now,
                    delivery_count: 0,
                });
                if pe.consumer != *consumer_name {
                    if let Some(old) = group.consumers.get_mut(&pe.consumer) {
                        old.pending = old.pending.saturating_sub(1);
                    }
                    pe.consumer = consumer_name.clone();
                }
                pe.delivery_count += 1;
                pe.delivery_time_ms = now;
            }
            if !noack {
                if let Some(c) = group.consumers.get_mut(consumer_name) {
                    c.pending = group
                        .pending
                        .values()
                        .filter(|p| &p.consumer == consumer_name)
                        .count();
                }
            }
        } else {
            for entry in &entries {
                if let Some(pe) = group.pending.get_mut(&entry.id) {
                    pe.delivery_count += 1;
                    pe.delivery_time_ms = now;
                }
            }
        }

        Ok(entries)
    }

    /// XACK — acknowledge pending IDs. Returns count removed from PEL.
    pub fn xack(&mut self, group_name: &Bytes, ids: &[StreamId]) -> Result<usize, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| "NOGROUP No such key '' or consumer group".to_string())?;
        let mut n = 0;
        for id in ids {
            if let Some(pe) = group.pending.remove(id) {
                n += 1;
                if let Some(c) = group.consumers.get_mut(&pe.consumer) {
                    c.pending = c.pending.saturating_sub(1);
                }
            }
        }
        Ok(n)
    }

    /// XPENDING summary: (total, min_id, max_id, consumers: [(name, count)])
    pub fn xpending_summary(
        &self,
        group_name: &Bytes,
    ) -> Result<(usize, Option<StreamId>, Option<StreamId>, Vec<(Bytes, usize)>), String> {
        let group = self
            .groups
            .get(group_name)
            .ok_or_else(|| "NOGROUP No such key '' or consumer group".to_string())?;
        let total = group.pending.len();
        let min_id = group.pending.keys().next().copied();
        let max_id = group.pending.keys().next_back().copied();
        let mut consumers: HashMap<Bytes, usize> = HashMap::new();
        for pe in group.pending.values() {
            *consumers.entry(pe.consumer.clone()).or_insert(0) += 1;
        }
        let mut list: Vec<(Bytes, usize)> = consumers.into_iter().collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        Ok((total, min_id, max_id, list))
    }

    /// Export entries for persistence / tests: (id, fields).
    pub fn export_entries(&self) -> Vec<(StreamId, Vec<(Bytes, Bytes)>)> {
        self.entries
            .values()
            .map(|e| (e.id, e.fields.clone()))
            .collect()
    }

    /// Full stream snapshot for RDB / AOF rewrite (entries, last id, groups, PEL).
    pub fn export_state(&self) -> StreamStateSnapshot {
        let entries = self.export_entries();
        let mut groups = Vec::with_capacity(self.groups.len());
        for g in self.groups.values() {
            let pending: Vec<PendingEntrySnapshot> = g
                .pending
                .values()
                .map(|pe| PendingEntrySnapshot {
                    id: pe.id,
                    consumer: pe.consumer.clone(),
                    delivery_time_ms: pe.delivery_time_ms,
                    delivery_count: pe.delivery_count,
                })
                .collect();
            let consumers: Vec<ConsumerSnapshot> = g
                .consumers
                .values()
                .map(|c| ConsumerSnapshot {
                    name: c.name.clone(),
                    seen_time_ms: c.seen_time_ms,
                    pending: c.pending,
                })
                .collect();
            groups.push(GroupSnapshot {
                name: g.name.clone(),
                last_delivered_id: g.last_delivered_id,
                pending,
                consumers,
            });
        }
        StreamStateSnapshot {
            last_generated_id: self.last_generated_id,
            entries,
            groups,
        }
    }

    /// Rebuild stream from a full persistence snapshot (replaces current state).
    pub fn import_state(&mut self, state: StreamStateSnapshot) {
        self.entries.clear();
        self.groups.clear();
        for (id, fields) in state.entries {
            self.entries.insert(
                id,
                StreamEntry {
                    id,
                    fields,
                },
            );
        }
        self.last_generated_id = state.last_generated_id;
        // Ensure last_generated_id is at least the max entry id.
        if let Some(max_id) = self.entries.keys().next_back().copied() {
            if max_id > self.last_generated_id {
                self.last_generated_id = max_id;
            }
        }
        // RDB/AOF snapshots do not yet persist these counters; approximate from length.
        self.entries_added = self.entries.len() as u64;
        self.max_deleted_entry_id = StreamId::ZERO;
        for g in state.groups {
            let mut group = ConsumerGroup::new(g.name.clone(), g.last_delivered_id);
            for c in g.consumers {
                group.consumers.insert(
                    c.name.clone(),
                    Consumer {
                        name: c.name,
                        seen_time_ms: c.seen_time_ms,
                        pending: c.pending,
                    },
                );
            }
            for pe in g.pending {
                // Ensure consumer entry exists for PEL owner.
                group.consumers.entry(pe.consumer.clone()).or_insert_with(|| {
                    Consumer {
                        name: pe.consumer.clone(),
                        seen_time_ms: pe.delivery_time_ms,
                        pending: 0,
                    }
                });
                group.pending.insert(
                    pe.id,
                    PendingEntry {
                        id: pe.id,
                        consumer: pe.consumer,
                        delivery_time_ms: pe.delivery_time_ms,
                        delivery_count: pe.delivery_count,
                    },
                );
            }
            // Recompute per-consumer pending counts from PEL.
            for c in group.consumers.values_mut() {
                c.pending = 0;
            }
            for pe in group.pending.values() {
                if let Some(c) = group.consumers.get_mut(&pe.consumer) {
                    c.pending += 1;
                }
            }
            self.groups.insert(g.name, group);
        }
    }
}

/// Full stream state for RDB / import helpers.
#[derive(Debug, Clone)]
pub struct StreamStateSnapshot {
    pub last_generated_id: StreamId,
    pub entries: Vec<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub groups: Vec<GroupSnapshot>,
}

/// Consumer group snapshot for persistence.
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub pending: Vec<PendingEntrySnapshot>,
    pub consumers: Vec<ConsumerSnapshot>,
}

/// PEL entry snapshot.
#[derive(Debug, Clone)]
pub struct PendingEntrySnapshot {
    pub id: StreamId,
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: u64,
}

/// Consumer snapshot (optional in RDB; used when present).
#[derive(Debug, Clone)]
pub struct ConsumerSnapshot {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub pending: usize,
}

impl Default for RedisStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Smallest ID strictly greater than `id`.
fn next_id(id: StreamId) -> StreamId {
    if id.seq < u64::MAX {
        StreamId::new(id.ms, id.seq + 1)
    } else if id.ms < u64::MAX {
        StreamId::new(id.ms + 1, 0)
    } else {
        StreamId::MAX
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_parse_and_order() {
        let a = StreamId::parse("1-0").unwrap();
        let b = StreamId::parse("1-1").unwrap();
        let c = StreamId::parse("2-0").unwrap();
        assert!(a < b && b < c);
        assert_eq!(a.to_string_id(), "1-0");
    }

    #[test]
    fn xadd_xrange_xlen() {
        let mut s = RedisStream::new();
        let id1 = s
            .xadd("*", vec![(Bytes::from("f"), Bytes::from("v1"))])
            .unwrap();
        let id2 = s
            .xadd("*", vec![(Bytes::from("f"), Bytes::from("v2"))])
            .unwrap();
        assert!(id2 > id1);
        assert_eq!(s.len(), 2);
        let range = s.xrange(StreamId::MIN, StreamId::MAX, None);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].fields[0].1.as_ref(), b"v1");
    }

    #[test]
    fn consumer_group_flow() {
        let mut s = RedisStream::new();
        s.xadd("1-0", vec![(Bytes::from("a"), Bytes::from("1"))])
            .unwrap();
        s.xadd("1-1", vec![(Bytes::from("a"), Bytes::from("2"))])
            .unwrap();
        s.group_create(Bytes::from("g"), StreamId::ZERO, true, None)
            .unwrap();
        let msgs = s
            .xreadgroup(&Bytes::from("g"), &Bytes::from("c1"), ">", Some(10))
            .unwrap();
        assert_eq!(msgs.len(), 2);
        let (total, _, _, _) = s.xpending_summary(&Bytes::from("g")).unwrap();
        assert_eq!(total, 2);
        let acked = s.xack(&Bytes::from("g"), &[msgs[0].id, msgs[1].id]).unwrap();
        assert_eq!(acked, 2);
        let (total, _, _, _) = s.xpending_summary(&Bytes::from("g")).unwrap();
        assert_eq!(total, 0);
    }
}
