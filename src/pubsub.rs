use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use crate::protocol::RespValue;

/// A unique identifier for each client connection
pub type ClientId = usize;

/// Default per-client broadcast buffer capacity (messages).
pub const DEFAULT_CLIENT_BUFFER_CAPACITY: usize = 1024;

/// Outcome of a publish attempt, including fan-out memory accounting hints.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishOutcome {
    /// Number of clients that accepted the message into their buffer.
    pub recipients: usize,
    /// Bytes newly enqueued into client pending buffers.
    pub bytes_enqueued: usize,
    /// Bytes dropped from full client buffers (oldest overwritten).
    pub bytes_overwritten: usize,
    /// Messages skipped (no sender / closed) or overwritten due to full buffers.
    pub messages_dropped: usize,
}

/// Pattern matching utility for Redis-style glob patterns.
///
/// Implemented iteratively (with star backtracking) so pathological patterns
/// cannot blow the call stack.
pub struct PatternMatcher;

impl PatternMatcher {
    /// Check if a channel matches a pattern (Redis-style glob matching)
    /// Supports: `*` (any chars), `?` (single char), `[...]` (char class), `\x` (escape)
    pub fn matches(pattern: &[u8], channel: &[u8]) -> bool {
        Self::matches_iter(pattern, channel)
    }

    /// Iterative glob match with a single star-backtrack checkpoint.
    fn matches_iter(pattern: &[u8], text: &[u8]) -> bool {
        let mut pi = 0usize;
        let mut ti = 0usize;
        // Last `*` position in pattern / corresponding text index for backtrack.
        let mut star_pi: Option<usize> = None;
        let mut star_ti = 0usize;

        while ti < text.len() {
            if pi < pattern.len() {
                match pattern[pi] {
                    b'*' => {
                        star_pi = Some(pi);
                        star_ti = ti;
                        pi += 1;
                        continue;
                    }
                    b'?' => {
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                    b'[' => {
                        let (matched, next_pi) =
                            Self::match_char_class(pattern, pi, text[ti]);
                        if matched {
                            pi = next_pi;
                            ti += 1;
                            continue;
                        }
                        // fall through to backtrack
                    }
                    b'\\' => {
                        if pi + 1 < pattern.len() && pattern[pi + 1] == text[ti] {
                            pi += 2;
                            ti += 1;
                            continue;
                        }
                        // fall through to backtrack
                    }
                    c if c == text[ti] => {
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                    _ => {
                        // mismatch — try backtrack
                    }
                }
            }

            if let Some(sp) = star_pi {
                // Expand `*` to consume one more text byte and retry.
                pi = sp + 1;
                star_ti += 1;
                ti = star_ti;
            } else {
                return false;
            }
        }

        // Text exhausted: remaining pattern may only be stars (and empty classes not needed).
        while pi < pattern.len() && pattern[pi] == b'*' {
            pi += 1;
        }
        pi == pattern.len()
    }

    fn match_char_class(pattern: &[u8], start: usize, ch: u8) -> (bool, usize) {
        let mut idx = start + 1;
        let mut negated = false;
        let mut matched = false;

        if idx < pattern.len() && pattern[idx] == b'^' {
            negated = true;
            idx += 1;
        }

        // Empty / unclosed class: treat as non-match, advance past what we scanned.
        if idx >= pattern.len() {
            return (false, idx);
        }

        while idx < pattern.len() && pattern[idx] != b']' {
            if idx + 2 < pattern.len() && pattern[idx + 1] == b'-' && pattern[idx + 2] != b']' {
                // Range: a-z
                if ch >= pattern[idx] && ch <= pattern[idx + 2] {
                    matched = true;
                }
                idx += 3;
            } else {
                // Single character
                if pattern[idx] == ch {
                    matched = true;
                }
                idx += 1;
            }
        }

        if idx < pattern.len() && pattern[idx] == b']' {
            idx += 1; // Skip closing ]
        } else {
            // Unclosed '[' — not a valid class match
            return (false, start + 1);
        }

        if negated {
            matched = !matched;
        }

        (matched, idx)
    }
}

/// Subscriber information
#[derive(Debug, Clone)]
pub struct Subscriber {
    pub client_id: ClientId,
    pub sender: broadcast::Sender<RespValue>,
}

/// Pub/Sub system for managing channels and pattern subscriptions
pub struct PubSub {
    /// Map of channel names to their subscribers
    channels: Arc<RwLock<HashMap<Bytes, HashSet<ClientId>>>>,
    
    /// Map of patterns to their subscribers
    patterns: Arc<RwLock<HashMap<Bytes, HashSet<ClientId>>>>,
    
    /// Map of client IDs to their broadcast senders
    clients: Arc<RwLock<HashMap<ClientId, broadcast::Sender<RespValue>>>>,
    
    /// Map of client IDs to their subscribed channels
    client_channels: Arc<RwLock<HashMap<ClientId, HashSet<Bytes>>>>,
    
    /// Map of client IDs to their subscribed patterns
    client_patterns: Arc<RwLock<HashMap<ClientId, HashSet<Bytes>>>>,

    /// Map of shard channel names to their subscribers (Redis 7.0+ Shard Pub/Sub)
    shard_channels: Arc<RwLock<HashMap<Bytes, HashSet<ClientId>>>>,

    /// Map of client IDs to their subscribed shard channels
    client_shard_channels: Arc<RwLock<HashMap<ClientId, HashSet<Bytes>>>>,

    /// Per-client FIFO of pending message sizes still buffered for delivery.
    /// Mirrors broadcast buffer occupancy for fan-out memory accounting.
    pending_by_client: Arc<RwLock<HashMap<ClientId, VecDeque<usize>>>>,
    
    /// Next client ID to assign
    next_client_id: Arc<RwLock<ClientId>>,

    /// Per-client broadcast channel capacity (messages). Applied at `register_client`.
    client_buffer_capacity: AtomicUsize,

    /// Messages dropped due to full buffers (overwrites) or failed sends.
    messages_dropped: AtomicU64,

    /// Negotiated RESP protocol version per client (2 default; 3 after HELLO 3).
    /// Used so fan-out can emit RESP3 push frames for RESP3 clients.
    client_protocol: Arc<RwLock<HashMap<ClientId, u8>>>,
}

impl PubSub {
    /// Create a new PubSub instance with the default client buffer capacity.
    pub fn new() -> Arc<Self> {
        Self::with_client_buffer_capacity(DEFAULT_CLIENT_BUFFER_CAPACITY)
    }

    /// Create a PubSub instance with a custom per-client broadcast buffer capacity.
    /// Capacity is clamped to at least 1 (tokio broadcast requirement).
    pub fn with_client_buffer_capacity(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
            client_channels: Arc::new(RwLock::new(HashMap::new())),
            client_patterns: Arc::new(RwLock::new(HashMap::new())),
            shard_channels: Arc::new(RwLock::new(HashMap::new())),
            client_shard_channels: Arc::new(RwLock::new(HashMap::new())),
            pending_by_client: Arc::new(RwLock::new(HashMap::new())),
            next_client_id: Arc::new(RwLock::new(0)),
            client_buffer_capacity: AtomicUsize::new(capacity.max(1)),
            messages_dropped: AtomicU64::new(0),
            client_protocol: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Record the negotiated RESP protocol version for a client (2 or 3).
    pub async fn set_client_protocol(&self, client_id: ClientId, version: u8) {
        let v = if version >= 3 { 3 } else { 2 };
        self.client_protocol.write().await.insert(client_id, v);
    }

    /// Build a pub/sub delivery frame (array for RESP2, push for RESP3).
    fn pubsub_frame(proto: u8, elements: Vec<RespValue>) -> RespValue {
        if proto >= 3 {
            RespValue::Push(elements)
        } else {
            RespValue::Array(elements)
        }
    }

    /// Current per-client broadcast buffer capacity (messages).
    pub fn client_buffer_capacity(&self) -> usize {
        self.client_buffer_capacity.load(Ordering::Relaxed).max(1)
    }

    /// Set per-client broadcast buffer capacity used for subsequent `register_client` calls.
    /// Existing clients keep the capacity they were registered with.
    pub fn set_client_buffer_capacity(&self, capacity: usize) {
        self.client_buffer_capacity
            .store(capacity.max(1), Ordering::Relaxed);
    }

    /// Total messages dropped (full-buffer overwrites + failed sends) since creation / reset.
    pub fn messages_dropped(&self) -> u64 {
        self.messages_dropped.load(Ordering::Relaxed)
    }

    /// Total bytes currently tracked as pending in client pub/sub buffers.
    pub async fn pending_memory(&self) -> usize {
        self.pending_by_client
            .read()
            .await
            .values()
            .map(|q| q.iter().sum::<usize>())
            .sum()
    }

    /// Pending buffered bytes for a single client.
    pub async fn client_pending_memory(&self, client_id: ClientId) -> usize {
        self.pending_by_client
            .read()
            .await
            .get(&client_id)
            .map(|q| q.iter().sum())
            .unwrap_or(0)
    }

    /// Estimate how many deliveries a publish to `channel` would perform
    /// (channel subscribers + matching pattern subscribers; dual subs count twice).
    pub async fn estimate_delivery_count(&self, channel: &Bytes) -> usize {
        let mut count = 0usize;

        if let Some(subscribers) = self.channels.read().await.get(channel) {
            count = count.saturating_add(subscribers.len());
        }

        let patterns = self.patterns.read().await;
        for (pattern, subscribers) in patterns.iter() {
            if PatternMatcher::matches(pattern, channel) {
                count = count.saturating_add(subscribers.len());
            }
        }

        count
    }

    /// Fan-out memory cost for admission control: `message_size * max(1, delivery_count)`.
    pub async fn fanout_memory_cost(&self, channel: &Bytes, message_size: usize) -> usize {
        let deliveries = self.estimate_delivery_count(channel).await;
        message_size.saturating_mul(deliveries.max(1))
    }

    /// Record that the network path successfully took one buffered message for `client_id`.
    /// Returns the message size to deallocate from the memory tracker.
    pub async fn note_delivered(&self, client_id: ClientId) -> usize {
        let mut pending = self.pending_by_client.write().await;
        if let Some(q) = pending.get_mut(&client_id) {
            let size = q.pop_front().unwrap_or(0);
            if q.is_empty() {
                pending.remove(&client_id);
            }
            size
        } else {
            0
        }
    }

    /// Record that a receiver lagged and lost `n` messages. Returns bytes to deallocate.
    pub async fn note_lagged(&self, client_id: ClientId, n: u64) -> usize {
        if n == 0 {
            return 0;
        }
        let mut pending = self.pending_by_client.write().await;
        let mut freed = 0usize;
        if let Some(q) = pending.get_mut(&client_id) {
            let drop_n = (n as usize).min(q.len());
            for _ in 0..drop_n {
                freed = freed.saturating_add(q.pop_front().unwrap_or(0));
            }
            if q.is_empty() {
                pending.remove(&client_id);
            }
        }
        // Drops were already counted at publish-time overwrite; only free accounting here.
        freed
    }

    /// Register a new client and return its ID and receiver
    pub async fn register_client(&self) -> (ClientId, broadcast::Receiver<RespValue>) {
        let mut next_id = self.next_client_id.write().await;
        let client_id = *next_id;
        *next_id += 1;
        drop(next_id);

        // Create a broadcast channel for this client with configured capacity
        let capacity = self.client_buffer_capacity();
        let (tx, rx) = broadcast::channel(capacity);
        
        let mut clients = self.clients.write().await;
        clients.insert(client_id, tx);
        self.client_protocol.write().await.insert(client_id, 2);
        
        (client_id, rx)
    }

    /// Unregister a client.
    ///
    /// Returns the total pending buffer bytes that were still accounted for this client
    /// (caller should release that amount from the Pub/Sub memory tracker).
    pub async fn unregister_client(&self, client_id: ClientId) -> usize {
        // Remove from all channels - release client_channels lock before acquiring channels lock
        // to maintain consistent lock ordering (channels → client_channels) and prevent deadlock.
        let channels_to_remove = {
            let mut client_channels = self.client_channels.write().await;
            client_channels.remove(&client_id).unwrap_or_default()
        };
        {
            let mut channel_map = self.channels.write().await;
            for channel in &channels_to_remove {
                if let Some(subscribers) = channel_map.get_mut(channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        channel_map.remove(channel);
                    }
                }
            }
        }

        // Remove from all patterns - same lock-order fix
        let patterns_to_remove = {
            let mut client_patterns = self.client_patterns.write().await;
            client_patterns.remove(&client_id).unwrap_or_default()
        };
        {
            let mut pattern_map = self.patterns.write().await;
            for pattern in &patterns_to_remove {
                if let Some(subscribers) = pattern_map.get_mut(pattern) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        pattern_map.remove(pattern);
                    }
                }
            }
        }

        // Remove from all shard channels - same lock-order discipline
        let shard_channels_to_remove = {
            let mut client_shard_channels = self.client_shard_channels.write().await;
            client_shard_channels.remove(&client_id).unwrap_or_default()
        };
        {
            let mut shard_channel_map = self.shard_channels.write().await;
            for ch in &shard_channels_to_remove {
                if let Some(subscribers) = shard_channel_map.get_mut(ch) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        shard_channel_map.remove(ch);
                    }
                }
            }
        }

        // Remove client sender + protocol version
        self.clients.write().await.remove(&client_id);
        self.client_protocol.write().await.remove(&client_id);

        // Take remaining pending buffer accounting for this client
        let pending = self
            .pending_by_client
            .write()
            .await
            .remove(&client_id)
            .map(|q| q.into_iter().sum())
            .unwrap_or(0);
        pending
    }

    /// Subscribe a client to a channel
    pub async fn subscribe(&self, client_id: ClientId, channel: Bytes) -> usize {
        // Add to channels map
        let mut channels = self.channels.write().await;
        channels.entry(channel.clone())
            .or_insert_with(HashSet::new)
            .insert(client_id);
        drop(channels);

        // Add to client_channels map.
        // Release client_channels lock before acquiring client_patterns lock to maintain
        // consistent lock ordering (client_channels → client_patterns) and prevent deadlock
        // with concurrent psubscribe (which holds client_patterns then reads client_channels).
        let channel_count = {
            let mut client_channels = self.client_channels.write().await;
            client_channels.entry(client_id)
                .or_insert_with(HashSet::new)
                .insert(channel);
            client_channels.get(&client_id).map(|s| s.len()).unwrap_or(0)
        };

        // client_channels lock is now released; safe to acquire client_patterns
        let pattern_count = self.client_patterns.read().await
            .get(&client_id).map(|s| s.len()).unwrap_or(0);

        channel_count + pattern_count
    }

    /// Unsubscribe a client from a channel
    pub async fn unsubscribe(&self, client_id: ClientId, channel: &Bytes) -> usize {
        // Remove from channels map
        let mut channels = self.channels.write().await;
        if let Some(subscribers) = channels.get_mut(channel) {
            subscribers.remove(&client_id);
            if subscribers.is_empty() {
                channels.remove(channel);
            }
        }
        drop(channels);

        // Remove from client_channels map.
        // Release client_channels lock before acquiring client_patterns lock (CR-1 fix).
        let channel_count = {
            let mut client_channels = self.client_channels.write().await;
            if let Some(client_chans) = client_channels.get_mut(&client_id) {
                client_chans.remove(channel);
            }
            client_channels.get(&client_id).map(|s| s.len()).unwrap_or(0)
        };

        let pattern_count = self.client_patterns.read().await
            .get(&client_id).map(|s| s.len()).unwrap_or(0);

        channel_count + pattern_count
    }

    /// Unsubscribe a client from all channels
    pub async fn unsubscribe_all(&self, client_id: ClientId) -> Vec<Bytes> {
        // Release client_channels lock before acquiring channels lock to prevent deadlock.
        let channels_to_remove = {
            let mut client_channels = self.client_channels.write().await;
            client_channels.remove(&client_id).unwrap_or_default()
        };

        {
            let mut channels = self.channels.write().await;
            for channel in &channels_to_remove {
                if let Some(subscribers) = channels.get_mut(channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        channels.remove(channel);
                    }
                }
            }
        }

        channels_to_remove.into_iter().collect()
    }

    /// Subscribe a client to a pattern
    pub async fn psubscribe(&self, client_id: ClientId, pattern: Bytes) -> usize {
        // Add to patterns map
        let mut patterns = self.patterns.write().await;
        patterns.entry(pattern.clone())
            .or_insert_with(HashSet::new)
            .insert(client_id);
        drop(patterns);

        // Add to client_patterns map.
        // Release client_patterns lock before acquiring client_channels lock to maintain
        // consistent lock ordering and prevent deadlock with concurrent subscribe
        // (which holds client_channels then reads client_patterns).
        let pattern_count = {
            let mut client_patterns = self.client_patterns.write().await;
            client_patterns.entry(client_id)
                .or_insert_with(HashSet::new)
                .insert(pattern);
            client_patterns.get(&client_id).map(|s| s.len()).unwrap_or(0)
        };

        // client_patterns lock is now released; safe to acquire client_channels
        let channel_count = self.client_channels.read().await
            .get(&client_id).map(|s| s.len()).unwrap_or(0);

        channel_count + pattern_count
    }

    /// Unsubscribe a client from a pattern
    pub async fn punsubscribe(&self, client_id: ClientId, pattern: &Bytes) -> usize {
        // Remove from patterns map
        let mut patterns = self.patterns.write().await;
        if let Some(subscribers) = patterns.get_mut(pattern) {
            subscribers.remove(&client_id);
            if subscribers.is_empty() {
                patterns.remove(pattern);
            }
        }
        drop(patterns);

        // Remove from client_patterns map.
        // Release client_patterns lock before acquiring client_channels lock (CR-1 fix).
        let pattern_count = {
            let mut client_patterns = self.client_patterns.write().await;
            if let Some(client_pats) = client_patterns.get_mut(&client_id) {
                client_pats.remove(pattern);
            }
            client_patterns.get(&client_id).map(|s| s.len()).unwrap_or(0)
        };

        let channel_count = self.client_channels.read().await
            .get(&client_id).map(|s| s.len()).unwrap_or(0);

        channel_count + pattern_count
    }

    /// Unsubscribe a client from all patterns
    pub async fn punsubscribe_all(&self, client_id: ClientId) -> Vec<Bytes> {
        // Release client_patterns lock before acquiring patterns lock to prevent deadlock.
        let patterns_to_remove = {
            let mut client_patterns = self.client_patterns.write().await;
            client_patterns.remove(&client_id).unwrap_or_default()
        };

        {
            let mut patterns = self.patterns.write().await;
            for pattern in &patterns_to_remove {
                if let Some(subscribers) = patterns.get_mut(pattern) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        patterns.remove(pattern);
                    }
                }
            }
        }

        patterns_to_remove.into_iter().collect()
    }

    /// Subscribe a client to a shard channel (Redis 7.0+ Shard Pub/Sub)
    /// SSUBSCRIBE shardchannel [shardchannel ...]
    pub async fn ssubscribe(&self, client_id: ClientId, channel: Bytes) -> usize {
        let mut shard_channels = self.shard_channels.write().await;
        shard_channels.entry(channel.clone())
            .or_insert_with(HashSet::new)
            .insert(client_id);
        drop(shard_channels);

        let mut client_shard_channels = self.client_shard_channels.write().await;
        client_shard_channels.entry(client_id)
            .or_insert_with(HashSet::new)
            .insert(channel);

        client_shard_channels.get(&client_id).map(|s| s.len()).unwrap_or(0)
    }

    /// Unsubscribe a client from a shard channel
    pub async fn sunsubscribe(&self, client_id: ClientId, channel: &Bytes) -> usize {
        let mut shard_channels = self.shard_channels.write().await;
        if let Some(subscribers) = shard_channels.get_mut(channel) {
            subscribers.remove(&client_id);
            if subscribers.is_empty() {
                shard_channels.remove(channel);
            }
        }
        drop(shard_channels);

        let mut client_shard_channels = self.client_shard_channels.write().await;
        if let Some(chans) = client_shard_channels.get_mut(&client_id) {
            chans.remove(channel);
        }

        client_shard_channels.get(&client_id).map(|s| s.len()).unwrap_or(0)
    }

    /// Unsubscribe a client from all shard channels
    pub async fn sunsubscribe_all(&self, client_id: ClientId) -> Vec<Bytes> {
        let channels_to_remove = {
            let mut client_shard_channels = self.client_shard_channels.write().await;
            client_shard_channels.remove(&client_id).unwrap_or_default()
        };

        {
            let mut shard_channels = self.shard_channels.write().await;
            for channel in &channels_to_remove {
                if let Some(subscribers) = shard_channels.get_mut(channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        shard_channels.remove(channel);
                    }
                }
            }
        }

        channels_to_remove.into_iter().collect()
    }

    /// Publish a message to a shard channel
    /// Returns number of clients that received the message
    pub async fn spublish(&self, channel: &Bytes, message: &Bytes) -> usize {
        self.spublish_with_outcome(channel, message).await.recipients
    }

    /// Publish to a shard channel with fan-out buffer accounting.
    pub async fn spublish_with_outcome(
        &self,
        channel: &Bytes,
        message: &Bytes,
    ) -> PublishOutcome {
        let message_size = message.len();
        let clients = self.clients.read().await;
        let protos = self.client_protocol.read().await;
        let mut delivered: Vec<ClientId> = Vec::new();
        let mut messages_dropped = 0usize;

        if let Some(subscribers) = self.shard_channels.read().await.get(channel) {
            for &client_id in subscribers {
                if let Some(sender) = clients.get(&client_id) {
                    let proto = protos.get(&client_id).copied().unwrap_or(2);
                    let msg = Self::pubsub_frame(
                        proto,
                        vec![
                            RespValue::BulkString(Some(Bytes::from_static(b"smessage"))),
                            RespValue::BulkString(Some(channel.clone())),
                            RespValue::BulkString(Some(message.clone())),
                        ],
                    );
                    match sender.send(msg) {
                        Ok(_) => delivered.push(client_id),
                        Err(_) => messages_dropped += 1,
                    }
                }
            }
        }
        drop(protos);
        drop(clients);

        self.account_deliveries(&delivered, message_size, messages_dropped)
            .await
    }

    /// Get the number of active shard channels
    pub async fn num_shard_channels(&self) -> usize {
        self.shard_channels.read().await.len()
    }

    /// Get the number of subscribers for a specific shard channel
    pub async fn num_shard_subscribers(&self, channel: &Bytes) -> usize {
        self.shard_channels.read().await
            .get(channel)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// List all active shard channels, optionally filtered by pattern
    pub async fn list_shard_channels(&self, pattern: Option<&Bytes>) -> Vec<Bytes> {
        let shard_channels = self.shard_channels.read().await;
        if let Some(pat) = pattern {
            shard_channels.keys()
                .filter(|ch| PatternMatcher::matches(pat, ch))
                .cloned()
                .collect()
        } else {
            shard_channels.keys().cloned().collect()
        }
    }

    /// Publish a message to a channel.
    /// Returns the number of clients that received the message.
    pub async fn publish(&self, channel: &Bytes, message: &Bytes) -> usize {
        self.publish_with_outcome(channel, message).await.recipients
    }

    /// Publish a message with detailed fan-out / buffer accounting.
    ///
    /// Never panics on a full client buffer: tokio broadcast overwrites the oldest
    /// message, and we mirror that in pending-size tracking (`bytes_overwritten`).
    pub async fn publish_with_outcome(
        &self,
        channel: &Bytes,
        message: &Bytes,
    ) -> PublishOutcome {
        let message_size = message.len();
        let clients = self.clients.read().await;
        let protos = self.client_protocol.read().await;
        let mut delivered: Vec<ClientId> = Vec::new();
        let mut messages_dropped = 0usize;

        // Send to direct channel subscribers
        if let Some(subscribers) = self.channels.read().await.get(channel) {
            for &client_id in subscribers {
                if let Some(sender) = clients.get(&client_id) {
                    let proto = protos.get(&client_id).copied().unwrap_or(2);
                    let msg = Self::pubsub_frame(
                        proto,
                        vec![
                            RespValue::BulkString(Some(Bytes::from_static(b"message"))),
                            RespValue::BulkString(Some(channel.clone())),
                            RespValue::BulkString(Some(message.clone())),
                        ],
                    );
                    // Full buffers do not panic: send overwrites the oldest slot.
                    match sender.send(msg) {
                        Ok(_) => delivered.push(client_id),
                        Err(_) => messages_dropped += 1,
                    }
                }
            }
        }

        // Send to pattern subscribers
        let patterns = self.patterns.read().await;
        for (pattern, subscribers) in patterns.iter() {
            if PatternMatcher::matches(pattern, channel) {
                for &client_id in subscribers {
                    if let Some(sender) = clients.get(&client_id) {
                        let proto = protos.get(&client_id).copied().unwrap_or(2);
                        let msg = Self::pubsub_frame(
                            proto,
                            vec![
                                RespValue::BulkString(Some(Bytes::from_static(b"pmessage"))),
                                RespValue::BulkString(Some(pattern.clone())),
                                RespValue::BulkString(Some(channel.clone())),
                                RespValue::BulkString(Some(message.clone())),
                            ],
                        );
                        match sender.send(msg) {
                            Ok(_) => delivered.push(client_id),
                            Err(_) => messages_dropped += 1,
                        }
                    }
                }
            }
        }
        drop(patterns);
        drop(protos);
        drop(clients);

        self.account_deliveries(&delivered, message_size, messages_dropped)
            .await
    }

    /// Update per-client pending size queues after a fan-out send pass.
    async fn account_deliveries(
        &self,
        delivered: &[ClientId],
        message_size: usize,
        mut messages_dropped: usize,
    ) -> PublishOutcome {
        let capacity = self.client_buffer_capacity();
        let mut bytes_enqueued = 0usize;
        let mut bytes_overwritten = 0usize;

        {
            let mut pending = self.pending_by_client.write().await;
            for &client_id in delivered {
                let q = pending.entry(client_id).or_default();
                // Mirror broadcast ring-buffer: at capacity, oldest is dropped.
                while q.len() >= capacity {
                    if let Some(old) = q.pop_front() {
                        bytes_overwritten = bytes_overwritten.saturating_add(old);
                        messages_dropped = messages_dropped.saturating_add(1);
                    } else {
                        break;
                    }
                }
                q.push_back(message_size);
                bytes_enqueued = bytes_enqueued.saturating_add(message_size);
            }
        }

        if messages_dropped > 0 {
            self.messages_dropped
                .fetch_add(messages_dropped as u64, Ordering::Relaxed);
        }

        PublishOutcome {
            recipients: delivered.len(),
            bytes_enqueued,
            bytes_overwritten,
            messages_dropped,
        }
    }

    /// Get the number of active channels (channels with at least one subscriber)
    pub async fn num_channels(&self) -> usize {
        self.channels.read().await.len()
    }

    /// Get the number of subscriptions for patterns
    pub async fn num_patterns(&self) -> usize {
        self.patterns.read().await.len()
    }

    /// Get the number of subscribers for a specific channel
    pub async fn num_subscribers(&self, channel: &Bytes) -> usize {
        self.channels.read().await
            .get(channel)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// List all active channels, optionally filtered by pattern
    pub async fn list_channels(&self, pattern: Option<&Bytes>) -> Vec<Bytes> {
        let channels = self.channels.read().await;
        if let Some(pat) = pattern {
            channels.keys()
                .filter(|ch| PatternMatcher::matches(pat, ch))
                .cloned()
                .collect()
        } else {
            channels.keys().cloned().collect()
        }
    }

    /// Get subscription count for a client
    pub async fn client_subscription_count(&self, client_id: ClientId) -> (usize, usize) {
        let channel_count = self.client_channels.read().await
            .get(&client_id)
            .map(|s| s.len())
            .unwrap_or(0);
        let pattern_count = self.client_patterns.read().await
            .get(&client_id)
            .map(|s| s.len())
            .unwrap_or(0);
        (channel_count, pattern_count)
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
            client_channels: Arc::new(RwLock::new(HashMap::new())),
            client_patterns: Arc::new(RwLock::new(HashMap::new())),
            shard_channels: Arc::new(RwLock::new(HashMap::new())),
            client_shard_channels: Arc::new(RwLock::new(HashMap::new())),
            pending_by_client: Arc::new(RwLock::new(HashMap::new())),
            next_client_id: Arc::new(RwLock::new(0)),
            client_buffer_capacity: AtomicUsize::new(DEFAULT_CLIENT_BUFFER_CAPACITY),
            messages_dropped: AtomicU64::new(0),
            client_protocol: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matcher() {
        // Simple wildcard tests
        assert!(PatternMatcher::matches(b"h*llo", b"hello"));
        assert!(PatternMatcher::matches(b"h*llo", b"hllo"));
        assert!(PatternMatcher::matches(b"h*llo", b"heeeello"));
        assert!(!PatternMatcher::matches(b"h*llo", b"helo"));

        // Question mark tests
        assert!(PatternMatcher::matches(b"h?llo", b"hello"));
        assert!(PatternMatcher::matches(b"h?llo", b"hallo"));
        assert!(!PatternMatcher::matches(b"h?llo", b"hllo"));

        // Character class tests
        assert!(PatternMatcher::matches(b"h[ae]llo", b"hello"));
        assert!(PatternMatcher::matches(b"h[ae]llo", b"hallo"));
        assert!(!PatternMatcher::matches(b"h[ae]llo", b"hillo"));

        // Range tests
        assert!(PatternMatcher::matches(b"h[a-z]llo", b"hello"));
        assert!(!PatternMatcher::matches(b"h[a-z]llo", b"h1llo"));

        // Negated class
        assert!(PatternMatcher::matches(b"h[^e]llo", b"hallo"));
        assert!(!PatternMatcher::matches(b"h[^e]llo", b"hello"));

        // Escape
        assert!(PatternMatcher::matches(b"h\\*llo", b"h*llo"));
        assert!(!PatternMatcher::matches(b"h\\*llo", b"hello"));

        // Complex patterns
        assert!(PatternMatcher::matches(b"news.*", b"news.tech"));
        assert!(PatternMatcher::matches(b"news.*", b"news.sports.football"));
        assert!(!PatternMatcher::matches(b"news.*", b"tech.news"));
    }

    #[test]
    fn pattern_matcher_pathological_stars_no_stack_overflow() {
        // Recursive matchers blow the stack on patterns like a*a*a*… vs long text.
        let mut pat = vec![b'*'; 64];
        pat.push(b'x');
        let text = vec![b'a'; 10_000];
        assert!(!PatternMatcher::matches(&pat, &text));
        // Many stars that can still match
        let pat2 = b"********************************";
        let text2 = b"anything-goes-here-with-length";
        assert!(PatternMatcher::matches(pat2, text2));
    }

    #[tokio::test]
    async fn publish_uses_push_for_resp3_clients() {
        let pubsub = PubSub::new();
        let (c2, mut rx2) = pubsub.register_client().await;
        let (c3, mut rx3) = pubsub.register_client().await;
        pubsub.set_client_protocol(c3, 3).await;

        let ch = Bytes::from("ch");
        pubsub.subscribe(c2, ch.clone()).await;
        pubsub.subscribe(c3, ch.clone()).await;
        pubsub.publish(&ch, &Bytes::from("m")).await;

        let m2 = rx2.recv().await.unwrap();
        let m3 = rx3.recv().await.unwrap();
        assert!(matches!(m2, RespValue::Array(_)), "resp2 got {:?}", m2);
        assert!(matches!(m3, RespValue::Push(_)), "resp3 got {:?}", m3);
    }

    #[tokio::test]
    async fn test_pubsub_subscribe_publish() {
        let pubsub = PubSub::new();
        let (client_id, mut rx) = pubsub.register_client().await;

        let channel = Bytes::from("test-channel");
        let count = pubsub.subscribe(client_id, channel.clone()).await;
        assert_eq!(count, 1);

        let message = Bytes::from("hello world");
        let recipients = pubsub.publish(&channel, &message).await;
        assert_eq!(recipients, 1);

        let received = rx.recv().await.unwrap();
        if let RespValue::Array(arr) = received {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0].as_bulk_string().unwrap(), &Bytes::from("message"));
            assert_eq!(arr[1].as_bulk_string().unwrap(), &Bytes::from("test-channel"));
            assert_eq!(arr[2].as_bulk_string().unwrap(), &Bytes::from("hello world"));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_pubsub_pattern_subscribe() {
        let pubsub = PubSub::new();
        let (client_id, mut rx) = pubsub.register_client().await;

        let pattern = Bytes::from("news.*");
        let count = pubsub.psubscribe(client_id, pattern).await;
        assert_eq!(count, 1);

        let channel = Bytes::from("news.tech");
        let message = Bytes::from("new article");
        let recipients = pubsub.publish(&channel, &message).await;
        assert_eq!(recipients, 1);

        let received = rx.recv().await.unwrap();
        if let RespValue::Array(arr) = received {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0].as_bulk_string().unwrap(), &Bytes::from("pmessage"));
            assert_eq!(arr[1].as_bulk_string().unwrap(), &Bytes::from("news.*"));
            assert_eq!(arr[2].as_bulk_string().unwrap(), &Bytes::from("news.tech"));
            assert_eq!(arr[3].as_bulk_string().unwrap(), &Bytes::from("new article"));
        } else {
            panic!("Expected array");
        }
    }
}
