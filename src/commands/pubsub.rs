use crate::cache::Cache;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::pubsub::ClientId;
use crate::memory::MemoryCategory;
use bytes::Bytes;

impl Cache {
    /// Handle PUBLISH command
    /// PUBLISH channel message
    /// Returns the number of clients that received the message
    pub async fn cmd_publish(&self, args: &[RespValue]) -> Result<RespValue> {
        self.stats.incr(&self.stats.cmd_publish);
        
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'publish' command"));
        }

        let channel = match args[0].as_bulk_string() {
            Some(s) => Bytes::from(s.to_vec()),
            None => return Ok(RespValue::error("ERR invalid channel")),
        };

        let message = match args[1].as_bulk_string() {
            Some(s) => Bytes::from(s.to_vec()),
            None => return Ok(RespValue::error("ERR invalid message")),
        };

        // Per-message size cap (independent of fan-out).
        let message_size = message.len();
        let max_message = self.memory_tracker.max_message_size();
        if message_size > max_message {
            return Err(Error::MessageTooLarge {
                size: message_size,
                max: max_message,
            });
        }

        // Fan-out admission: cost ≈ message_size * max(1, subscriber deliveries).
        // Pending buffer memory stays allocated until clients receive or disconnect.
        let fanout_cost = self.pubsub.fanout_memory_cost(&channel, message_size).await;
        if !self
            .memory_tracker
            .allocate(fanout_cost, MemoryCategory::PubSub)
        {
            return Err(Error::OutOfMemory);
        }

        let outcome = self.pubsub.publish_with_outcome(&channel, &message).await;
        self.stats
            .pubsub_messages_sent
            .fetch_add(outcome.recipients as u64, std::sync::atomic::Ordering::Relaxed);

        // Adjust for unused admission headroom and buffer overwrites of older messages.
        // - Pre-allocated `fanout_cost` for this publish's new enqueues.
        // - Free any unused portion when fewer bytes were enqueued (e.g. 0 subscribers).
        // - Free overwritten older pending bytes (accounted by prior publishes).
        if fanout_cost > outcome.bytes_enqueued {
            self.memory_tracker.deallocate(
                fanout_cost - outcome.bytes_enqueued,
                MemoryCategory::PubSub,
            );
        }
        if outcome.bytes_overwritten > 0 {
            self.memory_tracker
                .deallocate(outcome.bytes_overwritten, MemoryCategory::PubSub);
        }

        Ok(RespValue::Integer(outcome.recipients as i64))
    }

    /// Handle SUBSCRIBE command
    /// SUBSCRIBE channel [channel ...]
    /// Returns subscription confirmations
    pub async fn cmd_subscribe(&self, client_id: ClientId, args: &[RespValue]) -> Result<Vec<RespValue>> {
        self.stats.incr(&self.stats.cmd_subscribe);
        
        if args.is_empty() {
            return Ok(vec![RespValue::error("ERR wrong number of arguments for 'subscribe' command")]);
        }

        let mut responses = Vec::new();

        for arg in args {
            let channel = match arg.as_bulk_string() {
                Some(s) => Bytes::from(s.to_vec()),
                None => {
                    responses.push(RespValue::error("ERR invalid channel"));
                    continue;
                }
            };

            let count = self.pubsub.subscribe(client_id, channel.clone()).await;
            
            // Return subscription confirmation
            responses.push(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"subscribe"))),
                RespValue::BulkString(Some(channel)),
                RespValue::Integer(count as i64),
            ]));
        }

        Ok(responses)
    }

    /// Handle UNSUBSCRIBE command
    /// UNSUBSCRIBE [channel [channel ...]]
    /// If no channels specified, unsubscribe from all channels
    pub async fn cmd_unsubscribe(&self, client_id: ClientId, args: &[RespValue]) -> Result<Vec<RespValue>> {
        self.stats.incr(&self.stats.cmd_unsubscribe);
        
        let mut responses = Vec::new();

        if args.is_empty() {
            // Unsubscribe from all channels
            let channels = self.pubsub.unsubscribe_all(client_id).await;
            if channels.is_empty() {
                // No channels to unsubscribe from
                let (_, pattern_count) = self.pubsub.client_subscription_count(client_id).await;
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"unsubscribe"))),
                    RespValue::null(),
                    RespValue::Integer(pattern_count as i64),
                ]));
            } else {
                for channel in channels {
                    let count = self.pubsub.client_subscription_count(client_id).await;
                    responses.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"unsubscribe"))),
                        RespValue::BulkString(Some(channel)),
                        RespValue::Integer((count.0 + count.1) as i64),
                    ]));
                }
            }
        } else {
            for arg in args {
                let channel = match arg.as_bulk_string() {
                    Some(s) => Bytes::from(s.to_vec()),
                    None => {
                        responses.push(RespValue::error("ERR invalid channel"));
                        continue;
                    }
                };

                let count = self.pubsub.unsubscribe(client_id, &channel).await;
                
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"unsubscribe"))),
                    RespValue::BulkString(Some(channel)),
                    RespValue::Integer(count as i64),
                ]));
            }
        }

        Ok(responses)
    }

    /// Handle PSUBSCRIBE command (pattern subscribe)
    /// PSUBSCRIBE pattern [pattern ...]
    pub async fn cmd_psubscribe(&self, client_id: ClientId, args: &[RespValue]) -> Result<Vec<RespValue>> {
        self.stats.incr(&self.stats.cmd_psubscribe);
        
        if args.is_empty() {
            return Ok(vec![RespValue::error("ERR wrong number of arguments for 'psubscribe' command")]);
        }

        let mut responses = Vec::new();

        for arg in args {
            let pattern = match arg.as_bulk_string() {
                Some(s) => Bytes::from(s.to_vec()),
                None => {
                    responses.push(RespValue::error("ERR invalid pattern"));
                    continue;
                }
            };

            let count = self.pubsub.psubscribe(client_id, pattern.clone()).await;
            
            responses.push(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"psubscribe"))),
                RespValue::BulkString(Some(pattern)),
                RespValue::Integer(count as i64),
            ]));
        }

        Ok(responses)
    }

    /// Handle PUNSUBSCRIBE command (pattern unsubscribe)
    /// PUNSUBSCRIBE [pattern [pattern ...]]
    /// If no patterns specified, unsubscribe from all patterns
    pub async fn cmd_punsubscribe(&self, client_id: ClientId, args: &[RespValue]) -> Result<Vec<RespValue>> {
        self.stats.incr(&self.stats.cmd_punsubscribe);
        
        let mut responses = Vec::new();

        if args.is_empty() {
            // Unsubscribe from all patterns
            let patterns = self.pubsub.punsubscribe_all(client_id).await;
            if patterns.is_empty() {
                // No patterns to unsubscribe from
                let (channel_count, _) = self.pubsub.client_subscription_count(client_id).await;
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"punsubscribe"))),
                    RespValue::null(),
                    RespValue::Integer(channel_count as i64),
                ]));
            } else {
                for pattern in patterns {
                    let count = self.pubsub.client_subscription_count(client_id).await;
                    responses.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"punsubscribe"))),
                        RespValue::BulkString(Some(pattern)),
                        RespValue::Integer((count.0 + count.1) as i64),
                    ]));
                }
            }
        } else {
            for arg in args {
                let pattern = match arg.as_bulk_string() {
                    Some(s) => Bytes::from(s.to_vec()),
                    None => {
                        responses.push(RespValue::error("ERR invalid pattern"));
                        continue;
                    }
                };

                let count = self.pubsub.punsubscribe(client_id, &pattern).await;
                
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"punsubscribe"))),
                    RespValue::BulkString(Some(pattern)),
                    RespValue::Integer(count as i64),
                ]));
            }
        }

        Ok(responses)
    }

    /// Handle PUBSUB command
    /// PUBSUB CHANNELS [pattern]
    /// PUBSUB NUMSUB [channel [channel ...]]
    /// PUBSUB NUMPAT
    pub async fn cmd_pubsub(&self, args: &[RespValue]) -> Result<RespValue> {
        self.stats.incr(&self.stats.cmd_pubsub);
        
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'pubsub' command"));
        }

        let subcommand = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR invalid subcommand")),
        };

        match subcommand.as_str() {
            "CHANNELS" => {
                // PUBSUB CHANNELS [pattern]
                let pattern = if args.len() > 1 {
                    match args[1].as_bulk_string() {
                        Some(s) => Some(Bytes::from(s.to_vec())),
                        None => return Ok(RespValue::error("ERR invalid pattern")),
                    }
                } else {
                    None
                };

                let channels = self.pubsub.list_channels(pattern.as_ref()).await;
                let result: Vec<RespValue> = channels
                    .into_iter()
                    .map(|ch| RespValue::BulkString(Some(ch)))
                    .collect();
                Ok(RespValue::Array(result))
            }
            "NUMSUB" => {
                // PUBSUB NUMSUB [channel [channel ...]]
                let mut result = Vec::new();
                
                for arg in &args[1..] {
                    let channel = match arg.as_bulk_string() {
                        Some(s) => Bytes::from(s.to_vec()),
                        None => continue,
                    };

                    let count = self.pubsub.num_subscribers(&channel).await;
                    result.push(RespValue::BulkString(Some(channel)));
                    result.push(RespValue::Integer(count as i64));
                }

                Ok(RespValue::Array(result))
            }
            "NUMPAT" => {
                // PUBSUB NUMPAT
                let count = self.pubsub.num_patterns().await;
                Ok(RespValue::Integer(count as i64))
            }
            "SHARDCHANNELS" => {
                // PUBSUB SHARDCHANNELS [pattern]
                let pattern = if args.len() > 1 {
                    match args[1].as_bulk_string() {
                        Some(s) => Some(Bytes::from(s.to_vec())),
                        None => return Ok(RespValue::error("ERR invalid pattern")),
                    }
                } else {
                    None
                };
                let channels = self.pubsub.list_shard_channels(pattern.as_ref()).await;
                let result: Vec<RespValue> = channels
                    .into_iter()
                    .map(|ch| RespValue::BulkString(Some(ch)))
                    .collect();
                Ok(RespValue::Array(result))
            }
            "SHARDNUMSUB" => {
                // PUBSUB SHARDNUMSUB [shardchannel [shardchannel ...]]
                let mut result = Vec::new();
                for arg in &args[1..] {
                    let channel = match arg.as_bulk_string() {
                        Some(s) => Bytes::from(s.to_vec()),
                        None => continue,
                    };
                    let count = self.pubsub.num_shard_subscribers(&channel).await;
                    result.push(RespValue::BulkString(Some(channel)));
                    result.push(RespValue::Integer(count as i64));
                }
                Ok(RespValue::Array(result))
            }
            "HELP" => Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(
                    b"PUBSUB <subcommand> [<arg> ...]. Subcommands are:",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"CHANNELS [pattern] -- list active channels",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"NUMSUB [channel ...] -- subscriber counts per channel",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"NUMPAT -- number of pattern subscriptions",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"SHARDCHANNELS [pattern] -- list active shard channels",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"SHARDNUMSUB [shardchannel ...] -- shard subscriber counts",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"HELP -- print this help",
                ))),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try PUBSUB HELP.",
                subcommand
            ))),
        }
    }

    /// Handle SSUBSCRIBE command (Redis 7.0+ Shard Pub/Sub)
    /// SSUBSCRIBE shardchannel [shardchannel ...]
    pub async fn cmd_ssubscribe(&self, client_id: ClientId, args: &[RespValue]) -> Result<Vec<RespValue>> {
        if args.is_empty() {
            return Ok(vec![RespValue::error("ERR wrong number of arguments for 'ssubscribe' command")]);
        }

        let mut responses = Vec::new();
        for arg in args {
            let channel = match arg.as_bulk_string() {
                Some(s) => Bytes::from(s.to_vec()),
                None => {
                    responses.push(RespValue::error("ERR invalid channel"));
                    continue;
                }
            };
            let count = self.pubsub.ssubscribe(client_id, channel.clone()).await;
            responses.push(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"ssubscribe"))),
                RespValue::BulkString(Some(channel)),
                RespValue::Integer(count as i64),
            ]));
        }
        Ok(responses)
    }

    /// Handle SUNSUBSCRIBE command (Redis 7.0+ Shard Pub/Sub)
    /// SUNSUBSCRIBE [shardchannel [shardchannel ...]]
    pub async fn cmd_sunsubscribe(&self, client_id: ClientId, args: &[RespValue]) -> Result<Vec<RespValue>> {
        let mut responses = Vec::new();

        if args.is_empty() {
            let channels = self.pubsub.sunsubscribe_all(client_id).await;
            if channels.is_empty() {
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"sunsubscribe"))),
                    RespValue::null(),
                    RespValue::Integer(0),
                ]));
            } else {
                for channel in channels {
                    let count = self.pubsub.num_shard_subscribers(&channel).await;
                    responses.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"sunsubscribe"))),
                        RespValue::BulkString(Some(channel)),
                        RespValue::Integer(count as i64),
                    ]));
                }
            }
        } else {
            for arg in args {
                let channel = match arg.as_bulk_string() {
                    Some(s) => Bytes::from(s.to_vec()),
                    None => {
                        responses.push(RespValue::error("ERR invalid channel"));
                        continue;
                    }
                };
                let count = self.pubsub.sunsubscribe(client_id, &channel).await;
                responses.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"sunsubscribe"))),
                    RespValue::BulkString(Some(channel)),
                    RespValue::Integer(count as i64),
                ]));
            }
        }
        Ok(responses)
    }

    /// Handle SPUBLISH command (Redis 7.0+ Shard Pub/Sub)
    /// SPUBLISH shardchannel message
    pub async fn cmd_spublish(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'spublish' command"));
        }

        let channel = match args[0].as_bulk_string() {
            Some(s) => Bytes::from(s.to_vec()),
            None => return Ok(RespValue::error("ERR invalid channel")),
        };
        let message = match args[1].as_bulk_string() {
            Some(s) => Bytes::from(s.to_vec()),
            None => return Ok(RespValue::error("ERR invalid message")),
        };

        let message_size = message.len();
        let max_message = self.memory_tracker.max_message_size();
        if message_size > max_message {
            return Err(Error::MessageTooLarge {
                size: message_size,
                max: max_message,
            });
        }

        // Approximate shard fan-out from shard subscriber count.
        let shard_subs = self.pubsub.num_shard_subscribers(&channel).await;
        let fanout_cost = message_size.saturating_mul(shard_subs.max(1));
        if !self
            .memory_tracker
            .allocate(fanout_cost, MemoryCategory::PubSub)
        {
            return Err(Error::OutOfMemory);
        }

        let outcome = self.pubsub.spublish_with_outcome(&channel, &message).await;
        if fanout_cost > outcome.bytes_enqueued {
            self.memory_tracker.deallocate(
                fanout_cost - outcome.bytes_enqueued,
                MemoryCategory::PubSub,
            );
        }
        if outcome.bytes_overwritten > 0 {
            self.memory_tracker
                .deallocate(outcome.bytes_overwritten, MemoryCategory::PubSub);
        }

        Ok(RespValue::Integer(outcome.recipients as i64))
    }
}
