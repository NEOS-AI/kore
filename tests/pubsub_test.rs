use bytes::Bytes;
use kore::memory::MemoryCategory;
use kore::protocol::RespValue;
use kore::{Cache, Error, PubSub};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_pubsub_basic() {
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    // Register two clients
    let (client1, mut rx1) = cache.pubsub.register_client().await;
    let (client2, mut rx2) = cache.pubsub.register_client().await;
    
    // Subscribe client1 to a channel
    let channel = bytes::Bytes::from("test-channel");
    let count = cache.pubsub.subscribe(client1, channel.clone()).await;
    assert_eq!(count, 1);
    
    // Subscribe client2 to the same channel
    let count = cache.pubsub.subscribe(client2, channel.clone()).await;
    assert_eq!(count, 1);
    
    // Publish a message
    let message = bytes::Bytes::from("hello world");
    let recipients = cache.pubsub.publish(&channel, &message).await;
    assert_eq!(recipients, 2);
    
    // Both clients should receive the message
    let msg1 = rx1.recv().await.unwrap();
    let msg2 = rx2.recv().await.unwrap();
    
    // Verify message format
    if let kore::protocol::RespValue::Array(arr) = msg1 {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_bulk_string().unwrap(), &bytes::Bytes::from("message"));
        assert_eq!(arr[1].as_bulk_string().unwrap(), &bytes::Bytes::from("test-channel"));
        assert_eq!(arr[2].as_bulk_string().unwrap(), &bytes::Bytes::from("hello world"));
    } else {
        panic!("Expected array");
    }
    
    if let kore::protocol::RespValue::Array(arr) = msg2 {
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_bulk_string().unwrap(), &bytes::Bytes::from("message"));
    } else {
        panic!("Expected array");
    }
}

#[tokio::test]
async fn test_pubsub_pattern_subscribe() {
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (client_id, mut rx) = cache.pubsub.register_client().await;
    
    // Subscribe to pattern
    let pattern = bytes::Bytes::from("news.*");
    let count = cache.pubsub.psubscribe(client_id, pattern).await;
    assert_eq!(count, 1);
    
    // Publish to matching channel
    let channel = bytes::Bytes::from("news.tech");
    let message = bytes::Bytes::from("breaking news");
    let recipients = cache.pubsub.publish(&channel, &message).await;
    assert_eq!(recipients, 1);
    
    // Client should receive pmessage
    let msg = rx.recv().await.unwrap();
    if let kore::protocol::RespValue::Array(arr) = msg {
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0].as_bulk_string().unwrap(), &bytes::Bytes::from("pmessage"));
        assert_eq!(arr[1].as_bulk_string().unwrap(), &bytes::Bytes::from("news.*"));
        assert_eq!(arr[2].as_bulk_string().unwrap(), &bytes::Bytes::from("news.tech"));
        assert_eq!(arr[3].as_bulk_string().unwrap(), &bytes::Bytes::from("breaking news"));
    } else {
        panic!("Expected array");
    }
}

#[tokio::test]
async fn test_pubsub_unsubscribe() {
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (client_id, mut rx) = cache.pubsub.register_client().await;
    
    // Subscribe to channel
    let channel = bytes::Bytes::from("test-channel");
    let count = cache.pubsub.subscribe(client_id, channel.clone()).await;
    assert_eq!(count, 1);
    
    // Unsubscribe
    let count = cache.pubsub.unsubscribe(client_id, &channel).await;
    assert_eq!(count, 0);
    
    // Publish should not reach the client
    let message = bytes::Bytes::from("test");
    let recipients = cache.pubsub.publish(&channel, &message).await;
    assert_eq!(recipients, 0);
    
    // Client should not receive any message
    tokio::select! {
        _ = rx.recv() => panic!("Should not receive message"),
        _ = sleep(Duration::from_millis(100)) => {}
    }
}

#[tokio::test]
async fn test_pubsub_multiple_channels() {
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (client_id, mut rx) = cache.pubsub.register_client().await;
    
    // Subscribe to multiple channels
    let channel1 = bytes::Bytes::from("channel1");
    let channel2 = bytes::Bytes::from("channel2");
    
    cache.pubsub.subscribe(client_id, channel1.clone()).await;
    let count = cache.pubsub.subscribe(client_id, channel2.clone()).await;
    assert_eq!(count, 2);
    
    // Publish to both channels
    let msg1 = bytes::Bytes::from("message1");
    let msg2 = bytes::Bytes::from("message2");
    
    cache.pubsub.publish(&channel1, &msg1).await;
    cache.pubsub.publish(&channel2, &msg2).await;
    
    // Receive both messages
    let received1 = rx.recv().await.unwrap();
    let received2 = rx.recv().await.unwrap();
    
    // Verify we got messages from both channels
    let channels: Vec<_> = vec![received1, received2]
        .iter()
        .filter_map(|msg| {
            if let kore::protocol::RespValue::Array(arr) = msg {
                arr.get(1)?.as_bulk_string().map(|s| s.to_vec())
            } else {
                None
            }
        })
        .collect();
    
    assert!(channels.contains(&b"channel1".to_vec()));
    assert!(channels.contains(&b"channel2".to_vec()));
}

#[tokio::test]
async fn test_pubsub_stats() {
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (client1, _rx1) = cache.pubsub.register_client().await;
    let (client2, _rx2) = cache.pubsub.register_client().await;
    
    let channel = bytes::Bytes::from("stats-channel");
    
    cache.pubsub.subscribe(client1, channel.clone()).await;
    cache.pubsub.subscribe(client2, channel.clone()).await;
    
    // Check stats
    assert_eq!(cache.pubsub.num_channels().await, 1);
    assert_eq!(cache.pubsub.num_subscribers(&channel).await, 2);
    
    // Add pattern subscription
    let pattern = bytes::Bytes::from("test.*");
    cache.pubsub.psubscribe(client1, pattern).await;
    
    assert_eq!(cache.pubsub.num_patterns().await, 1);
}

#[tokio::test]
async fn test_pubsub_list_channels() {
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (client_id, _rx) = cache.pubsub.register_client().await;
    
    // Subscribe to multiple channels
    cache.pubsub.subscribe(client_id, bytes::Bytes::from("news.tech")).await;
    cache.pubsub.subscribe(client_id, bytes::Bytes::from("news.sports")).await;
    cache.pubsub.subscribe(client_id, bytes::Bytes::from("weather")).await;
    
    // List all channels
    let all_channels = cache.pubsub.list_channels(None).await;
    assert_eq!(all_channels.len(), 3);
    
    // List channels with pattern
    let pattern = bytes::Bytes::from("news.*");
    let news_channels = cache.pubsub.list_channels(Some(&pattern)).await;
    assert_eq!(news_channels.len(), 2);
}

#[tokio::test]
async fn test_pubsub_client_cleanup() {
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (client_id, _rx) = cache.pubsub.register_client().await;
    
    let channel = bytes::Bytes::from("cleanup-test");
    cache.pubsub.subscribe(client_id, channel.clone()).await;
    
    assert_eq!(cache.pubsub.num_channels().await, 1);
    assert_eq!(cache.pubsub.num_subscribers(&channel).await, 1);
    
    // Unregister client
    cache.unregister_pubsub_client(client_id).await;
    
    // Channel should be removed
    assert_eq!(cache.pubsub.num_channels().await, 0);
    assert_eq!(cache.pubsub.num_subscribers(&channel).await, 0);
}

fn bulk_bytes(b: Bytes) -> RespValue {
    RespValue::BulkString(Some(b))
}

/// Fan-out memory after PUBLISH should scale with subscriber count.
#[tokio::test]
async fn test_publish_memory_scales_with_subscribers() {
    let cache = Cache::new(16, 1024 * 1024);
    let channel = Bytes::from("fanout-scale");
    let payload = Bytes::from(vec![b'x'; 64]);

    let mut receivers = Vec::new();
    for n in [1usize, 4, 8] {
        // Fresh subscribers only for this iteration
        while let Some((id, _)) = receivers.pop() {
            cache.unregister_pubsub_client(id).await;
        }
        for _ in 0..n {
            let (id, rx) = cache.pubsub.register_client().await;
            cache.pubsub.subscribe(id, channel.clone()).await;
            receivers.push((id, rx));
        }

        let before = cache.category_memory(MemoryCategory::PubSub);
        let args = [bulk_bytes(channel.clone()), bulk_bytes(payload.clone())];
        let resp = cache.cmd_publish(&args).await.unwrap();
        assert_eq!(resp, RespValue::Integer(n as i64));

        let after = cache.category_memory(MemoryCategory::PubSub);
        assert_eq!(
            after - before,
            payload.len() * n,
            "pending pubsub memory should be message_size * subscribers ({n})"
        );
        assert_eq!(cache.pubsub.pending_memory().await, payload.len() * n);
    }

    for (id, _) in receivers {
        cache.unregister_pubsub_client(id).await;
    }
    assert_eq!(cache.category_memory(MemoryCategory::PubSub), 0);
}

/// PUBLISH is rejected with OOM when fan-out cost would exceed maxmemory.
#[tokio::test]
async fn test_publish_rejected_when_maxmemory_tight() {
    // 300 bytes maxmemory; 4 subscribers * 100-byte message = 400 → OOM.
    // 2 subscribers * 100 = 200 → succeeds.
    let cache = Cache::new(16, 300);
    let channel = Bytes::from("oom-ch");
    let payload = Bytes::from(vec![b'y'; 100]);

    let mut held = Vec::new();
    for _ in 0..4 {
        let (id, rx) = cache.pubsub.register_client().await;
        cache.pubsub.subscribe(id, channel.clone()).await;
        held.push((id, rx));
    }

    let args = [bulk_bytes(channel.clone()), bulk_bytes(payload.clone())];
    let err = cache
        .cmd_publish(&args)
        .await
        .expect_err("fan-out 4*100 exceeds maxmemory 300");
    assert!(
        matches!(err, Error::OutOfMemory),
        "expected OutOfMemory, got {err:?}"
    );
    assert_eq!(
        cache.category_memory(MemoryCategory::PubSub),
        0,
        "failed publish must not leave pubsub memory allocated"
    );

    // With fewer subscribers it should succeed
    while held.len() > 2 {
        let (id, _) = held.pop().unwrap();
        cache.unregister_pubsub_client(id).await;
    }
    let resp = cache.cmd_publish(&args).await.unwrap();
    assert_eq!(resp, RespValue::Integer(2));
    assert_eq!(cache.category_memory(MemoryCategory::PubSub), 200);

    for (id, _) in held {
        cache.unregister_pubsub_client(id).await;
    }
    assert_eq!(cache.category_memory(MemoryCategory::PubSub), 0);
}

/// Fan-out accounting is released on deliver and on unregister (no leak).
#[tokio::test]
async fn test_publish_fanout_does_not_leak_accounting() {
    let cache = Cache::new(16, 1024 * 1024);
    let channel = Bytes::from("noleak");
    let payload = Bytes::from(vec![b'z'; 50]);

    let (id1, mut rx1) = cache.pubsub.register_client().await;
    let (id2, _rx2) = cache.pubsub.register_client().await;
    cache.pubsub.subscribe(id1, channel.clone()).await;
    cache.pubsub.subscribe(id2, channel.clone()).await;

    let args = [bulk_bytes(channel.clone()), bulk_bytes(payload.clone())];
    cache.cmd_publish(&args).await.unwrap();
    assert_eq!(cache.category_memory(MemoryCategory::PubSub), 100);
    assert_eq!(cache.pubsub.pending_memory().await, 100);

    // Deliver to client1
    let _msg = rx1.recv().await.unwrap();
    cache.note_pubsub_delivered(id1).await;
    assert_eq!(cache.category_memory(MemoryCategory::PubSub), 50);
    assert_eq!(cache.pubsub.client_pending_memory(id1).await, 0);
    assert_eq!(cache.pubsub.client_pending_memory(id2).await, 50);

    // Client2 disconnects without reading — remaining pending must be freed
    cache.unregister_pubsub_client(id2).await;
    assert_eq!(cache.category_memory(MemoryCategory::PubSub), 0);
    assert_eq!(cache.pubsub.pending_memory().await, 0);

    cache.unregister_pubsub_client(id1).await;
    assert_eq!(cache.category_memory(MemoryCategory::PubSub), 0);
}

/// Flooding a small client buffer must not panic; drops/overwrites are counted.
#[tokio::test]
async fn test_broadcast_full_does_not_panic() {
    let pubsub = PubSub::with_client_buffer_capacity(8);
    assert_eq!(pubsub.client_buffer_capacity(), 8);

    let (client_id, _rx) = pubsub.register_client().await;
    let channel = Bytes::from("full-buf");
    pubsub.subscribe(client_id, channel.clone()).await;

    // Far more messages than capacity; must not panic.
    for i in 0..64 {
        let msg = Bytes::from(format!("msg-{i}"));
        let outcome = pubsub.publish_with_outcome(&channel, &msg).await;
        assert_eq!(outcome.recipients, 1);
    }

    // After capacity is filled, further publishes overwrite oldest slots.
    assert!(
        pubsub.messages_dropped() > 0,
        "expected overwrite/drop stats on full buffer"
    );
    assert!(
        pubsub.pending_memory().await <= 8 * 16, // "msg-XX" sizes are small
        "pending queue should be capped near buffer capacity"
    );

    // Pending queue length must not exceed capacity
    // (each message size varies; check via note_delivered count)
    let mut delivered = 0usize;
    loop {
        let n = pubsub.note_delivered(client_id).await;
        if n == 0 {
            break;
        }
        delivered += 1;
        assert!(delivered <= 8, "pending entries exceeded capacity");
    }
}

/// Lagged receivers surface RecvError::Lagged after buffer overflow.
#[tokio::test]
async fn test_lagged_receiver_is_detected() {
    let pubsub = PubSub::with_client_buffer_capacity(2);
    let (client_id, mut rx) = pubsub.register_client().await;
    let channel = Bytes::from("lag-ch");
    pubsub.subscribe(client_id, channel.clone()).await;

    for i in 0..5 {
        pubsub
            .publish(&channel, &Bytes::from(format!("m{i}")))
            .await;
    }

    // First recv after overflow should report Lagged (or we keep reading until it does).
    let mut saw_lagged = false;
    for _ in 0..8 {
        match rx.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                assert!(n >= 1);
                let freed = pubsub.note_lagged(client_id, n).await;
                // pending accounting should shrink
                let _ = freed;
                saw_lagged = true;
                break;
            }
            Ok(_) => {
                let _ = pubsub.note_delivered(client_id).await;
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
        }
    }
    assert!(saw_lagged, "expected RecvError::Lagged after overflowing buffer");
}
