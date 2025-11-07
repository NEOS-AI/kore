use kore::Cache;
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
    cache.pubsub.unregister_client(client_id).await;
    
    // Channel should be removed
    assert_eq!(cache.pubsub.num_channels().await, 0);
    assert_eq!(cache.pubsub.num_subscribers(&channel).await, 0);
}
