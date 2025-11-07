// Pub/Sub examples for Kore
//
// This file demonstrates various Pub/Sub patterns and use cases

use kore::Cache;
use bytes::Bytes;
use tokio::time::{sleep, Duration};

/// Example 1: Basic Publisher-Subscriber pattern
async fn basic_pubsub() {
    println!("\n=== Example 1: Basic Pub/Sub ===");
    
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    // Register subscribers
    let (subscriber1, mut rx1) = cache.pubsub.register_client().await;
    let (subscriber2, mut rx2) = cache.pubsub.register_client().await;
    
    // Subscribe to channel
    let channel = Bytes::from("notifications");
    cache.pubsub.subscribe(subscriber1, channel.clone()).await;
    cache.pubsub.subscribe(subscriber2, channel.clone()).await;
    
    println!("Subscribers registered for 'notifications' channel");
    
    // Spawn tasks to listen for messages
    let listener1 = tokio::spawn(async move {
        while let Ok(msg) = rx1.recv().await {
            println!("Subscriber 1 received: {:?}", msg);
        }
    });
    
    let listener2 = tokio::spawn(async move {
        while let Ok(msg) = rx2.recv().await {
            println!("Subscriber 2 received: {:?}", msg);
        }
    });
    
    // Publish messages
    sleep(Duration::from_millis(100)).await;
    let message = Bytes::from("Welcome to Kore Pub/Sub!");
    let count = cache.pubsub.publish(&channel, &message).await;
    println!("Published message to {} subscribers", count);
    
    sleep(Duration::from_millis(100)).await;
    listener1.abort();
    listener2.abort();
}

/// Example 2: Pattern-based subscription
async fn pattern_subscription() {
    println!("\n=== Example 2: Pattern Subscription ===");
    
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (subscriber, mut rx) = cache.pubsub.register_client().await;
    
    // Subscribe to pattern: all channels starting with "log."
    let pattern = Bytes::from("log.*");
    cache.pubsub.psubscribe(subscriber, pattern).await;
    println!("Subscribed to pattern: log.*");
    
    // Spawn listener
    let listener = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            println!("Pattern subscriber received: {:?}", msg);
        }
    });
    
    // Publish to different matching channels
    sleep(Duration::from_millis(100)).await;
    
    cache.pubsub.publish(
        &Bytes::from("log.info"),
        &Bytes::from("Application started")
    ).await;
    
    cache.pubsub.publish(
        &Bytes::from("log.error"),
        &Bytes::from("Connection failed")
    ).await;
    
    cache.pubsub.publish(
        &Bytes::from("log.debug"),
        &Bytes::from("Processing request")
    ).await;
    
    // This won't match
    cache.pubsub.publish(
        &Bytes::from("metrics.cpu"),
        &Bytes::from("75%")
    ).await;
    
    sleep(Duration::from_millis(100)).await;
    listener.abort();
}

/// Example 3: Multiple channels per subscriber
async fn multiple_channels() {
    println!("\n=== Example 3: Multiple Channels ===");
    
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (subscriber, mut rx) = cache.pubsub.register_client().await;
    
    // Subscribe to multiple channels
    let channels = vec![
        Bytes::from("news"),
        Bytes::from("sports"),
        Bytes::from("tech"),
    ];
    
    for channel in &channels {
        cache.pubsub.subscribe(subscriber, channel.clone()).await;
        println!("Subscribed to: {:?}", String::from_utf8_lossy(channel));
    }
    
    // Listen for messages
    let listener = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            println!("Received: {:?}", msg);
        }
    });
    
    sleep(Duration::from_millis(100)).await;
    
    // Publish to each channel
    cache.pubsub.publish(&channels[0], &Bytes::from("Breaking: Market rally")).await;
    cache.pubsub.publish(&channels[1], &Bytes::from("Team wins championship")).await;
    cache.pubsub.publish(&channels[2], &Bytes::from("New AI breakthrough")).await;
    
    sleep(Duration::from_millis(100)).await;
    listener.abort();
}

/// Example 4: Dynamic subscription management
async fn dynamic_subscriptions() {
    println!("\n=== Example 4: Dynamic Subscription Management ===");
    
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    let (subscriber, mut rx) = cache.pubsub.register_client().await;
    
    let channel = Bytes::from("updates");
    
    // Subscribe
    cache.pubsub.subscribe(subscriber, channel.clone()).await;
    println!("Subscribed to 'updates'");
    
    let listener = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            println!("Received update: {:?}", msg);
        }
    });
    
    // Publish some messages
    cache.pubsub.publish(&channel, &Bytes::from("Update 1")).await;
    sleep(Duration::from_millis(50)).await;
    
    cache.pubsub.publish(&channel, &Bytes::from("Update 2")).await;
    sleep(Duration::from_millis(50)).await;
    
    // Unsubscribe
    cache.pubsub.unsubscribe(subscriber, &channel).await;
    println!("Unsubscribed from 'updates'");
    
    // This message won't be received
    cache.pubsub.publish(&channel, &Bytes::from("Update 3 - won't receive")).await;
    
    sleep(Duration::from_millis(100)).await;
    listener.abort();
}

/// Example 5: Pub/Sub statistics and introspection
async fn pubsub_stats() {
    println!("\n=== Example 5: Pub/Sub Statistics ===");
    
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    // Create multiple subscribers
    let (sub1, _rx1) = cache.pubsub.register_client().await;
    let (sub2, _rx2) = cache.pubsub.register_client().await;
    let (sub3, _rx3) = cache.pubsub.register_client().await;
    
    // Subscribe to various channels
    cache.pubsub.subscribe(sub1, Bytes::from("news")).await;
    cache.pubsub.subscribe(sub2, Bytes::from("news")).await;
    cache.pubsub.subscribe(sub3, Bytes::from("sports")).await;
    
    // Pattern subscriptions
    cache.pubsub.psubscribe(sub1, Bytes::from("log.*")).await;
    cache.pubsub.psubscribe(sub2, Bytes::from("metric.*")).await;
    
    // Get statistics
    let num_channels = cache.pubsub.num_channels().await;
    println!("Active channels: {}", num_channels);
    
    let num_patterns = cache.pubsub.num_patterns().await;
    println!("Active patterns: {}", num_patterns);
    
    let news_subs = cache.pubsub.num_subscribers(&Bytes::from("news")).await;
    println!("Subscribers on 'news': {}", news_subs);
    
    // List all channels
    let channels = cache.pubsub.list_channels(None).await;
    println!("All channels: {:?}", channels);
    
    // List channels matching pattern
    let news_channels = cache.pubsub.list_channels(Some(&Bytes::from("new*"))).await;
    println!("Channels matching 'new*': {:?}", news_channels);
}

/// Example 6: Fan-out messaging pattern
async fn fanout_pattern() {
    println!("\n=== Example 6: Fan-out Messaging ===");
    
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    // Create multiple worker subscribers
    let mut workers = Vec::new();
    for i in 1..=5 {
        let (worker, mut rx) = cache.pubsub.register_client().await;
        cache.pubsub.subscribe(worker, Bytes::from("tasks")).await;
        
        let task = tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                println!("Worker {} processing: {:?}", i, msg);
                // Simulate work
                sleep(Duration::from_millis(10)).await;
            }
        });
        
        workers.push(task);
    }
    
    println!("5 workers ready and listening on 'tasks' channel");
    
    sleep(Duration::from_millis(100)).await;
    
    // Publish tasks - all workers will receive all messages
    for i in 1..=10 {
        let task = format!("Task #{}", i);
        cache.pubsub.publish(&Bytes::from("tasks"), &Bytes::from(task)).await;
        sleep(Duration::from_millis(20)).await;
    }
    
    sleep(Duration::from_millis(200)).await;
    
    for worker in workers {
        worker.abort();
    }
}

/// Example 7: Topic-based routing
async fn topic_routing() {
    println!("\n=== Example 7: Topic-based Routing ===");
    
    let cache = Cache::new(16, 1024 * 1024 * 100);
    
    // Tech subscriber
    let (tech_sub, mut tech_rx) = cache.pubsub.register_client().await;
    cache.pubsub.psubscribe(tech_sub, Bytes::from("topic.tech.*")).await;
    
    let tech_listener = tokio::spawn(async move {
        while let Ok(msg) = tech_rx.recv().await {
            println!("[TECH] {:?}", msg);
        }
    });
    
    // Business subscriber
    let (biz_sub, mut biz_rx) = cache.pubsub.register_client().await;
    cache.pubsub.psubscribe(biz_sub, Bytes::from("topic.business.*")).await;
    
    let biz_listener = tokio::spawn(async move {
        while let Ok(msg) = biz_rx.recv().await {
            println!("[BUSINESS] {:?}", msg);
        }
    });
    
    // All-topics subscriber
    let (all_sub, mut all_rx) = cache.pubsub.register_client().await;
    cache.pubsub.psubscribe(all_sub, Bytes::from("topic.*")).await;
    
    let all_listener = tokio::spawn(async move {
        while let Ok(msg) = all_rx.recv().await {
            println!("[ALL] {:?}", msg);
        }
    });
    
    sleep(Duration::from_millis(100)).await;
    
    // Publish to different topics
    cache.pubsub.publish(
        &Bytes::from("topic.tech.ai"),
        &Bytes::from("New AI model released")
    ).await;
    
    cache.pubsub.publish(
        &Bytes::from("topic.business.market"),
        &Bytes::from("Stock market update")
    ).await;
    
    cache.pubsub.publish(
        &Bytes::from("topic.tech.security"),
        &Bytes::from("Critical security patch")
    ).await;
    
    sleep(Duration::from_millis(100)).await;
    
    tech_listener.abort();
    biz_listener.abort();
    all_listener.abort();
}

#[tokio::main]
async fn main() {
    // Run all examples
    basic_pubsub().await;
    pattern_subscription().await;
    multiple_channels().await;
    dynamic_subscriptions().await;
    pubsub_stats().await;
    fanout_pattern().await;
    topic_routing().await;
    
    println!("\n=== All examples completed ===");
}
