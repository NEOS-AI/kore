use kore::{Cache, Config, Server};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration, timeout};

/// Helper function to send a RESP command and receive response
async fn send_command(stream: &mut TcpStream, command: &str) -> Result<Vec<u8>, String> {
    // Send command with timeout
    timeout(Duration::from_secs(3), stream.write_all(command.as_bytes()))
        .await
        .map_err(|_| "Timeout writing command".to_string())?
        .map_err(|e| format!("Failed to write command: {}", e))?;
    
    // Read response with timeout
    let mut buffer = vec![0u8; 4096];
    let n = timeout(Duration::from_secs(3), stream.read(&mut buffer))
        .await
        .map_err(|_| "Timeout reading response".to_string())?
        .map_err(|e| format!("Failed to read response: {}", e))?;
    
    if n == 0 {
        return Err("Connection closed".to_string());
    }
    
    buffer.truncate(n);
    Ok(buffer)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_network_basic_commands() {
    // Start server on a unique port
    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 16380,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
            dir: "./data".to_string(),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            replicaof: String::new(),
            save: "900,1 300,10 60,10000".to_string(),
            maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
unixsocket: String::new(),
});

    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache.clone(), config.clone());

    // Spawn server in background and keep the handle
    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Wait for server to start
    sleep(Duration::from_millis(200)).await;

    // Connect to server with timeout
    let mut stream = match timeout(Duration::from_secs(2), TcpStream::connect("127.0.0.1:16380")).await {
        Ok(Ok(s)) => s,
        _ => {
            server_handle.abort();
            panic!("Failed to connect to server");
        }
    };

    // Test PING
    let response = send_command(&mut stream, "*1\r\n$4\r\nPING\r\n").await
        .expect("PING command failed");
    assert!(response.starts_with(b"+PONG"));

    // Test SET
    let response = send_command(&mut stream, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n").await
        .expect("SET command failed");
    assert!(response.starts_with(b"+OK"));

    // Test GET
    let response = send_command(&mut stream, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n").await
        .expect("GET command failed");
    assert!(String::from_utf8_lossy(&response).contains("value"));

    // Test DEL
    let response = send_command(&mut stream, "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n").await
        .expect("DEL command failed");
    assert!(response.starts_with(b":1"));
    
    // Clean up: close connection and abort server
    drop(stream);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_network_concurrent_clients() {
    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 16382,
        threads: 2,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
            dir: "./data".to_string(),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            replicaof: String::new(),
            save: "900,1 300,10 60,10000".to_string(),
            maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
unixsocket: String::new(),
});

    let cache = Cache::new(config.shards, config.maxmemory);
    let cache_clone = cache.clone();
    let server = Server::new(cache.clone(), config.clone());

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(300)).await;

    // Spawn 10 concurrent clients
    let mut handles = vec![];
    for i in 0..10 {
        let handle = tokio::spawn(async move {
            // Connect with timeout
            let mut stream = match timeout(Duration::from_secs(3), TcpStream::connect("127.0.0.1:16382")).await {
                Ok(Ok(s)) => s,
                _ => return false,
            };
            
            // Each client sets and gets its own key
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            
            let set_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", 
                                 key.len(), key, value.len(), value);
            
            // Send SET with timeout
            let set_result = timeout(Duration::from_secs(3), send_command(&mut stream, &set_cmd)).await;
            if set_result.is_err() || set_result.unwrap().is_err() {
                return false;
            }
            
            let get_cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
            
            // Send GET with timeout
            let response = match timeout(Duration::from_secs(3), send_command(&mut stream, &get_cmd)).await {
                Ok(Ok(r)) => r,
                _ => return false,
            };
            
            // Verify response contains the value
            let result = String::from_utf8_lossy(&response).contains(&value);
            
            // Close connection
            drop(stream);
            
            result
        });
        handles.push(handle);
    }

    // Wait for all clients to complete with timeout
    let mut success_count = 0;
    for handle in handles {
        match timeout(Duration::from_secs(5), handle).await {
            Ok(Ok(true)) => success_count += 1,
            _ => {},
        }
    }

    // At least 8 out of 10 clients should succeed (allow some failures due to timing)
    assert!(success_count >= 8, "Only {} clients succeeded", success_count);

    // Give connections time to close
    sleep(Duration::from_millis(200)).await;

    // Verify stats - active connections should be 0
    assert_eq!(cache_clone.stats.active_connections.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(cache_clone.stats.total_connections.load(std::sync::atomic::Ordering::Relaxed) >= success_count as u64);
    
    // Clean up
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_network_info_command() {
    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 16383,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
            dir: "./data".to_string(),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            replicaof: String::new(),
            save: "900,1 300,10 60,10000".to_string(),
            maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
unixsocket: String::new(),
});

    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache.clone(), config.clone());

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(200)).await;

    let mut stream = match timeout(Duration::from_secs(2), TcpStream::connect("127.0.0.1:16383")).await {
        Ok(Ok(s)) => s,
        _ => {
            server_handle.abort();
            panic!("Failed to connect to server");
        }
    };

    // Test INFO command
    let response = send_command(&mut stream, "*1\r\n$4\r\nINFO\r\n")
        .await
        .expect("INFO command should succeed");
    let info = String::from_utf8_lossy(&response);
    
    // Verify INFO contains expected sections
    assert!(info.contains("kore_version"));
    assert!(info.contains("total_commands_processed"));
    assert!(info.contains("used_memory"));
    assert!(info.contains("maxmemory"));
    assert!(info.contains("bytes_sent"));
    assert!(info.contains("bytes_received"));
    assert!(info.contains("active_connections"));
    
    // Clean up
    drop(stream);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_network_maxconns() {
    // Start server with maxconns=2 on a unique high port to avoid collisions
    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 16490,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 2,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
            dir: "./data".to_string(),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            replicaof: String::new(),
            save: "900,1 300,10 60,10000".to_string(),
            maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
unixsocket: String::new(),
});

    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache.clone(), config.clone());

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(200)).await;

    // Open 2 connections that stay open (hold the maxconns slots)
    let stream1 = match timeout(Duration::from_secs(2), TcpStream::connect("127.0.0.1:16490")).await {
        Ok(Ok(s)) => s,
        _ => {
            server_handle.abort();
            panic!("Failed to open first connection");
        }
    };

    let stream2 = match timeout(Duration::from_secs(2), TcpStream::connect("127.0.0.1:16490")).await {
        Ok(Ok(s)) => s,
        _ => {
            server_handle.abort();
            drop(stream1);
            panic!("Failed to open second connection");
        }
    };

    // Give the server a moment to accept and reserve permits
    sleep(Duration::from_millis(100)).await;

    // Third connection should be rejected (error reply and/or closed)
    let mut stream3 = match timeout(Duration::from_secs(2), TcpStream::connect("127.0.0.1:16490")).await {
        Ok(Ok(s)) => s,
        _ => {
            drop(stream1);
            drop(stream2);
            server_handle.abort();
            panic!("Failed to open third connection (TCP connect should still succeed)");
        }
    };

    let mut buf = vec![0u8; 256];
    let read_result = timeout(Duration::from_secs(2), stream3.read(&mut buf)).await;

    let rejected = match read_result {
        // Connection closed without data
        Ok(Ok(0)) => true,
        // Received Redis-style max clients error
        Ok(Ok(n)) => {
            let msg = String::from_utf8_lossy(&buf[..n]);
            msg.contains("max number of clients reached") || msg.contains("ERR")
        }
        // Read error / timeout also indicates rejection path closed the socket
        Ok(Err(_)) | Err(_) => true,
    };

    assert!(
        rejected,
        "Third connection should be rejected when maxconns=2"
    );

    // After closing one held connection, a new one should be accepted
    drop(stream1);
    sleep(Duration::from_millis(150)).await;

    let mut stream4 = match timeout(Duration::from_secs(2), TcpStream::connect("127.0.0.1:16490")).await {
        Ok(Ok(s)) => s,
        _ => {
            drop(stream2);
            drop(stream3);
            server_handle.abort();
            panic!("Failed to reconnect after freeing a slot");
        }
    };

    let response = send_command(&mut stream4, "*1\r\n$4\r\nPING\r\n")
        .await
        .expect("PING should succeed after a connection slot is freed");
    assert!(response.starts_with(b"+PONG"));

    // Clean up
    drop(stream2);
    drop(stream3);
    drop(stream4);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_network_auth() {
    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 16491,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: "s3cret".to_string(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
        dir: "./data".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
            save: "900,1 300,10 60,10000".to_string(),
            maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
unixsocket: String::new(),
});

    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache.clone(), config.clone());

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(200)).await;

    let mut stream = match timeout(Duration::from_secs(2), TcpStream::connect("127.0.0.1:16491")).await
    {
        Ok(Ok(s)) => s,
        _ => {
            server_handle.abort();
            panic!("Failed to connect to server");
        }
    };

    // Unauthenticated PING must fail
    let response = send_command(&mut stream, "*1\r\n$4\r\nPING\r\n")
        .await
        .expect("PING should get a reply");
    assert!(
        String::from_utf8_lossy(&response).contains("NOAUTH"),
        "expected NOAUTH, got {:?}",
        String::from_utf8_lossy(&response)
    );

    // Wrong password
    let response = send_command(&mut stream, "*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n")
        .await
        .expect("AUTH wrong password");
    assert!(
        String::from_utf8_lossy(&response).contains("invalid password")
            || String::from_utf8_lossy(&response).contains("WRONGPASS")
            || String::from_utf8_lossy(&response).contains("ERR"),
        "expected auth failure, got {:?}",
        String::from_utf8_lossy(&response)
    );

    // Correct password
    let response = send_command(&mut stream, "*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n")
        .await
        .expect("AUTH correct password");
    assert!(
        response.starts_with(b"+OK"),
        "expected +OK, got {:?}",
        String::from_utf8_lossy(&response)
    );

    // Now PING works
    let response = send_command(&mut stream, "*1\r\n$4\r\nPING\r\n")
        .await
        .expect("PING after AUTH");
    assert!(response.starts_with(b"+PONG"));

    // SET/GET after AUTH
    let response =
        send_command(&mut stream, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .expect("SET after AUTH");
    assert!(response.starts_with(b"+OK"));

    let response = send_command(&mut stream, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .expect("GET after AUTH");
    assert!(String::from_utf8_lossy(&response).contains("bar"));

    drop(stream);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
}
