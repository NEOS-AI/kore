//! Redlock CLI flag → Server wiring.
//!
//! Verifies `Redlock::from_config` and that the server path can hold a
//! constructed Redlock. Production path uses remote RESP backends; tests
//! inject in-process `Cache` instances where locks must succeed offline.

use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::{Cache, Redlock, Server};
use bytes::Bytes;
use std::sync::Arc;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 16, 1024 * 1024, false)
}

fn enabled_config(retry_count: usize, retry_delay_ms: u64) -> Config {
    let mut c = Config::default();
    c.enable_redlock = true;
    c.redlock_instances = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003".to_string();
    c.redlock_retry_count = retry_count;
    c.redlock_retry_delay_ms = retry_delay_ms;
    c.shards = 16;
    c.maxmemory = 16 * 1024 * 1024;
    c
}

fn local_backends(n: usize) -> Vec<Arc<Cache>> {
    (0..n).map(|_| make_cache()).collect()
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(handler: &mut CommandHandler, value: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { handler.handle(value).await.unwrap() })
}

fn bulk_str(v: &RespValue) -> String {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected bulk string, got {:?}", other),
    }
}

#[test]
fn test_redlock_from_config_disabled_is_none() {
    let config = Config::default();
    assert!(!config.enable_redlock);

    let result = Redlock::from_config(&config, None).unwrap();
    assert!(result.is_none(), "disabled redlock must yield None");

    // Explicit backends ignored when disabled
    let result = Redlock::from_config(&config, Some(local_backends(3))).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_redlock_from_config_applies_retry_params() {
    let config = enabled_config(7, 350);
    config.validate().expect("enabled config should validate");

    let redlock = Redlock::from_config(&config, Some(local_backends(3)))
        .unwrap()
        .expect("enabled redlock should be Some");

    assert_eq!(redlock.retry_count(), 7);
    assert_eq!(redlock.retry_delay_ms(), 350);
    assert_eq!(redlock.instance_count(), 3);
    assert_eq!(redlock.quorum, 2);
}

#[test]
fn test_redlock_from_config_builds_remote_backends_when_none_injected() {
    let config = enabled_config(3, 200);
    let redlock = Redlock::from_config(&config, None)
        .unwrap()
        .expect("should build remote RESP backends from addresses");
    assert_eq!(redlock.instance_count(), 3);
    // Addresses are not listening → acquire soft-fails (no panic)
    let r = redlock.lock("offline", Bytes::from("v"), 100);
    assert!(r.is_err());
}

#[test]
fn test_server_redlock_disabled_by_default() {
    let config = Arc::new(Config::default());
    let cache = make_cache();
    let server = Server::new(cache.clone(), config.clone());

    assert!(
        server.redlock().is_none(),
        "Server must not hold Redlock when flags are off"
    );

    let mut handler = CommandHandler::new(cache, config);
    let body = bulk_str(&handle(&mut handler, cmd(&["INFO"])));
    assert!(
        body.contains("redlock_enabled:0"),
        "INFO should report redlock disabled: {}",
        body
    );
    assert!(body.contains("redlock_instances:0"), "got: {}", body);
    assert!(body.contains("redlock_retry_count:3"), "got: {}", body);
    assert!(body.contains("redlock_retry_delay_ms:200"), "got: {}", body);
}

#[test]
fn test_server_exposes_redlock_when_enabled() {
    let config = enabled_config(5, 150);
    config.validate().unwrap();
    let config = Arc::new(config);

    let redlock = Redlock::from_config(&config, Some(local_backends(3)))
        .unwrap()
        .expect("redlock");

    let cache = make_cache();
    let server = Server::new(cache.clone(), config.clone()).with_redlock(Some(redlock));

    let held = server
        .redlock()
        .expect("Server should expose Redlock when enabled");
    assert_eq!(held.retry_count(), 5);
    assert_eq!(held.retry_delay_ms(), 150);
    assert_eq!(held.instance_count(), 3);

    let mut handler = CommandHandler::new(cache, config);
    let body = bulk_str(&handle(&mut handler, cmd(&["INFO"])));
    assert!(
        body.contains("redlock_enabled:1"),
        "INFO should report redlock enabled: {}",
        body
    );
    assert!(
        body.contains("redlock_instances:3"),
        "INFO should report instance count: {}",
        body
    );
    assert!(body.contains("redlock_retry_count:5"), "got: {}", body);
    assert!(body.contains("redlock_retry_delay_ms:150"), "got: {}", body);
}

#[test]
fn test_enable_redlock_requires_at_least_three_instances() {
    let mut config = Config::default();
    config.enable_redlock = true;
    config.redlock_instances = "127.0.0.1:7001,127.0.0.1:7002".to_string();
    let err = config.validate().unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("at least 3") || msg.contains("3 instances"),
        "validation message: {}",
        msg
    );
}


#[test]
fn test_info_reports_fair_queue_disabled_by_default() {
    let config = Arc::new(Config::default());
    let cache = make_cache();
    let mut handler = CommandHandler::new(cache, config);
    let body = bulk_str(&handle(&mut handler, cmd(&["INFO"])));
    assert!(
        body.contains("fair_queue_enabled:0"),
        "INFO should report fair queue disabled: {}",
        body
    );
    assert!(body.contains("# FairQueue"), "got: {}", body);
}

#[test]
fn test_from_config_enables_fair_queue() {
    let mut config = enabled_config(5, 100);
    config.enable_fair_queue = true;
    config.fair_queue_max_size = 64;
    config.fair_queue_cleanup_ms = 200;
    config.validate().unwrap();

    let redlock = Redlock::from_config(&config, Some(local_backends(3)))
        .unwrap()
        .expect("redlock");
    assert!(redlock.fair_queue_enabled());
    let stats = redlock.get_fair_queue_stats().expect("stats");
    assert_eq!(stats.total_enqueued, 0);

    let info = redlock.fair_queue_info_lines();
    assert!(info.contains("fair_queue_enabled:1"), "got: {}", info);
}

#[test]
fn test_fair_queue_requires_redlock() {
    let mut config = Config::default();
    config.enable_fair_queue = true;
    let err = config.validate().unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("enable_fair_queue") || msg.contains("redlock"),
        "got: {}",
        msg
    );
}

#[test]
fn test_server_handler_info_with_fair_queue_redlock() {
    let mut config = enabled_config(3, 50);
    config.enable_fair_queue = true;
    config.validate().unwrap();
    let config = Arc::new(config);
    let cache = make_cache();
    let redlock = Redlock::from_config(&config, Some(local_backends(3)))
        .unwrap();
    let server = Server::new(cache.clone(), config.clone()).with_redlock(redlock);
    assert!(server.redlock().is_some());
    assert!(server.redlock().unwrap().fair_queue_enabled());

    let mut handler = CommandHandler::new(cache, config)
        .with_redlock(server.redlock().cloned());
    // enqueue via lock so stats move
    let rl = server.redlock().unwrap();
    let _lock = rl.lock("info-res", Bytes::from("c1"), 2000).unwrap();
    let body = bulk_str(&handle(&mut handler, cmd(&["INFO"])));
    assert!(body.contains("fair_queue_enabled:1"), "got: {}", body);
    assert!(body.contains("fair_queue_total_enqueued:"), "got: {}", body);
}
