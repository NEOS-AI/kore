//! Redlock CLI flag → Server wiring (MVP).
//!
//! Verifies `Redlock::from_config` and that the server path can hold a
//! constructed Redlock. Remote RESP backends are deferred; tests inject
//! in-process `Cache` instances.

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
fn test_redlock_from_config_builds_local_backends_when_none_injected() {
    let config = enabled_config(3, 200);
    let redlock = Redlock::from_config(&config, None)
        .unwrap()
        .expect("should build local backends from instance count");
    assert_eq!(redlock.instance_count(), 3);
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
