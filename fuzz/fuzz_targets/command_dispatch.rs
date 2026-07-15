//! Fuzz target: command name bytes through handler must not panic.
#![no_main]
use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

fn bulk(b: &[u8]) -> RespValue {
    RespValue::BulkString(Some(Bytes::copy_from_slice(b)))
}

fuzz_target!(|data: &[u8]| {
    // Cap size so we don't thrash allocator.
    if data.len() > 512 {
        return;
    }
    let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
    let config = Arc::new(Config::default());
    let mut h = CommandHandler::new(cache, config);

    // Split input into command + up to 3 args on 0-bytes.
    let parts: Vec<&[u8]> = data.split(|&b| b == 0).take(4).collect();
    if parts.is_empty() || parts[0].is_empty() {
        return;
    }
    let mut args = vec![bulk(parts[0])];
    for p in parts.iter().skip(1) {
        args.push(bulk(p));
    }
    let cmd = RespValue::Array(args);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    if let Ok(rt) = rt {
        let _ = rt.block_on(async { h.handle(cmd).await });
    }
});
