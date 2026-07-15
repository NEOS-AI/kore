//! Fuzz target: RESP parser must not panic on arbitrary input.
#![no_main]
use libfuzzer_sys::fuzz_target;
use kore::protocol::RespParser;

fuzz_target!(|data: &[u8]| {
    let mut parser = RespParser::new();
    // Feed in chunks to exercise partial-parse paths.
    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + 1 + (data[offset] as usize % 64)).min(data.len());
        parser.feed(&data[offset..end]);
        offset = end;
        loop {
            match parser.parse() {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => {
                    parser = RespParser::new();
                    break;
                }
            }
        }
    }
});
