//! Redis-style approximated LFU (logarithmic counter + minute decay).
//!
//! Layout of the 24-bit value (stored in the low bits of a `u64`):
//! - bits 8..23: last access/decrement time in Unix minutes (`u16`)
//! - bits 0..7:  logarithmic counter (`u8`, init = [`LFU_INIT_VAL`])
//!
//! Matches Redis `LFULogIncr` / `LFUDecrAndReturn` behaviour used by
//! `allkeys-lfu` / `volatile-lfu` eviction.

use std::time::{SystemTime, UNIX_EPOCH};

/// Counter value for a newly created object (Redis `LFU_INIT_VAL`).
pub const LFU_INIT_VAL: u8 = 5;

/// Default `lfu-log-factor` (Redis default 10). Higher → slower counter growth.
pub const LFU_LOG_FACTOR_DEFAULT: u8 = 10;

/// Default `lfu-decay-time` in minutes (Redis default 1). `0` disables decay.
pub const LFU_DECAY_TIME_DEFAULT: u8 = 1;

/// Current Unix time in minutes, truncated to 16 bits (Redis wrap-around).
pub fn minutes_now() -> u16 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    ((secs / 60) & 0xffff) as u16
}

/// Pack last-decrement-time + counter into a u64.
#[inline]
pub fn pack(ldt: u16, counter: u8) -> u64 {
    ((ldt as u64) << 8) | (counter as u64)
}

/// Unpack stored LFU word.
#[inline]
pub fn unpack(v: u64) -> (u16, u8) {
    (((v >> 8) & 0xffff) as u16, (v & 0xff) as u8)
}

/// Minutes elapsed between `ldt` and `now`, handling 16-bit wrap (Redis formula).
pub fn time_elapsed(ldt: u16, now: u16) -> u16 {
    if now >= ldt {
        now - ldt
    } else {
        // Redis: return 65535-ldt+now;
        65535u16.wrapping_sub(ldt).wrapping_add(now)
    }
}

/// Apply time-based decay to the counter (Redis `LFUDecrAndReturn` core).
///
/// Every `decay_time` minutes of idle time subtracts 1 from the counter.
/// `decay_time == 0` disables decay.
pub fn decr_counter(ldt: u16, counter: u8, decay_time: u8, now: u16) -> u8 {
    if decay_time == 0 {
        return counter;
    }
    let elapsed = time_elapsed(ldt, now) as u32;
    let num_periods = elapsed / (decay_time as u32);
    if num_periods == 0 {
        counter
    } else if num_periods >= counter as u32 {
        0
    } else {
        counter - num_periods as u8
    }
}

/// Probabilistic logarithmic counter increment (Redis `LFULogIncr`).
pub fn log_incr(counter: u8, log_factor: u8) -> u8 {
    if counter == 255 {
        return 255;
    }
    let r: f64 = rand::random();
    let baseval = if counter > LFU_INIT_VAL {
        (counter - LFU_INIT_VAL) as f64
    } else {
        0.0
    };
    let factor = log_factor.max(1) as f64;
    let p = 1.0 / (baseval * factor + 1.0);
    if r < p {
        counter + 1
    } else {
        counter
    }
}

/// Initial packed LFU value for a new key.
pub fn initial() -> u64 {
    pack(minutes_now(), LFU_INIT_VAL)
}

/// Update packed LFU on key access: decay, then log-increment, stamp now.
pub fn on_access(packed: u64, log_factor: u8, decay_time: u8) -> u64 {
    let now = minutes_now();
    let (ldt, counter) = unpack(packed);
    let c = decr_counter(ldt, counter, decay_time, now);
    let c = log_incr(c, log_factor);
    pack(now, c)
}

/// Effective (decayed) counter for eviction ranking — does not mutate storage.
pub fn effective_counter(packed: u64, decay_time: u8) -> u8 {
    let now = minutes_now();
    let (ldt, counter) = unpack(packed);
    decr_counter(ldt, counter, decay_time, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrip() {
        let p = pack(0x1234, 42);
        assert_eq!(unpack(p), (0x1234, 42));
    }

    #[test]
    fn initial_has_init_val() {
        let (_ldt, c) = unpack(initial());
        assert_eq!(c, LFU_INIT_VAL);
    }

    #[test]
    fn no_decay_when_decay_time_zero() {
        assert_eq!(decr_counter(100, 50, 0, 200), 50);
    }

    #[test]
    fn decay_subtracts_periods() {
        // 10 minutes idle, decay_time=1 → subtract 10
        assert_eq!(decr_counter(100, 50, 1, 110), 40);
        // 10 minutes idle, decay_time=5 → subtract 2
        assert_eq!(decr_counter(100, 50, 5, 110), 48);
        // more periods than counter → 0
        assert_eq!(decr_counter(100, 3, 1, 110), 0);
    }

    #[test]
    fn time_elapsed_wrap() {
        // Redis: 65535 - 65530 + 5 = 10
        assert_eq!(time_elapsed(65530, 5), 10);
    }

    #[test]
    fn log_incr_saturates_at_255() {
        assert_eq!(log_incr(255, 10), 255);
    }

    #[test]
    fn log_incr_often_bumps_low_counters() {
        // counter at INIT_VAL: p = 1.0 → always increments
        assert_eq!(log_incr(LFU_INIT_VAL, 10), LFU_INIT_VAL + 1);
        // counter below INIT_VAL: same (baseval=0, p=1)
        assert_eq!(log_incr(0, 10), 1);
    }

    #[test]
    fn on_access_never_decreases_without_idle_time() {
        // same minute → no decay; low counter always increments
        let start = pack(minutes_now(), LFU_INIT_VAL);
        let next = on_access(start, 10, 1);
        let (_, c0) = unpack(start);
        let (_, c1) = unpack(next);
        assert!(c1 >= c0);
        assert_eq!(c1, c0 + 1); // deterministic at INIT_VAL
    }
}
