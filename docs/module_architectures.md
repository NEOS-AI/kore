# 파일 구조

## 1. Commands 모듈 파일 구조
```
src/commands/
├── mod.rs          - CommandHandler 구조체와 메인 execute() 메서드
├── basic.rs        - PING, ECHO, AUTH 등 기본 명령어
├── key_value.rs    - GET, SET, DEL, EXISTS, MGET, MSET 등
├── counter.rs      - INCR, DECR, INCRBY, DECRBY 등
├── expiration.rs   - EXPIRE, PEXPIRE, TTL, PTTL 등
├── admin.rs        - DBSIZE, KEYS, FLUSH, INFO, SWEEP, CONFIG 등
└── sorted_set.rs   - ZADD, ZRANGE, ZCARD, ZSCORE, ZREM 등
```

## 2. Cache 모듈 파일 구조
```
src/cache/
├── mod.rs          - Cache 구조체 정의 및 생성자
├── storage.rs      - store, load, delete, exists 등 기본 저장소 작업
├── operations.rs   - incr, decr 등 원자적 연산
├── expiration.rs   - expire, ttl 등 만료 관련 작업
├── eviction.rs     - evict_lru, sweep 등 메모리 관리
├── sorted_sets.rs  - 정렬된 집합 관련 작업
└── config.rs       - max_entry_size, eviction_sample_size 등 설정
```
