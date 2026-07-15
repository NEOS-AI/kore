# Kore Pub/Sub

Redis 스타일의 Pub/Sub (Publish/Subscribe) 메시징 시스템 구현입니다.

## 개요

Kore의 Pub/Sub 시스템은 메시지 브로커 패턴을 구현하여, 클라이언트들이 채널을 통해 메시지를 발행하고 구독할 수 있게 합니다.

## 주요 기능

### 1. 기본 채널 구독 (Channel Subscription)
- **SUBSCRIBE**: 하나 이상의 채널 구독
- **UNSUBSCRIBE**: 채널 구독 해제
- **PUBLISH**: 채널에 메시지 발행

### 2. 패턴 기반 구독 (Pattern Subscription)
- **PSUBSCRIBE**: 패턴을 사용한 채널 구독
- **PUNSUBSCRIBE**: 패턴 구독 해제
- 지원하는 패턴:
  - `*`: 임의의 문자열 매칭
  - `?`: 단일 문자 매칭
  - `[...]`: 문자 클래스 매칭
  - `\x`: 이스케이프 문자

### 3. 정보 조회 (Introspection)
- **PUBSUB CHANNELS [pattern]**: 활성 채널 목록 조회
- **PUBSUB NUMSUB [channel ...]**: 채널별 구독자 수 조회
- **PUBSUB NUMPAT**: 패턴 구독 수 조회

## 자료구조

### PubSub 구조체
```rust
pub struct PubSub {
    channels: HashMap<Bytes, HashSet<ClientId>>,     // 채널 → 구독자 매핑
    patterns: HashMap<Bytes, HashSet<ClientId>>,     // 패턴 → 구독자 매핑
    clients: HashMap<ClientId, BroadcastSender>,     // 클라이언트 → 송신자 매핑
    client_channels: HashMap<ClientId, HashSet<Bytes>>,  // 클라이언트 → 채널 매핑
    client_patterns: HashMap<ClientId, HashSet<Bytes>>,  // 클라이언트 → 패턴 매핑
    pending_by_client: HashMap<ClientId, VecDeque<usize>>, // 클라이언트별 pending 메시지 크기
    client_buffer_capacity: AtomicUsize,             // 기본 1024, 테스트에서 축소 가능
    messages_dropped: AtomicU64,                     // full-buffer overwrite / failed send
}
```

### 핵심 설계 특징
1. **효율적인 메시지 전달**: `broadcast` 채널 사용으로 다중 구독자에게 효율적 전달
2. **패턴 매칭**: Redis 스타일 glob 패턴 지원
3. **자동 정리**: 클라이언트 연결 종료 시 자동으로 구독 정보·pending 메모리 정리
4. **동시성 안전**: `RwLock`과 `tokio::sync` 사용으로 스레드 안전성 보장
5. **Fan-out 메모리 한도**: publish 시 `message_size * max(1, N)` admission, 수신/종료 시 해제
6. **슬로우 클라이언트**: `RecvError::Lagged` 시 연결 종료, full buffer는 panic 없이 drop

## 사용 예제

### 기본 구독 및 발행
```bash
# 클라이언트 1
SUBSCRIBE news

# 클라이언트 2
PUBLISH news "Breaking news!"
# 반환: (integer) 1  # 메시지를 받은 클라이언트 수
```

### 패턴 구독
```bash
# 모든 뉴스 관련 채널 구독
PSUBSCRIBE news.*

# 매칭되는 채널에 발행
PUBLISH news.tech "Tech news"
PUBLISH news.sports "Sports update"
```

### 정보 조회
```bash
# 활성 채널 목록
PUBSUB CHANNELS
# 반환: 1) "news.tech"
#       2) "news.sports"

# 특정 채널의 구독자 수
PUBSUB NUMSUB news.tech news.sports
# 반환: 1) "news.tech"
#       2) (integer) 5
#       3) "news.sports"
#       4) (integer) 3

# 패턴 구독 수
PUBSUB NUMPAT
# 반환: (integer) 2
```

## 아키텍처

### 메시지 흐름
```
Publisher → PUBLISH → PubSub 시스템 → Broadcast Channels → Subscribers
                           ↓
                    Pattern Matching
                           ↓
                  Pattern Subscribers
```

### 네트워크 계층 통합
1. 클라이언트 연결 시 자동 등록
2. 명령 처리와 Pub/Sub 메시지 전송을 동시에 처리
3. `tokio::select!`를 사용한 비동기 메시지 멀티플렉싱
4. 연결 종료 시 자동 정리

### 핵심 컴포넌트

#### PatternMatcher
- Redis 스타일 glob 패턴 매칭 구현
- 재귀적 매칭 알고리즘
- 문자 클래스, 와일드카드, 이스케이프 지원

#### 클라이언트 관리
- 고유 ClientId 할당
- `broadcast::channel`을 통한 메시지 전달
- 구독 정보 추적 및 관리

## 성능 고려사항

### 최적화 전략
1. **Sharded Locks**: 채널별 독립적인 락으로 경합 감소
2. **Broadcast Channel**: 다중 수신자에게 효율적인 메시지 전파
3. **빠른 패턴 매칭**: 최적화된 glob 매칭 알고리즘
4. **메모리 효율**: 빈 채널/패턴 자동 제거

### 확장성
- 수천 개의 동시 구독자 지원
- 패턴 구독과 일반 구독 동시 지원
- 클라이언트당 여러 채널/패턴 구독 가능

## 제한사항

현재 구현의 제한사항:
1. 메시지 지속성 없음 (메모리 기반)
2. 메시지 순서 보장은 단일 채널 내에서만
3. 최대 구독 수는 메모리에 의해 제한

## 메모리·슬로우 클라이언트 정책 (fan-out)

### Fan-out admission (`maxmemory`)

`PUBLISH` / `SPUBLISH`는 메시지 1회 크기가 아니라 **예상 전달 횟수**를 반영해 메모리를 선점합니다.

- **비용**: `message_size * max(1, delivery_count)`
  - `delivery_count` = 채널 구독자 수 + 매칭 패턴 구독자 수 (둘 다 구독 시 2회 과금)
- **거부**: 비용이 `maxmemory` 여유를 넘으면 Redis 스타일 **OOM** (`Error::OutOfMemory`)
- **메시지 크기 상한**: 구독자 수와 무관하게 `MemoryTracker::max_message_size` 초과 시 `MessageTooLarge`

선점한 메모리는 발행 직후 즉시 해제하지 않습니다. 클라이언트별 pending 버퍼에 남아 있는 동안 `MemoryCategory::PubSub`에 계상됩니다.

### Pending 버퍼 수명

| 이벤트 | 동작 |
|--------|------|
| 성공적 publish → 클라이언트 큐 적재 | fan-out 비용만큼 allocate (이미 admission에서 선점) |
| 클라이언트가 메시지 수신 (`recv` 성공) | 해당 메시지 크기 deallocate |
| 버퍼 full로 오래된 메시지 overwrite | overwrite된 바이트 deallocate + `messages_dropped` 증가 |
| `RecvError::Lagged` | 유실분 deallocate 후 **슬로우 클라이언트 연결 종료** |
| 클라이언트 unregister / 연결 종료 | 잔여 pending 전부 deallocate |

### 클라이언트 버퍼 capacity

- 기본값: **1024** 메시지 (`DEFAULT_CLIENT_BUFFER_CAPACITY`)
- 설정: `PubSub::with_client_buffer_capacity(n)` 또는 `set_client_buffer_capacity(n)`
- capacity는 **`register_client` 시점**에 broadcast 채널에 적용됩니다 (이후 변경은 신규 클라이언트만)
- 테스트에서는 작은 값(예: 8)으로 full/lag 경로를 검증합니다

### Full buffer / Lagged 동작

- tokio `broadcast`는 버퍼가 가득 차도 **패닉하지 않고** 가장 오래된 슬롯을 덮어씁니다
- Kore는 동일 정책을 pending 크기 큐에 미러링하며, drop/overwrite를 `PubSub::messages_dropped()`로 집계합니다
- 네트워크 경로에서 `RecvError::Lagged(n)`이 나면 로그 후 해당 클라이언트를 disconnect 합니다 (슬로우 컨슈머 보호)

### 아직 남은 갭

- 발행 경로의 실제 힙 복제 비용(RESP 직렬화 버퍼 등)은 근사치이며 메시지 payload 크기 기준입니다
- multi-DB에서 pub/sub는 공유되지만 카테고리 메모리는 발행 시점의 DB `MemoryTracker`에 계상됩니다
- shard pub/sub(`SPUBLISH`)도 동일 admission을 쓰지만, 패턴 구독은 shard 경로에 포함되지 않습니다

## Redis와의 호환성

### 지원하는 명령어
- ✅ PUBLISH
- ✅ SUBSCRIBE
- ✅ UNSUBSCRIBE
- ✅ PSUBSCRIBE
- ✅ PUNSUBSCRIBE
- ✅ PUBSUB CHANNELS
- ✅ PUBSUB NUMSUB
- ✅ PUBSUB NUMPAT

### 미지원 기능
- ❌ PUBSUB SHARDCHANNELS (Redis 7.0+)
- ❌ PUBSUB SHARDNUMSUB (Redis 7.0+)
- ❌ Sharded Pub/Sub

## 테스트

단위 테스트 실행:
```bash
cargo test --test pubsub_test
```

통합 테스트 포함:
- 기본 구독/발행 테스트
- 패턴 구독 테스트
- 다중 채널 테스트
- 구독 해제 테스트
- 통계 정보 테스트
- 클라이언트 정리 테스트
- fan-out 메모리 스케일 / maxmemory 거부 / 누수 없음
- full buffer non-panic + Lagged 감지

## 향후 개선 방향

1. **메트릭스**: `messages_dropped` / pending bytes를 Prometheus·INFO에 노출
2. **ACL 통합**: 채널별 접근 제어
3. **메시지 압축**: 큰 메시지에 대한 압축 지원
4. **정밀 회계**: RESP 직렬화·채널 메타데이터까지 포함한 실힙 측정
5. **공유 MemoryTracker**: multi-DB에서 pub/sub pending을 프로세스 전역 한도로 통일
