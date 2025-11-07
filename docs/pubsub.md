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
}
```

### 핵심 설계 특징
1. **효율적인 메시지 전달**: `broadcast` 채널 사용으로 다중 구독자에게 효율적 전달
2. **패턴 매칭**: Redis 스타일 glob 패턴 지원
3. **자동 정리**: 클라이언트 연결 종료 시 자동으로 구독 정보 정리
4. **동시성 안전**: `RwLock`과 `tokio::sync` 사용으로 스레드 안전성 보장

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

## 향후 개선 방향

1. **Sharded Pub/Sub**: Redis 7.0 스타일 샤드 채널 지원
2. **메시지 버퍼링**: 느린 구독자를 위한 백프레셔 처리
3. **메트릭스**: Prometheus 메트릭스 추가
4. **ACL 통합**: 채널별 접근 제어
5. **메시지 압축**: 큰 메시지에 대한 압축 지원
