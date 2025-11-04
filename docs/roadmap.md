# Roadmap

## Currently working on..

- [x] 분산락 관련 기능
    - [x] 분산락 관련 명령어 지원 (SETNX, GETDEL, GETEX)
    - [x] 기본 분산락 패턴 구현 및 문서화
    - [x] Redlock 알고리즘 구현
        - [x] 다중 인스턴스 지원
        - [x] 쿼럼 기반 락 획득
        - [x] 자동 재시도 및 백오프
        - [x] 락 연장(extend) 기능
        - [x] Clock drift 보정
        - [x] 자동 락 해제 (Drop trait)
    - [x] 데드락 감지 (Deadlock Detection)
        - [x] Wait-for 그래프 기반 감지
        - [x] DFS 순환 탐지 알고리즘
        - [x] 자동 희생자 선택 및 해제
        - [x] 락 소유권 및 대기 추적
        - [x] 통계 및 모니터링
        - [x] Redlock 통합

## Plans

- [ ] Support for Redis Pub-Sub
- [ ] Geospatial data type support
    - [ ] GEOADD command support
    - [ ] GEOSEARCH commmand support
- [ ] Cluster (kore cluster)
- [ ] Redlock 추가 기능
    - [x] 데드락 자동 감지
    - [ ] Lua 스크립트 지원
    - [ ] 락 모니터링 대시보드
    - [ ] Fair lock queuing
- [ ] 데드락 감지 고급 기능
    - [ ] 크로스 프로세스 감지
    - [ ] 비동기(async) 지원
    - [ ] 커스텀 희생자 선택 전략
    - [ ] 웹 UI 모니터링
