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
    - [ ] Readlock 고급 기능 지원
        - [ ] Lua 스크립트 지원
        - [x] Fair lock queueing
            - [x] FIFO 순서 보장
            - [x] 우선순위(priority) 지원
            - [x] 큐 통계 및 모니터링
            - [x] Starvation 방지
            - [x] 큐 크기 제한

- [x] Geospatial data type support
    - [x] GEOADD command support
    - [x] GEOSEARCH commmand support
    - [x] Tracking the geospatial commands

## Plans

- [ ] Support for Redis Pub-Sub
- [ ] Cluster (kore cluster)
- [ ] 데드락 감지 고급 기능
    - [ ] 크로스 프로세스 감지
    - [ ] 비동기(async) 지원
    - [ ] 커스텀 희생자 선택 전략
    - [ ] 웹 UI 모니터링
- [ ] Export data to file
    - [ ] Export to 'RDB' file
    - [ ] Export to 'AOF' file
- [ ] Load data from file (init with file)
