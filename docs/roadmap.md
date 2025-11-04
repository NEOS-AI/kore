# Roadmap

## Currently working on..

## Completed

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

## Plans

- [ ] Support for Redis Pub-Sub
- [ ] Geospatial data type support
    - [ ] GEOADD command support
    - [ ] GEOSEARCH commmand support
- [ ] Cluster (kore cluster)
- [ ] Redlock 추가 기능
    - [ ] Lua 스크립트 지원
    - [ ] 락 모니터링 및 메트릭
    - [ ] 데드락 자동 감지
    - [ ] Fair lock queuing
