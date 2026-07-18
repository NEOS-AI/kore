# Roadmap

## Currently working on..

- [x] Support for Redis Pub-Sub

## Persistence / search letter batches (recent)

Tracked in detail in root `TODO.md`. High level:

- [x] FT write classification, aliases, AOF/RDB FT schema (BT–BY, CA)
- [x] Scratch-load AOF/RDB (preserve target on Err); load quiesce + WATCH bump (CB–CD)
- [x] FT merge schema/alias equality on clash (CG); multi-DB LOADING gate (CK)
- [x] Shared FT.CREATE parser; HNSW `EF_CONSTRUCTION` AOF/RDB; ACL `@search` (CA, CE)
- [ ] True multi-DB atomic keyspace install under concurrent readers (residual; LOADING gate mitigates)
- [x] HNSW graph-based ANN search (Batch CQ; layer-0 edges + `ef_search`; multi-layer insert still simplified)
- [x] HNSW remove/unlink + insert-time force-keep + update rewire (Batch CS)
- [x] HNSW hard-delete bridge repair — reconnect former neighbors on remove (Batch CT)
- [ ] HNSW recall@k / throughput numbers vs FLAT (methodology in `docs/benchmarks.md`)

## Plans

- [x] Cluster (kore cluster) — MVP: hash slots + MOVED/CROSSSLOT/ASK + CLUSTER/ASKING stubs (single-node)
- [x] Cluster gossip / membership + thin failover (RESP MEET/PING, single-observer fail, replica claim slots)
- [x] Cluster thin slot reshard (`CLUSTER MIGRATEKEYS` multi-type keys + SETSLOT operator flow)
- [ ] Cluster automatic reshard / full multi-type MIGRATE orchestration
- [ ] 데드락 감지 고급 기능
    - [ ] 크로스 프로세스 감지
    - [ ] 비동기(async) 지원
    - [ ] 커스텀 희생자 선택 전략
    - [ ] 웹 UI 모니터링
- [x] Export data to file
    - [x] Export to 'RDB' file (Kore `KORDB` format; `SAVE` / `BGSAVE`)
    - [x] Export to 'AOF' file (RESP log; `BGREWRITEAOF`)
- [x] Load data from file (init with file)
- [x] Async replication (`SYNC` + `REPLICAOF` / `--replicaof`)
- [x] PSYNC partial resync + backlog; replica read path (`ROLE` / `INFO replication`)
- [x] Multi-DB + streams RDB/AOF persistence (KORDB v3)
- [x] Minimal failover promote (`REPLICAOF NO ONE` / `FAILOVER`)
- [x] Blocking `XREAD` / `XREADGROUP` (`BLOCK`)
- [x] Multi-DB replication parity (SYNC/PSYNC all DBs + SELECT apply)
- [x] AOF SELECT concurrency fix + atomic SELECT+cmd replication
- [x] CI (GitHub Actions) + benchmarks runbook
- [x] ACL MVP + TLS + metrics/HEALTH
- [x] Coordinated `FAILOVER TO`
- [x] Redlock CLI wiring; FT.SEARCH RESP + search memory; pub/sub fan-out limits
