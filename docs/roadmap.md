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
- [x] HNSW hard-delete bridge repair — closest-peer reconnect (Batch CT; 2-chain)
- [x] HNSW undirected former snapshot + multi-way spanning bridge reconnect (Batch CU)
- [x] HNSW NN-path bridge branch test + must_keep prune safety (Batch CW)
- [x] HNSW recall@k unit gate + N=300 indicative micro (Batch CV; not large-N ANN win)
- [x] HNSW tighter recall gate + optional larger-N median bench (Batch DK; `#[ignore]` N=5000)

## Plans

- [x] Cluster (kore cluster) — MVP: hash slots + MOVED/CROSSSLOT/ASK + CLUSTER/ASKING stubs (single-node)
- [x] Cluster gossip / membership + thin failover (RESP MEET/PING, single-observer fail, replica claim slots)
- [x] Cluster thin slot reshard (`CLUSTER MIGRATEKEYS` multi-type keys + SETSLOT operator flow)
- [x] Cluster reshard orchestration slice (Batch DM: `CLUSTER RESHARD` source-side 4-step flow + range; dual-end NODE best-effort, not atomic)
- [x] Cluster dual-end NODE harden (Batch DN: verify+retry after RESHARD; `CLUSTER RESHARD FINISH` NODE-only recovery; still not 2PC)
- [ ] Cluster automatic reshard / full multi-type MIGRATE orchestration (residuals: true atomic dual-end NODE / epoch ownership gossip / Redis `MIGRATE` / planner)
- [x] 데드락 감지 고급 기능
    - [x] 크로스 프로세스 감지 (Batch DC–DE snapshot merge MVP; no transport)
    - [x] 비동기(async) 지원
    - [x] 커스텀 희생자 선택 전략
    - [x] 웹 UI 모니터링 (Batch DF–DJ; residual: string-only repaint test — see TODO.md)
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
