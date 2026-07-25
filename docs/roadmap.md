# Roadmap

## Currently working on..

- [x] Support for Redis Pub-Sub
- **Post-FG queue** (unified keyspace facade shipped): next is **Batch FG-2** (physical single-map migrate, one type first). Details in root `TODO.md` → *Next work queue (post-FE)*.

## Persistence / search letter batches (recent)

Tracked in detail in root `TODO.md`. High level:

- [x] FT write classification, aliases, AOF/RDB FT schema (BT–BY, CA)
- [x] Scratch-load AOF/RDB (preserve target on Err); load quiesce + WATCH bump (CB–CD)
- [x] FT merge schema/alias equality on clash (CG); multi-DB LOADING gate (CK)
- [x] Shared FT.CREATE parser; HNSW `EF_CONSTRUCTION` AOF/RDB; ACL `@search` (CA, CE)
- [x] Multi-DB lock-step keyspace install (Batch DR: epoch write + stable-view read; LOADING + SYNC deny)
- [x] Multi-DB install panic rollback + walk audit (Batch DS: retain discards + Drop restore; AOF rewrite under epoch read; mid-fill single-DB residual)
- [x] MIGRATE absolute expire + residual closeout (Batch DT: PXAT/PEXPIREAT; LOADING/UI string-contract accepted)
- [x] Cluster ownership epoch + gossip (Batch DU: per-slot epoch; CLUSTER OWNERS/EPOCH; MEET+heartbeat merge)
- [x] Dest-first dual-end NODE + epoch fence (Batch DV: no MOVED-to-IMPORTING on dest fail; stale gossip reject)
- [x] Multi-master fail quorum (Batch DW: pfail→fail votes; FAILREPORTS; ≤2 masters single-observer)
- [x] Reshard planner (Batch DX: `CLUSTER RESHARD PLAN|AUTO`; greedy donors; remote RESP execute)
- [x] Multi-replica failover election (Batch DY: max replica id; ROLEMAP + MEETPEER role)
- [x] Loser re-point at election winner (Batch DZ: topology REPLICATE + REPLICAOF)
- [x] Offset-based replica election (Batch EA: ROLEMAP offset; max offset then max id)
- [x] Replica-priority election (Batch EB: `--cluster-replica-priority`; 0 never promote)
- [x] CLUSTER FAILOVER FORCE/TAKEOVER (Batch EC: operator manual promote + claim)
- [x] Cluster ops helpers (Batch ED: COUNTKEYSINSLOT/GETKEYSINSLOT/REPLICAS/BUMPEPOCH)
- [x] Slot bootstrap (Batch EE: ADDSLOTS/DELSLOTS/FLUSHSLOTS; unbound empty owner)
- [x] Slot range bootstrap (Batch EF: ADDSLOTSRANGE/DELSLOTSRANGE)
- [x] Topology forget/reset (Batch EG: FORGET; RESET SOFT|HARD key wipe)
- [x] Dual-end NODE partial_source ASK safety (Batch EH: re-assert MIGRATING; COUNT-FAILURE-REPORTS)
- [x] CLUSTER SHARDS / LINKS (Batch EI: Redis-7 shard view; LINKS empty no bus)
- [x] Post-commit dual NODE verify + MYSHARDID (Batch EJ; partial_verify status)
- [x] CLUSTER SET-CONFIG-EPOCH (Batch EK; only if epoch > current)
- [x] Live cluster CONFIG (Batch EL: cluster-replica-priority + cluster-node-timeout)
- [x] CLUSTER SAVECONFIG (Batch EM: write nodes.conf)
- [x] nodes.conf load-on-boot (Batch EN: restore id/peers/slots/epoch; fallback single-node)
- [x] nodes.conf autosave (Batch EO: topology-mutating CLUSTER ops + failover claim)
- [x] Dual-end NODE dest rollback (Batch EP: source NODE fail → `rolled_back` + IMPORTING)
- [x] cluster-require-full-coverage (Batch EQ: cluster_state fail + CLUSTERDOWN gate; CONFIG/CLI)
- [x] Cluster READONLY replica reads (Batch ER: serve master slots; writes still MOVED)
- [x] cluster-allow-reads-when-down (Batch ES: reads while cluster_state fail; writes blocked)
- [x] CLUSTER SLOTS lists replicas (Batch ET: master then replica endpoints per range)
- [x] cluster-announce-ip/port (Batch EU: client-facing addr in NODES/MEET/MOVED myself)
- [x] CLUSTER SLOT-STATS (Batch EV: SLOTSRANGE key-count + ORDERBY/LIMIT)
- [x] Sentinel-lite (Batch EW: MONITOR/s_down/GET-MASTER-ADDR/FAILOVER; no multi-sentinel ODOWN)
- [x] Sentinel ODOWN quorum (Batch EX: MEET + IS-MASTER-DOWN-BY-ADDR votes; failover on o_down)
- [x] Dual-end NODE preflight (Batch EY: prepare MYID+owner check; failed_preflight)
- [x] Dual-end NODE wire 2PC slice 1 (Batch FB: SETSLOT PREPARE/ABORTPREPARE + dest-first commit; failed_prepare)
- [x] Sentinel conf persistence (Batch EZ: FLUSHCONFIG + load sentinel.conf; autosave)
- [x] Sentinel hello bus lite (Batch FA: HELLO CSV + PUBLISH + peer exchange)
- [x] Sentinel promote-success gate (Batch FC: FAILOVER/REPLICAOF/ROLE=master; failover_in_progress)
- [x] HNSW graph-based ANN search (Batch CQ; layer-0 edges + `ef_search`)
- [x] HNSW remove/unlink + insert-time force-keep + update rewire (Batch CS)
- [x] HNSW hard-delete bridge repair — closest-peer reconnect (Batch CT; 2-chain)
- [x] HNSW undirected former snapshot + multi-way spanning bridge reconnect (Batch CU)
- [x] HNSW NN-path bridge branch test + must_keep prune safety (Batch CW)
- [x] HNSW recall@k unit gate + N=300 indicative micro (Batch CV; not large-N ANN win)
- [x] HNSW tighter recall gate + optional larger-N median bench (Batch DK; `#[ignore]` N=5000)
- [x] HNSW multi-layer insert (Batch FF: geometric level assignment + upper-layer SEARCH-LAYER / connect; query descent; edges/levels not AOF/RDB durable)

## Plans

- [x] Cluster (kore cluster) — MVP: hash slots + MOVED/CROSSSLOT/ASK + CLUSTER/ASKING stubs (single-node)
- [x] Cluster gossip / membership + thin failover (RESP MEET/PING, single-observer fail, replica claim slots)
- [x] Cluster thin slot reshard (`CLUSTER MIGRATEKEYS` multi-type keys + SETSLOT operator flow)
- [x] Cluster reshard orchestration slice (Batch DM: `CLUSTER RESHARD` source-side 4-step flow + range; dual-end NODE best-effort, not atomic)
- [x] Cluster dual-end NODE harden (Batch DN: verify+retry after RESHARD; `CLUSTER RESHARD FINISH` NODE-only recovery; still not 2PC)
- [x] Cluster RESHARD honesty (Batch DO: partial `failed_keys` counts; range abort-on-partial; FINISH source-keys warning; source-before-dest NODE window docs)
- [x] Redis key-level `MIGRATE` (Batch DP: COPY/REPLACE/AUTH/KEYS/timeout; shared recreate path with MIGRATEKEYS; no DUMP/RESTORE)
- [x] MIGRATE honesty (Batch DQ: multi-key IOERR `migrated=`/`skipped=`; typed TTL via SET PX / trailing PEXPIRE)
- [x] MIGRATE absolute expire (Batch DT: snapshot unix-ms end; string `SET PXAT` / typed `PEXPIREAT`; remaining-ms shrink closed)
- [x] Cluster automatic reshard / multi-type MIGRATE orchestration (MVP complete: PLAN/AUTO DX, epoch gossip DU, dest-first NODE DV, fail quorum DW, EY preflight, EP dest rollback, **FB RESP prepare/commit 2PC slice**)
- [x] Sentinel promote-success gate (Batch FC: real promote required; in-process failover_in_progress)
- [x] **FD** measured benchmarks vs Valkey (`docs/benchmarks.md`; single-host median of 3; no portable-win claims)
- [x] Sentinel leader election depth (Batch FE: voted-leader / elect gate; residuals: election-timeout, hello SUBSCRIBE, CKQUORUM live)
- [x] HNSW multi-layer insert (Batch FF)
- [x] Unified keyspace design + `KeyValue` facade (Batch FG slice A; multi-map storage)
- [ ] **Next (see `TODO.md` Next work queue post-FE):** **FG-2** physical single-map migrate · later NODE 2PC slice 2 / Sentinel promote rank / `nodes.conf` flags
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
- [x] Sentinel-lite (Batch EW: subjective-down + manual/auto failover; not full Sentinel)
- [x] Sentinel multi-instance ODOWN lite (Batch EX: MEET peers + vote quorum)
- [x] Sentinel FLUSHCONFIG / load-on-boot (Batch EZ)
- [x] Sentinel hello bus lite (Batch FA: peer HELLO + PUBLISH on master)
- [x] Sentinel cross-process leader election lite (Batch FE; residuals: election-timeout SM, hello SUBSCRIBE)
- [x] Blocking `XREAD` / `XREADGROUP` (`BLOCK`)
- [x] Multi-DB replication parity (SYNC/PSYNC all DBs + SELECT apply)
- [x] AOF SELECT concurrency fix + atomic SELECT+cmd replication
- [x] CI (GitHub Actions) + benchmarks runbook
- [x] ACL MVP + TLS + metrics/HEALTH
- [x] Coordinated `FAILOVER TO`
- [x] Redlock CLI wiring; FT.SEARCH RESP + search memory; pub/sub fan-out limits
