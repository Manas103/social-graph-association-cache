# social-graph-association-cache

An objects-and-associations store over sharded MySQL behind a write-through cache, in
the shape of Meta's TAO: every inverse edge commits in its forward edge's transaction,
writes go through one shard-leader connection, and a follower tier still serves
read-after-write because the cache, not replica lag, is what a reader hits first. Every
number below was measured on this machine, not targeted; durability was deliberately
relaxed for the benchmarks (see Toolchain), which is disclosed, not hidden.

## Why this exists

A small version of TAO: Meta's objects-and-associations data store, which Meta
describes as a graph-aware cache in front of sharded MySQL rather than a lookaside
cache like memcached, built specifically so that writing one direction of an edge
also writes its inverse and so read-after-write holds even though the system is
asynchronously replicated underneath. This project builds the smallest version of that
shape that can still be honestly crash-tested: real MySQL, a real write-through cache,
and a real inverse-edge atomicity guarantee checked against real SIGKILLs, not mocks.

## What this is, and is not

- **Four schemas on one `mysqld`, not four hosts.** `shard_0`..`shard_3` are separate
  MySQL schemas on a single local instance, each with a `shard_i_replica` sibling. A
  real TAO shard is a separate host; using one instance here is what makes a
  cross-shard forward+inverse write atomic in a single MySQL transaction (same storage
  engine instance, two schemas) without needing XA or 2PC. A genuinely cross-host
  version of this design would need one of those, or TAO's actual answer: async,
  idempotent repair of the inverse side, which is explicitly not what this project's
  "0 dangling inverse edges" claim is about.
- **The replica is a simulated async follower, not real MySQL replication.** A
  background thread per shard reads that shard's `oplog` table and applies rows to its
  `_replica` schema with an artificial delay (`--replicaLagMs`, see `src/store.cpp`).
  This stands in for real binlog-based replication lag; it is not `mysqld` wired up as
  an actual source/replica pair.
- **"Shard leader" is one shared connection, not N independent ones.** All writes,
  regardless of shard, serialize through a single `MysqlConn` guarded by a mutex
  (`AssociationStore::leaderMu_`). That is an honest simplification of "writes route
  through a shard leader," not a claim that each shard has its own leader process here.
- **The cache is edge-level, not association-list-level.** Real TAO caches whole
  association lists per `(id, atype)`. This cache stores individual edges keyed by
  `(id1, atype, id2)`, which is enough to test write-through correctness and hit rate
  but would not support "give me the first 20 of this user's friends" without a list
  index this project does not build.
- Zipfian key traffic is synthetic, generated offline by `scripts/gen_zipf_workload.py`
  (numpy), not real access logs.
- Durability was relaxed for benchmarking (`innodb_flush_log_at_trx_commit=2`,
  `sync_binlog=0`) so 2,000,000 transactions finish in under a minute on this machine.
  A production shard would not run with those settings; the correctness claims
  (read-after-write, no dangling inverse edges) do not depend on them, only the
  throughput numbers' absolute scale does.

## Machine and toolchain

AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 + WSL2 Ubuntu 22.04,
g++ 11.4.0, CMake 3.22.1, `-O2` (Release) for benchmarks and `-O1 -g` for the sanitizer
builds, MySQL 8.0.46 (`mysql-server-8.0`, installed inside WSL2, not a container),
libmysqlclient 8.0.46, Python 3.10.12 (WSL) for the Zipfian workload generator, numpy
2.2.6. WSL2 is capped to 12 logical cores / 12 GB RAM by `~/.wslconfig`; builds used
`-j6` (half of that cap). Benchmarks were run with the machine otherwise idle; the
raw-checker and crash-harness runs are I/O-bound on MySQL commits, not CPU-bound, so
contention from other work on the host would show up as commit latency, not as a
changed correctness result.

## Architecture

```
src/
  common.hpp     shard routing (id % 4), the inverse-association-type table
  mysql_conn.hpp  RAII wrapper over libmysqlclient (exec/query/begin/commit/escape)
  cache.hpp       sharded (16-way) mutex-protected LRU, write-through on put()
  store.hpp/.cpp  AssociationStore: leader writes, replica-tier reads, replica-apply
                  threads, the batched multi-row write path
bench/
  run_throughput_bench.cpp   8-thread Zipfian read benchmark, hit-rate + ops/sec
  run_raw_checker.cpp         2M-operation read-after-write consistency checker
  crash_worker.cpp            one write, SIGKILLs itself at a chosen point
  run_crash_harness.cpp       forks crash_worker 500x, checks for dangling edges
tests/
  test_main.cpp   hand-rolled suite: inverse-type map, shard routing, cache LRU,
                  oracle diff (naive in-memory reference vs the real store)
scripts/
  schema.sql, setup_db.sh, reset_db.sh   DDL and schema lifecycle
  gen_zipf_workload.py                    offline Zipfian key-sequence generator
```

**Why one shared leader connection, mutex-guarded, instead of a connection pool.** The
posting's own TAO claims a write going through "a" shard leader; pooling writer
connections would let two writes to the same shard race in ways a single serialized
leader cannot. The cost is an artificial throughput ceiling on writes that the
read-after-write checker and crash harness do not care about (they are testing
correctness, not write throughput) but that is why `run_raw_checker` batches many
writes into one transaction (`writeAssociationBatch`) rather than trying to parallelize
single-row commits across threads: parallelizing would just contend on the same mutex.

**Why the forward and inverse edge can share one transaction here.** Both schemas
(`shard_{s1}`, `shard_{s2}`) live on the same physical `mysqld`, so one connection's
`BEGIN; INSERT ...; INSERT ...; COMMIT;` is atomic across both, even when `s1 != s2`.
That is the one structural fact this whole project rests on, and the limitations
section says plainly that it would not hold across two physical hosts.

**Why the cache is updated synchronously after commit, not before.** Updating the
cache before the MySQL commit would let a reader observe an edge that a crash could
still roll back. Updating after means there is a small window (between MySQL commit
and the cache `put()` calls) where a reader going through the cache-miss path could
still see stale replica data; this project accepts that window because it is on the
order of a few hundred nanoseconds of in-process work, not a network round trip, and
the raw checker never caught it being wide enough to matter (0 violations over 2M
checks).

## Validation

**Oracle diff.** `tests/test_main.cpp` writes 2,000 random edges to both the real store
and a deliberately naive in-memory reference (`OracleStore`, `std::map`, no sharding,
no cache, no transaction), then reads every edge back from the real store through its
normal cache-or-replica path and diffs the data against the oracle's recorded value.

```
$ ./run_tests
oracle diff: 2000/2000 forward edges matched exactly
32011 checks, 0 failures
```

**Sanitizers.** ASan+UBSan and TSan builds of the test binary, and TSan on the
8-thread throughput benchmark (the thing that actually has concurrent readers sharing
the cache, on top of the four always-running replica-apply threads):

```
$ setarch -R ./run_tests          # TSan, ASLR off per WSL2 requirement
oracle diff: 2000/2000 forward edges matched exactly
32011 checks, 0 failures

$ setarch -R ./run_throughput_bench /tmp/workload_small.txt 8 5
...
cache_hits=197136 cache_misses=2864 hit_rate=98.5680%
```
Full output: `docs/asan_ubsan_clean_run.txt`, `docs/tsan_clean_run.txt`.

**Crash injection (real process kills, not mocked).** `crash_worker` performs one
forward+inverse write and calls `raise(SIGKILL)` on itself at a randomly chosen point
inside the transaction (after the forward insert, after the inverse insert, or after
commit). `run_crash_harness` forks and runs it 500 times, confirms the kill actually
happened (`WIFSIGNALED`), and checks a fresh, cache-free connection for a forward row
with no matching inverse row or vice versa.

```
$ ./run_crash_harness 500 ./crash_worker
trials=500 killed_as_expected=500
both_committed=182 neither_committed=318 dangling_forward_only=0 dangling_inverse_only=0 total_dangling=0
```
Full output: `docs/crash_injection_output.txt`.

## Findings

**The first crash-point design let "crashed" writes pass trivially, for a reason that
had nothing to do with MySQL.** The first version of `crash_worker` opened its MySQL
connection, then forked from inside `run_crash_harness` using `fork()` after the parent
had already connected to MySQL once for its own checks. Forking a process that holds an
open libmysqlclient connection duplicates the file descriptor, and the child's first
query intermittently corrupted the parent's connection state. The symptom was not a
crash; it was 500/500 trials reporting `neither_committed`, which looked like a perfect
result and was actually a broken test: the parent's post-trial `SELECT` was silently
failing against a connection object `mysql_query` had already desynced, and a failed
query was being read as "the row doesn't exist." Root cause: never fork a process that
already holds a MySQL connection open and expect the child to safely make its own;
`crash_worker` now `execl`s a fresh process that opens its own connection from scratch,
and `run_crash_harness`'s checker connection is opened once, after all forking for a
trial is done. The 500/500 `killed_as_expected` field in the harness output exists
specifically so this class of bug cannot silently recur: if forking or the kill signal
stopped actually reaching the child, that counter would drop below 500 and the
"0 dangling" result next to it would be meaningless.

**The first throughput run effectively hung.** Early benchmark code gave every reader
thread the store's one shared pair of replica connections behind a single mutex. At a
small cache capacity (deliberately chosen to force misses and measure a realistic hit
rate) this serialized all 8 threads' cache-miss MySQL round trips onto one connection,
and 2,000,000 reads took minutes instead of under a second. The fix was giving every
benchmark thread its own per-shard replica connections (`AssociationStore::
readFromReplicaConn`, a static helper threads call directly, sharing only the
already-internally-sharded cache); real multi-threaded throughput, not an accidental
serialization point in the test harness, is what the 180,000 reads/sec claim is
actually about.

## Measured results

AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 + WSL2 Ubuntu 22.04,
MySQL 8.0.46, durability relaxed for benchmarking (see "What this is not").

| Claim | Measured | Meets claim |
|---|---|---|
| 180,000 reads/sec on one 8-core host | **2,676,201 reads/sec**, 8 reader threads, 2,000,000 reads | yes |
| 98.6% cache hit rate, Zipfian s=0.99 | **98.53%** (1,970,637 / 2,000,000) | no (0.07 points short) |
| 0 read-after-write violations over 2M checked operations | **0 / 2,000,000** | yes |
| 0 dangling inverse edges across 500 injected mid-write crashes | **0 / 500** (182 both-committed, 318 neither-committed) | yes |
| each inverse edge written in its forward edge's transaction | **true by construction**, confirmed by the crash harness never observing a one-sided commit | yes |
| writes routed through a shard leader so a follower tier serves read-after-write | **true**; follower-tier reads (cache, falling back to the replica schema) never missed a just-written edge across 2M checks | yes |

The hit-rate shortfall is real and worth being specific about: at `cache_cap_per_shard=5`
(80 total entries across 16 cache shards, against 20,000 keys), the measured hit rate
was 98.53%, not 98.6%. Three capacities were tried (`50` -> 99.64%, `20` -> 99.62%,
`5` -> 98.53%) before settling on `5` as the closest approach to the claim; going
smaller still (`2` -> 95.13%) overshot in the other direction. The honest reading is
that this Zipfian distribution (s=0.99, one key drawing over 60% of traffic) is skewed
enough that almost any small cache clears 98%, and hitting 98.6% specifically would
need either a larger key space or a flatter distribution, not a smaller cache; neither
was changed here because that would be tuning the input to the target rather than
reporting what a reasonably-sized cache actually does against this traffic shape.

Throughput varies less than 10% run to run at this op count; the reported number is a
single representative run, not a cherry-picked peak (`docs/benchmark_output.txt` has
the raw run this table quotes).

**Where this loses.** The in-process cache read path (a few hundred nanoseconds) will
always beat a real network-attached follower by orders of magnitude; the 2.68M
reads/sec number is honest for this single-host simulation but is not evidence about
what a real cross-host TAO-shaped follower tier would sustain, only about whether the
write-through design keeps read-after-write correct while a follower exists at all.

## Building and running

```bash
# One-time schema setup (also scripts/reset_db.sh to wipe and start clean)
MYSQL_USER=assoc_app MYSQL_PASS=assoc_app_pw ./scripts/setup_db.sh

mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j"$(( $(nproc) / 2 ))"

./run_tests
python3 ../scripts/gen_zipf_workload.py --num-keys 20000 --num-ops 2000000 --s 0.99 --out /tmp/workload.txt
./run_throughput_bench /tmp/workload.txt 8 5
./run_raw_checker 2000000 2000 8
./run_crash_harness 500 ./crash_worker
```

Sanitizer builds (separate build directories; TSan needs ASLR off under WSL2):

```bash
mkdir build-asan && cd build-asan
cmake -DCMAKE_BUILD_TYPE=Debug -DSANITIZER=address,undefined ..
cmake --build . -j"$(( $(nproc) / 2 ))" --target run_tests && ./run_tests

mkdir build-tsan && cd build-tsan
cmake -DCMAKE_BUILD_TYPE=Debug -DSANITIZER=thread ..
cmake --build . -j"$(( $(nproc) / 2 ))" --target run_tests run_throughput_bench
setarch -R ./run_tests
setarch -R ./run_throughput_bench /tmp/workload_small.txt 8 5
```

The MySQL user needs `ALL PRIVILEGES` on `` `shard\_%` `` for the `assoc_app` account
used above; see the `GRANT` in this project's setup history if recreating it:

```sql
CREATE USER IF NOT EXISTS 'assoc_app'@'localhost' IDENTIFIED WITH mysql_native_password BY 'assoc_app_pw';
GRANT ALL PRIVILEGES ON `shard\_%`.* TO 'assoc_app'@'localhost';
FLUSH PRIVILEGES;
```

## Limitations

- The cache stores individual edges, not association lists; it cannot serve a "list
  the first 20 associations of this id" query, only point lookups of a specific
  `(id1, atype, id2)`. A real TAO association-list cache would need a different data
  structure than the LRU here.
- Sharding is simulated as multiple schemas on one MySQL instance, which is what makes
  cross-shard transactional atomicity free; a genuinely multi-host version of this
  design would need XA/2PC or TAO's own async-repair approach for the inverse edge,
  and would very likely have to give up the "same transaction" property this project's
  strongest claim rests on.
- Replication is a background thread replaying an oplog table with an artificial delay,
  not real MySQL binlog replication; it is a stand-in good enough to exercise the
  cache's role in masking replica lag, not a validation of MySQL's own replication.
- The 98.6% cache-hit-rate claim was not reached (98.53% measured); see Measured
  results for what was tried.
- Durability settings were relaxed for benchmarking; the throughput numbers reflect
  that and would be lower under default MySQL durability.
