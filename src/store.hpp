#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cache.hpp"
#include "common.hpp"
#include "mysql_conn.hpp"

namespace assoc {

struct DbConfig {
    std::string user = "assoc_app";
    std::string pass = "assoc_app_pw";
    std::string socket_path; // e.g. /var/run/mysqld/mysqld.sock
};

// AssociationStore is the object+association API: one leader connection used for all
// writes (see README for why one physical server makes that an honest simplification),
// one replica connection per shard used for cache-miss reads, and a write-through cache
// in front of both. A background thread per shard applies that shard's oplog to its
// replica schema with an artificial delay, standing in for real async MySQL replication
// lag (see README, "what this is not").
class AssociationStore {
public:
    AssociationStore(const DbConfig& cfg, int replicaLagMs, size_t cacheCapacityPerShard);
    ~AssociationStore();

    // Writes the forward edge (id1,atype,id2) and its inverse (id2,inverse(atype),id1)
    // in one transaction on the leader connection, appends both to their shards'
    // oplogs for replica apply, then updates the cache synchronously. Returns false
    // (and leaves no trace in MySQL) if `inject` requests a crash that this call
    // reaches; AssociationStore itself never crashes the process -- that is only done
    // by bench/crash_worker.cpp, which bypasses this class entirely so it can call
    // raise(SIGKILL) at a precise point. See CrashPoint there.
    bool writeAssociation(int64_t id1, const std::string& atype, int64_t id2,
                           const std::string& data, int64_t time_ms);

    // Same contract as writeAssociation, but for many edges in one transaction, with
    // the per-shard rows for each of assoc and oplog sent as one multi-row INSERT
    // instead of one row at a time. Used only by the 2M-operation read-after-write
    // checker, which would otherwise spend most of its wall time on per-statement
    // round trips rather than on what it is actually measuring. The atomicity and
    // cache-population guarantees are identical to writeAssociation; this only
    // changes how many MySQL round trips it costs to provide them.
    bool writeAssociationBatch(const std::vector<Edge>& edges);

    // Follower-tier read: cache first, then the shard's replica schema on miss
    // (backfilling the cache). This is the path the read-after-write checker exercises.
    std::optional<Edge> readAssociation(int64_t id1, const std::string& atype, int64_t id2);

    // Bypasses the cache and reads straight from the shard's leader schema. Used by
    // the oracle test and the crash harness, which must see ground truth, not cache state.
    std::optional<Edge> readAssociationFromLeader(int64_t id1, const std::string& atype,
                                                   int64_t id2);

    uint64_t cacheHits() const { return cache_.hits(); }
    uint64_t cacheMisses() const { return cache_.misses(); }

    WriteThroughCache& cache() { return cache_; }
    const DbConfig& dbConfig() const { return cfg_; }

    // Reads a single shard's replica schema directly, bypassing the cache. Public so
    // multi-threaded benchmarks can open their own per-thread connections (a MysqlConn
    // handle is not safe to share across threads) while still reusing the shared cache
    // returned by cache(). Static: it needs no instance state besides the connection
    // and shard number it is given.
    static std::optional<Edge> readFromReplicaConn(MysqlConn& conn, int shard, int64_t id1,
                                                     const std::string& atype, int64_t id2);

    void stopReplicaThreads();

private:
    std::optional<Edge> readFromSchema(const std::string& schema, int64_t id1,
                                        const std::string& atype, int64_t id2);
    void replicaApplyLoop(int shard);

    DbConfig cfg_;
    int replicaLagMs_;
    std::unique_ptr<MysqlConn> leader_;
    std::vector<std::unique_ptr<MysqlConn>> replicas_; // one per shard, used by readAssociation
    WriteThroughCache cache_;
    std::vector<std::thread> replicaThreads_;
    std::atomic<bool> stop_{false};
    std::mutex leaderMu_;  // the leader connection is shared; serialize its use
    std::mutex replicaMu_; // serializes replicas_ (readAssociation is called from one thread
                            // at a time in this project's tests; benchmarks use their own
                            // connections via readFromReplicaConn instead of this path)
};

} // namespace assoc
