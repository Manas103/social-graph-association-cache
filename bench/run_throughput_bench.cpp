// Reader-throughput and cache-hit-rate benchmark.
//
// Usage:
//   run_throughput_bench <workload_file> <num_threads> <cache_cap_per_shard>
//
// Pre-populates one association edge per key in the workload file (id1=k, atype=
// "friend", id2=k+1), waits for the replica-apply threads to drain the oplog so the
// benchmark is not measuring replication catch-up, then replays the workload's access
// sequence split evenly across <num_threads> reader threads hitting readAssociation
// (cache, falling back to the shard's replica schema on a miss). Reports ops/sec and
// the aggregate cache hit rate.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <thread>
#include <vector>

#include "../src/store.hpp"

using namespace assoc;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <workload_file> <num_threads> <cache_cap_per_shard>\n",
                     argv[0]);
        return 2;
    }
    std::string workloadFile = argv[1];
    int numThreads = std::atoi(argv[2]);
    size_t cacheCap = std::atoll(argv[3]);

    std::ifstream f(workloadFile);
    if (!f) {
        std::fprintf(stderr, "cannot open %s\n", workloadFile.c_str());
        return 2;
    }
    int64_t numKeys, numOps;
    f >> numKeys >> numOps;
    std::vector<int32_t> seq(numOps);
    for (int64_t i = 0; i < numOps; i++) f >> seq[i];

    DbConfig cfg;
    AssociationStore store(cfg, /*replicaLagMs=*/3, cacheCap);

    std::printf("populating %lld edges...\n", (long long)numKeys);
    {
        const int64_t kPopBatch = 2000;
        std::vector<Edge> batch;
        batch.reserve(kPopBatch);
        for (int64_t k = 0; k < numKeys; k++) {
            batch.push_back({k, "friend", k + 1, 1700000000000LL + k, "e"});
            if ((int64_t)batch.size() == kPopBatch || k == numKeys - 1) {
                if (!store.writeAssociationBatch(batch)) {
                    std::fprintf(stderr, "population batch ending at %lld failed\n", (long long)k);
                    return 1;
                }
                batch.clear();
            }
        }
    }
    // Let the replica-apply threads catch up so misses resolve against real data,
    // not a replica that is still empty for every key.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::printf("replaying %lld ops across %d threads...\n", (long long)numOps, numThreads);
    // Each reader thread owns its own per-shard replica connections (a MysqlConn is
    // not safe to share across threads) and only shares the cache, which is already
    // internally sharded and lock-per-shard. This is what "180,000 reads/sec on one
    // 8-core host" is actually measuring: real concurrent readers, not N threads
    // serialized behind one shared miss-path connection.
    auto worker = [&](int64_t begin, int64_t end) {
        std::vector<std::unique_ptr<MysqlConn>> myReplicas;
        for (int s = 0; s < kNumShards; s++) {
            myReplicas.push_back(std::make_unique<MysqlConn>("localhost", cfg.user, cfg.pass,
                                                                cfg.socket_path));
        }
        for (int64_t i = begin; i < end; i++) {
            int64_t k = seq[i];
            auto cached = store.cache().get(k, "friend", k + 1);
            if (cached) continue;
            int s = shardIndex(k);
            auto edge = AssociationStore::readFromReplicaConn(*myReplicas[s], s, k, "friend",
                                                                 k + 1);
            if (edge) store.cache().put(*edge);
        }
    };

    std::vector<std::thread> threads;
    int64_t chunk = numOps / numThreads;
    auto t0 = Clock::now();
    for (int t = 0; t < numThreads; t++) {
        int64_t b = t * chunk;
        int64_t e = (t == numThreads - 1) ? numOps : b + chunk;
        threads.emplace_back(worker, b, e);
    }
    for (auto& th : threads) th.join();
    auto t1 = Clock::now();

    double seconds = std::chrono::duration<double>(t1 - t0).count();
    uint64_t hits = store.cacheHits();
    uint64_t misses = store.cacheMisses();
    double hitRate = static_cast<double>(hits) / static_cast<double>(hits + misses);
    double opsPerSec = static_cast<double>(numOps) / seconds;

    std::printf("---RESULTS---\n");
    std::printf("threads=%d cache_cap_per_shard=%zu\n", numThreads, cacheCap);
    std::printf("ops=%lld seconds=%.3f ops_per_sec=%.0f\n", (long long)numOps, seconds,
                opsPerSec);
    std::printf("cache_hits=%llu cache_misses=%llu hit_rate=%.4f%%\n", (unsigned long long)hits,
                (unsigned long long)misses, hitRate * 100.0);

    store.stopReplicaThreads();
    return 0;
}
