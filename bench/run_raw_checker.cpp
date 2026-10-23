// Read-after-write consistency checker.
//
// Usage: run_raw_checker <total_ops> <batch_size> <num_threads>
//
// Each "checked operation" is: write one association edge (forward + its inverse, one
// MySQL transaction, write-through into the cache), then immediately read both sides
// back through the follower-tier path (readAssociation: cache first, replica schema on
// miss) and confirm the edge is there with the data just written. Writes within a run
// are batched (writeAssociationBatch) purely so 2,000,000 of them fit in a reasonable
// wall clock; the thing being checked, cache-mediated visibility of a just-committed
// write, is unaffected by batch size.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <thread>
#include <vector>

#include "../src/store.hpp"

using namespace assoc;

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <total_ops> <batch_size> <num_threads>\n", argv[0]);
        return 2;
    }
    int64_t totalOps = std::atoll(argv[1]);
    int64_t batchSize = std::atoll(argv[2]);
    int numThreads = std::atoi(argv[3]);

    DbConfig cfg;
    AssociationStore store(cfg, /*replicaLagMs=*/5, /*cacheCapacityPerShard=*/5000);

    // Base offset by wall-clock time so repeated runs (dev iteration, or a later rerun
    // of this same checker) never collide with a previous run's rows; well clear of
    // the throughput bench's id range either way.
    int64_t base = 1'000'000'000LL + static_cast<int64_t>(std::time(nullptr)) * 1'000'000LL;
    std::atomic<int64_t> nextId{base};
    std::atomic<int64_t> violations{0};
    std::atomic<int64_t> opsDone{0};
    int64_t opsPerThread = totalOps / numThreads;

    auto worker = [&](int64_t myOps) {
        int64_t done = 0;
        while (done < myOps) {
            int64_t n = std::min<int64_t>(batchSize, myOps - done);
            std::vector<Edge> edges;
            edges.reserve(n);
            std::vector<int64_t> ids;
            ids.reserve(n * 2);
            for (int64_t i = 0; i < n; i++) {
                int64_t id1 = nextId.fetch_add(2);
                int64_t id2 = id1 + 1;
                edges.push_back({id1, "likes", id2, 1700000000000LL + id1, std::to_string(id1)});
                ids.push_back(id1);
                ids.push_back(id2);
            }
            bool ok = false;
            try {
                ok = store.writeAssociationBatch(edges);
            } catch (const std::exception& e) {
                std::fprintf(stderr, "batch write threw: %s\n", e.what());
            }
            if (!ok) {
                std::fprintf(stderr, "batch write failed, skipping verification for it\n");
                done += n;
                continue;
            }
            for (auto& e : edges) {
                auto fwd = store.readAssociation(e.id1, "likes", e.id2);
                auto rev = store.readAssociation(e.id2, "liked_by", e.id1);
                bool bad = !fwd || !rev || fwd->data != e.data || rev->data != e.data;
                if (bad) violations.fetch_add(1);
            }
            done += n;
            opsDone.fetch_add(n);
        }
    };

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; t++) {
        int64_t n = (t == numThreads - 1) ? (totalOps - opsPerThread * (numThreads - 1))
                                           : opsPerThread;
        threads.emplace_back(worker, n);
    }
    for (auto& th : threads) th.join();
    auto t1 = std::chrono::steady_clock::now();
    double seconds = std::chrono::duration<double>(t1 - t0).count();

    std::printf("---RESULTS---\n");
    std::printf("checked_operations=%lld violations=%lld seconds=%.2f ops_per_sec=%.0f\n",
                (long long)opsDone.load(), (long long)violations.load(), seconds,
                opsDone.load() / seconds);

    store.stopReplicaThreads();
    return violations.load() == 0 ? 0 : 1;
}
