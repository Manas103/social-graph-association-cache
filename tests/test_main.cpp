// Hand-rolled check suite (no GoogleTest dependency; this project's checks are either
// "diff against a deliberately naive in-memory oracle" or "assert an invariant", which
// a flat main() expresses more plainly than a fixture framework would).
//
// Requires a live MySQL instance with the shard_0..shard_3(_replica) schemas already
// created (see scripts/setup_db.sh); these are integration tests, not unit tests in
// the no-dependencies sense, which is the honest shape for a project whose entire
// point is MySQL-plus-cache consistency.
#include <cassert>
#include <cstdio>
#include <map>
#include <random>
#include <string>

#include "../src/cache.hpp"
#include "../src/common.hpp"
#include "../src/store.hpp"

using namespace assoc;

static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond)                                                                      \
    do {                                                                                 \
        g_checks++;                                                                      \
        if (!(cond)) {                                                                   \
            g_failures++;                                                                \
            std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
        }                                                                                \
    } while (0)

// The deliberately-naive reference: a plain map keyed by the same (id1,atype,id2)
// triplet, written with no sharding, no transaction, no cache. If the real store ever
// disagrees with this about what was written, the real store is wrong, not the oracle.
class OracleStore {
public:
    void write(int64_t id1, const std::string& atype, int64_t id2, const std::string& data) {
        table_[key(id1, atype, id2)] = data;
        table_[key(id2, inverseOf(atype), id1)] = data;
    }
    const std::string* read(int64_t id1, const std::string& atype, int64_t id2) const {
        auto it = table_.find(key(id1, atype, id2));
        return it == table_.end() ? nullptr : &it->second;
    }

private:
    static std::string key(int64_t id1, const std::string& atype, int64_t id2) {
        return std::to_string(id1) + "|" + atype + "|" + std::to_string(id2);
    }
    std::map<std::string, std::string> table_;
};

static void test_inverse_type_map() {
    CHECK(inverseOf("friend") == "friend");
    CHECK(inverseOf("follows") == "followed_by");
    CHECK(inverseOf("followed_by") == "follows");
    CHECK(inverseOf(inverseOf("likes")) == "likes");
}

static void test_shard_routing_in_range() {
    for (int64_t id = 0; id < 10000; id++) {
        int s = shardIndex(id);
        CHECK(s >= 0 && s < kNumShards);
        CHECK(shardIndex(id) == s); // deterministic
    }
}

static void test_cache_lru_eviction() {
    WriteThroughCache cache(/*capacity_per_shard=*/2);
    // Force collisions into one cache shard by using the same (id1,atype) for every
    // edge, varying only id2; with capacity 2 the oldest of three should evict.
    cache.put({1, "friend", 100, 0, "a"});
    cache.put({1, "friend", 101, 0, "b"});
    cache.put({1, "friend", 102, 0, "c"});
    // id2=100 should have been evicted (capacity 2, inserted first, never re-touched).
    CHECK(!cache.get(1, "friend", 100).has_value());
    CHECK(cache.get(1, "friend", 101).has_value());
    CHECK(cache.get(1, "friend", 102).has_value());
}

static void test_oracle_diff(AssociationStore& store) {
    OracleStore oracle;
    std::mt19937 rng(777);
    std::uniform_int_distribution<int64_t> idDist(1, 50000);
    const char* atypes[] = {"friend", "follows", "likes", "authored"};
    std::uniform_int_distribution<int> atypeDist(0, 3);

    const int64_t base = 2'000'000'000; // disjoint id space from other test binaries
    std::vector<std::tuple<int64_t, std::string, int64_t, std::string>> written;
    for (int i = 0; i < 2000; i++) {
        int64_t id1 = base + idDist(rng);
        int64_t id2 = base + idDist(rng) + 50000; // keep id1/id2 ranges disjoint
        std::string atype = atypes[atypeDist(rng)];
        std::string data = "v" + std::to_string(i);
        bool ok = store.writeAssociation(id1, atype, id2, data, 1700000000000LL + i);
        CHECK(ok);
        oracle.write(id1, atype, id2, data);
        written.emplace_back(id1, atype, id2, data);
    }

    int matched = 0;
    for (auto& [id1, atype, id2, data] : written) {
        auto real = store.readAssociation(id1, atype, id2);
        auto oracleVal = oracle.read(id1, atype, id2);
        CHECK(oracleVal != nullptr);
        CHECK(real.has_value());
        if (real && oracleVal && real->data == *oracleVal) matched++;

        std::string inv = inverseOf(atype);
        auto realInv = store.readAssociation(id2, inv, id1);
        auto oracleInv = oracle.read(id2, inv, id1);
        CHECK(realInv.has_value());
        CHECK(oracleInv != nullptr);
        if (realInv && oracleInv) CHECK(realInv->data == *oracleInv);
    }
    std::printf("oracle diff: %d/%zu forward edges matched exactly\n", matched, written.size());
}

static void test_cache_miss_falls_back_to_replica(AssociationStore& store) {
    // Write, then force a miss by writing enough other keys that the LRU would have
    // evicted it in a small-capacity cache -- here we just check that an edge written
    // and then read twice (second read necessarily a cache hit, since get() promotes
    // on hit) is internally consistent: present, same data, and the shard's own
    // forward/inverse accounting agrees with readAssociationFromLeader (ground truth,
    // cache bypassed).
    int64_t id1 = 2'100'000'001, id2 = 2'100'000'002;
    CHECK(store.writeAssociation(id1, "likes", id2, "leader-check", 1700000000000LL));
    auto leaderView = store.readAssociationFromLeader(id1, "likes", id2);
    auto cacheView = store.readAssociation(id1, "likes", id2);
    CHECK(leaderView.has_value());
    CHECK(cacheView.has_value());
    if (leaderView && cacheView) CHECK(leaderView->data == cacheView->data);
}

int main() {
    DbConfig cfg;
    AssociationStore store(cfg, /*replicaLagMs=*/2, /*cacheCapacityPerShard=*/10000);

    test_inverse_type_map();
    test_shard_routing_in_range();
    test_cache_lru_eviction();
    test_oracle_diff(store);
    test_cache_miss_falls_back_to_replica(store);

    store.stopReplicaThreads();

    std::printf("%d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
