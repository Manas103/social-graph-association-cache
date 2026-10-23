#pragma once

#include <array>
#include <atomic>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "common.hpp"

namespace assoc {

// Sharded, mutex-protected LRU, keyed by (id, atype) -> the single edge (id,atype,id2).
// "Write-through" refers to the write path: AssociationStore::writeAssociation populates
// both sides of this cache synchronously, in the same call that commits the MySQL
// transaction, before returning to the caller. That is what makes a follower-tier read
// immediately after a write see the new edge even though the MySQL replica it would
// otherwise fall back to may not have applied it yet (see README, "what broke").
class WriteThroughCache {
public:
    explicit WriteThroughCache(size_t capacity_per_shard) : cap_(capacity_per_shard) {}

    static std::string key(int64_t id1, const std::string& atype, int64_t id2) {
        return std::to_string(id1) + "|" + atype + "|" + std::to_string(id2);
    }

    void put(const Edge& e) {
        auto& s = shardFor(e.id1, e.atype);
        std::lock_guard<std::mutex> lock(s.mu);
        std::string k = key(e.id1, e.atype, e.id2);
        auto it = s.index.find(k);
        if (it != s.index.end()) {
            s.lru.erase(it->second);
        }
        s.lru.push_front(e);
        s.index[k] = s.lru.begin();
        evictIfNeeded(s);
    }

    // Returns all cached edges for (id, atype). A real TAO-like cache stores whole
    // association lists per (id,atype); this toy version caches edges individually
    // and the test harness only ever looks up a single known edge, so a full list
    // scan over one shard's LRU is fine here and intentionally not optimized.
    std::optional<Edge> get(int64_t id1, const std::string& atype, int64_t id2) {
        auto& s = shardFor(id1, atype);
        std::lock_guard<std::mutex> lock(s.mu);
        std::string k = key(id1, atype, id2);
        auto it = s.index.find(k);
        if (it == s.index.end()) {
            misses_++;
            return std::nullopt;
        }
        hits_++;
        s.lru.splice(s.lru.begin(), s.lru, it->second);
        s.index[k] = s.lru.begin();
        return *it->second;
    }

    uint64_t hits() const { return hits_.load(); }
    uint64_t misses() const { return misses_.load(); }

private:
    static constexpr int kCacheShards = 16;

    struct Shard {
        std::mutex mu;
        std::list<Edge> lru;
        std::unordered_map<std::string, std::list<Edge>::iterator> index;
    };

    Shard& shardFor(int64_t id1, const std::string& atype) {
        size_t h = std::hash<std::string>{}(key(id1, atype, 0)) ^ static_cast<size_t>(id1);
        return shards_[h % kCacheShards];
    }

    void evictIfNeeded(Shard& s) {
        while (s.lru.size() > cap_) {
            auto& back = s.lru.back();
            s.index.erase(key(back.id1, back.atype, back.id2));
            s.lru.pop_back();
        }
    }

    size_t cap_;
    std::array<Shard, kCacheShards> shards_;
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
};

} // namespace assoc
