#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace assoc {

// Number of logical shards. Each shard is a MySQL schema (shard_0 .. shard_{N-1})
// plus a schema (shard_0_replica .. ) holding its simulated async follower copy.
// All schemas live on one physical mysqld instance (see README, "what this is not").
constexpr int kNumShards = 4;

inline int shardIndex(int64_t id) {
    // Unsigned cast so negative ids (unused here, but defensive) still land in range.
    return static_cast<int>(static_cast<uint64_t>(id) % static_cast<uint64_t>(kNumShards));
}

inline std::string leaderSchema(int shard) {
    return "shard_" + std::to_string(shard);
}

inline std::string replicaSchema(int shard) {
    return "shard_" + std::to_string(shard) + "_replica";
}

// Every association type has a fixed inverse. A symmetric type (friend) is its own
// inverse. This table is the entire "what points back at what" model: TAO calls it
// the inverse-type map and it is what lets a write know which second row to produce.
inline const std::unordered_map<std::string, std::string>& inverseTypeMap() {
    static const std::unordered_map<std::string, std::string> m = {
        {"friend", "friend"},
        {"follows", "followed_by"},
        {"followed_by", "follows"},
        {"likes", "liked_by"},
        {"liked_by", "likes"},
        {"authored", "authored_by"},
        {"authored_by", "authored"},
    };
    return m;
}

inline std::string inverseOf(const std::string& atype) {
    auto& m = inverseTypeMap();
    auto it = m.find(atype);
    return it == m.end() ? atype : it->second;
}

struct Edge {
    int64_t id1 = 0;
    std::string atype;
    int64_t id2 = 0;
    int64_t time_ms = 0;
    std::string data;
};

} // namespace assoc
