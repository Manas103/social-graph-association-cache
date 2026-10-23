#include "store.hpp"

#include <chrono>
#include <thread>

namespace assoc {

namespace {

// Parses the one row the schema's PRIMARY KEY guarantees is unique, or none.
std::optional<Edge> rowToEdge(const std::vector<std::vector<std::string>>& rows) {
    if (rows.empty()) return std::nullopt;
    const auto& r = rows[0];
    Edge e;
    e.id1 = std::stoll(r[0]);
    e.atype = r[1];
    e.id2 = std::stoll(r[2]);
    e.time_ms = std::stoll(r[3]);
    e.data = r[4];
    return e;
}

} // namespace

AssociationStore::AssociationStore(const DbConfig& cfg, int replicaLagMs,
                                     size_t cacheCapacityPerShard)
    : cfg_(cfg), replicaLagMs_(replicaLagMs), cache_(cacheCapacityPerShard) {
    leader_ = std::make_unique<MysqlConn>("localhost", cfg_.user, cfg_.pass, cfg_.socket_path);
    for (int s = 0; s < kNumShards; s++) {
        replicas_.push_back(
            std::make_unique<MysqlConn>("localhost", cfg_.user, cfg_.pass, cfg_.socket_path));
        replicaThreads_.emplace_back([this, s] { replicaApplyLoop(s); });
    }
}

AssociationStore::~AssociationStore() { stopReplicaThreads(); }

void AssociationStore::stopReplicaThreads() {
    if (stop_.exchange(true)) return;
    for (auto& t : replicaThreads_) {
        if (t.joinable()) t.join();
    }
}

bool AssociationStore::writeAssociation(int64_t id1, const std::string& atype, int64_t id2,
                                          const std::string& data, int64_t time_ms) {
    std::string inv = inverseOf(atype);
    int s1 = shardIndex(id1);
    int s2 = shardIndex(id2);
    std::string l1 = leaderSchema(s1), l2 = leaderSchema(s2);

    std::lock_guard<std::mutex> lock(leaderMu_);
    std::string ed = leader_->escape(data);
    std::string eat = leader_->escape(atype);
    std::string einv = leader_->escape(inv);

    leader_->begin();
    try {
        leader_->exec("INSERT INTO " + l1 + ".assoc (id1,atype,id2,time_ms,data) VALUES (" +
                       std::to_string(id1) + ",'" + eat + "'," + std::to_string(id2) + "," +
                       std::to_string(time_ms) + ",'" + ed + "')");
        leader_->exec("INSERT INTO " + l2 + ".assoc (id1,atype,id2,time_ms,data) VALUES (" +
                       std::to_string(id2) + ",'" + einv + "'," + std::to_string(id1) + "," +
                       std::to_string(time_ms) + ",'" + ed + "')");
        leader_->exec("INSERT INTO " + l1 + ".oplog (id1,atype,id2,time_ms,data) VALUES (" +
                       std::to_string(id1) + ",'" + eat + "'," + std::to_string(id2) + "," +
                       std::to_string(time_ms) + ",'" + ed + "')");
        leader_->exec("INSERT INTO " + l2 + ".oplog (id1,atype,id2,time_ms,data) VALUES (" +
                       std::to_string(id2) + ",'" + einv + "'," + std::to_string(id1) + "," +
                       std::to_string(time_ms) + ",'" + ed + "')");
        leader_->commit();
    } catch (...) {
        leader_->rollback();
        return false;
    }

    Edge fwd{id1, atype, id2, time_ms, data};
    Edge rev{id2, inv, id1, time_ms, data};
    cache_.put(fwd);
    cache_.put(rev);
    return true;
}

bool AssociationStore::writeAssociationBatch(const std::vector<Edge>& edges) {
    std::unordered_map<std::string, std::string> assocValues;
    std::unordered_map<std::string, std::string> oplogValues;
    std::vector<Edge> toCache;
    toCache.reserve(edges.size() * 2);

    std::lock_guard<std::mutex> lock(leaderMu_);
    auto appendRow = [&](std::unordered_map<std::string, std::string>& values,
                           const std::string& schema, int64_t id1, const std::string& atype,
                           int64_t id2, int64_t time_ms, const std::string& data) {
        std::string row = "(" + std::to_string(id1) + ",'" + leader_->escape(atype) + "'," +
                           std::to_string(id2) + "," + std::to_string(time_ms) + ",'" +
                           leader_->escape(data) + "')";
        auto& v = values[schema];
        v += v.empty() ? row : ("," + row);
    };

    for (const auto& e : edges) {
        std::string inv = inverseOf(e.atype);
        int s1 = shardIndex(e.id1), s2 = shardIndex(e.id2);
        appendRow(assocValues, leaderSchema(s1), e.id1, e.atype, e.id2, e.time_ms, e.data);
        appendRow(assocValues, leaderSchema(s2), e.id2, inv, e.id1, e.time_ms, e.data);
        appendRow(oplogValues, leaderSchema(s1), e.id1, e.atype, e.id2, e.time_ms, e.data);
        appendRow(oplogValues, leaderSchema(s2), e.id2, inv, e.id1, e.time_ms, e.data);
        toCache.push_back({e.id1, e.atype, e.id2, e.time_ms, e.data});
        toCache.push_back({e.id2, inv, e.id1, e.time_ms, e.data});
    }

    leader_->begin();
    try {
        for (auto& [schema, values] : assocValues) {
            leader_->exec("INSERT INTO " + schema +
                           ".assoc (id1,atype,id2,time_ms,data) VALUES " + values);
        }
        for (auto& [schema, values] : oplogValues) {
            leader_->exec("INSERT INTO " + schema +
                           ".oplog (id1,atype,id2,time_ms,data) VALUES " + values);
        }
        leader_->commit();
    } catch (const std::exception&) {
        leader_->rollback();
        throw;
    }

    for (auto& e : toCache) cache_.put(e);
    return true;
}

std::optional<Edge> AssociationStore::readFromSchema(const std::string& schema, int64_t id1,
                                                        const std::string& atype, int64_t id2) {
    std::lock_guard<std::mutex> lock(replicaMu_);
    int s = shardIndex(id1);
    auto& conn = *replicas_[s];
    std::string eat = conn.escape(atype);
    auto rows = conn.query("SELECT id1,atype,id2,time_ms,data FROM " + schema +
                            ".assoc WHERE id1=" + std::to_string(id1) + " AND atype='" + eat +
                            "' AND id2=" + std::to_string(id2));
    return rowToEdge(rows);
}

std::optional<Edge> AssociationStore::readAssociation(int64_t id1, const std::string& atype,
                                                         int64_t id2) {
    auto cached = cache_.get(id1, atype, id2);
    if (cached) return cached;
    int s = shardIndex(id1);
    auto edge = readFromSchema(replicaSchema(s), id1, atype, id2);
    if (edge) cache_.put(*edge);
    return edge;
}

std::optional<Edge> AssociationStore::readAssociationFromLeader(int64_t id1,
                                                                    const std::string& atype,
                                                                    int64_t id2) {
    std::lock_guard<std::mutex> lock(leaderMu_);
    int s = shardIndex(id1);
    std::string eat = leader_->escape(atype);
    auto rows = leader_->query("SELECT id1,atype,id2,time_ms,data FROM " + leaderSchema(s) +
                                ".assoc WHERE id1=" + std::to_string(id1) + " AND atype='" + eat +
                                "' AND id2=" + std::to_string(id2));
    return rowToEdge(rows);
}

std::optional<Edge> AssociationStore::readFromReplicaConn(MysqlConn& conn, int shard,
                                                             int64_t id1, const std::string& atype,
                                                             int64_t id2) {
    std::string eat = conn.escape(atype);
    auto rows = conn.query("SELECT id1,atype,id2,time_ms,data FROM " + replicaSchema(shard) +
                            ".assoc WHERE id1=" + std::to_string(id1) + " AND atype='" + eat +
                            "' AND id2=" + std::to_string(id2));
    return rowToEdge(rows);
}

void AssociationStore::replicaApplyLoop(int shard) {
    MysqlConn conn("localhost", cfg_.user, cfg_.pass, cfg_.socket_path);
    std::string leader = leaderSchema(shard);
    std::string replica = replicaSchema(shard);
    while (!stop_.load()) {
        auto rows = conn.query("SELECT lsn,id1,atype,id2,time_ms,data FROM " + leader +
                                ".oplog WHERE applied=0 ORDER BY lsn LIMIT 200");
        if (rows.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            continue;
        }
        // Artificial delay models real async-replication lag: the row exists on the
        // leader and in the oplog the instant the write transaction above commits,
        // but a true MySQL replica would not have applied it yet either.
        if (replicaLagMs_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(replicaLagMs_));
        }
        for (auto& r : rows) {
            int64_t lsn = std::stoll(r[0]);
            int64_t id1 = std::stoll(r[1]);
            std::string atype = conn.escape(r[2]);
            int64_t id2 = std::stoll(r[3]);
            int64_t time_ms = std::stoll(r[4]);
            std::string data = conn.escape(r[5]);
            conn.exec("INSERT INTO " + replica +
                      ".assoc (id1,atype,id2,time_ms,data) VALUES (" + std::to_string(id1) +
                      ",'" + atype + "'," + std::to_string(id2) + "," + std::to_string(time_ms) +
                      ",'" + data + "') ON DUPLICATE KEY UPDATE time_ms=time_ms");
            conn.exec("UPDATE " + leader + ".oplog SET applied=1 WHERE lsn=" +
                      std::to_string(lsn));
        }
    }
}

} // namespace assoc
