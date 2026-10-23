// A single association write that kills itself at a chosen point, standing in for a
// process crash mid-write. Spawned and SIGKILL'd (by self, not by a parent, so timing
// is exact rather than racing a parent's kill signal) once per run_crash_harness trial.
//
// Usage: crash_worker <crash_point> <id1> <id2> <atype>
//   crash_point: 1 = after the forward INSERT, before the inverse INSERT, before COMMIT
//                2 = after the inverse INSERT, before COMMIT
//                3 = after COMMIT returns
//
// Deliberately bypasses AssociationStore: this program only ever issues the minimal
// forward+inverse assoc INSERT pair inside one transaction, so the crash point lands
// exactly where the usage comment says and nothing else (cache, oplog) is in the way.
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "../src/common.hpp"
#include "../src/mysql_conn.hpp"

using namespace assoc;

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr, "usage: %s <crash_point> <id1> <id2> <atype>\n", argv[0]);
        return 2;
    }
    int crashPoint = std::atoi(argv[1]);
    int64_t id1 = std::atoll(argv[2]);
    int64_t id2 = std::atoll(argv[3]);
    std::string atype = argv[4];
    std::string inv = inverseOf(atype);

    MysqlConn conn("localhost", "assoc_app", "assoc_app_pw", "");
    int s1 = shardIndex(id1), s2 = shardIndex(id2);

    conn.begin();
    conn.exec("INSERT INTO " + leaderSchema(s1) +
              ".assoc (id1,atype,id2,time_ms,data) VALUES (" + std::to_string(id1) + ",'" +
              atype + "'," + std::to_string(id2) + ",1700000000000,'crash-test')");
    if (crashPoint == 1) std::raise(SIGKILL);

    conn.exec("INSERT INTO " + leaderSchema(s2) +
              ".assoc (id1,atype,id2,time_ms,data) VALUES (" + std::to_string(id2) + ",'" + inv +
              "'," + std::to_string(id1) + ",1700000000000,'crash-test')");
    if (crashPoint == 2) std::raise(SIGKILL);

    conn.commit();
    if (crashPoint == 3) std::raise(SIGKILL);

    return 0; // crash_point 0 (control, not used by the 500-trial harness): clean write
}
