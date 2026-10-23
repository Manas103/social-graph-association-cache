// Forks and SIGKILLs crash_worker 500 times at randomized points inside the
// forward+inverse write transaction, then checks, from a fresh connection with no
// cache involved, whether either side of the association was left dangling (one side
// committed and visible, the other not). Real process kills, real MySQL, no mocking.
//
// Usage: run_crash_harness <num_trials> <path_to_crash_worker>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <random>
#include <string>

#include "../src/common.hpp"
#include "../src/mysql_conn.hpp"

using namespace assoc;

bool rowExists(MysqlConn& conn, int shard, int64_t id1, const std::string& atype, int64_t id2) {
    auto rows = conn.query("SELECT 1 FROM " + leaderSchema(shard) +
                            ".assoc WHERE id1=" + std::to_string(id1) + " AND atype='" + atype +
                            "' AND id2=" + std::to_string(id2));
    return !rows.empty();
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s <num_trials> <path_to_crash_worker>\n", argv[0]);
        return 2;
    }
    int numTrials = std::atoi(argv[1]);
    std::string workerPath = argv[2];

    std::mt19937 rng(12345);
    std::uniform_int_distribution<int> crashPointDist(1, 3);

    int danglingForwardOnly = 0, danglingInverseOnly = 0, bothPresent = 0, neitherPresent = 0;
    int killedAsExpected = 0;

    MysqlConn checker("localhost", "assoc_app", "assoc_app_pw", "");
    const std::string atype = "likes";
    std::string inv = inverseOf(atype);

    for (int trial = 0; trial < numTrials; trial++) {
        int crashPoint = crashPointDist(rng);
        int64_t id1 = 9'000'000'000LL + trial * 10;
        int64_t id2 = id1 + 1;

        pid_t pid = fork();
        if (pid == 0) {
            execl(workerPath.c_str(), workerPath.c_str(), std::to_string(crashPoint).c_str(),
                  std::to_string(id1).c_str(), std::to_string(id2).c_str(), atype.c_str(),
                  nullptr);
            _exit(127); // execl failed
        }
        int status = 0;
        waitpid(pid, &status, 0);
        if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) killedAsExpected++;

        int s1 = shardIndex(id1), s2 = shardIndex(id2);
        bool fwd = rowExists(checker, s1, id1, atype, id2);
        bool rev = rowExists(checker, s2, id2, inv, id1);
        if (fwd && rev) bothPresent++;
        else if (fwd && !rev) danglingForwardOnly++;
        else if (!fwd && rev) danglingInverseOnly++;
        else neitherPresent++;
    }

    int dangling = danglingForwardOnly + danglingInverseOnly;
    std::printf("---RESULTS---\n");
    std::printf("trials=%d killed_as_expected=%d\n", numTrials, killedAsExpected);
    std::printf("both_committed=%d neither_committed=%d dangling_forward_only=%d "
                "dangling_inverse_only=%d total_dangling=%d\n",
                bothPresent, neitherPresent, danglingForwardOnly, danglingInverseOnly, dangling);
    return dangling == 0 ? 0 : 1;
}
