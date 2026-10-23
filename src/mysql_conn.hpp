#pragma once

#include <mysql/mysql.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace assoc {

// Minimal RAII wrapper around the libmysqlclient C API. Exists so the rest of the
// codebase never calls mysql_* directly, and so a crash-injection point is just
// "stop calling methods on this object", not signal plumbing through C callbacks.
class MysqlConn {
public:
    MysqlConn(const std::string& host, const std::string& user, const std::string& pass,
               const std::string& socket_path, unsigned port = 0) {
        conn_ = mysql_init(nullptr);
        if (!conn_) throw std::runtime_error("mysql_init failed");
        const char* sock = socket_path.empty() ? nullptr : socket_path.c_str();
        if (!mysql_real_connect(conn_, host.c_str(), user.c_str(), pass.c_str(), nullptr, port,
                                 sock, 0)) {
            std::string err = mysql_error(conn_);
            mysql_close(conn_);
            conn_ = nullptr;
            throw std::runtime_error("mysql_real_connect failed: " + err);
        }
        mysql_set_character_set(conn_, "utf8mb4");
    }

    ~MysqlConn() {
        if (conn_) mysql_close(conn_);
    }

    MysqlConn(const MysqlConn&) = delete;
    MysqlConn& operator=(const MysqlConn&) = delete;

    void exec(const std::string& sql) {
        if (mysql_query(conn_, sql.c_str()) != 0) {
            throw std::runtime_error("mysql_query failed [" + sql + "]: " + mysql_error(conn_));
        }
        // Discard any result set (DDL/DML has none, but be defensive).
        MYSQL_RES* res = mysql_store_result(conn_);
        if (res) mysql_free_result(res);
    }

    // Returns rows of raw strings (NULL becomes empty string; schema never needs NULL here).
    std::vector<std::vector<std::string>> query(const std::string& sql) {
        if (mysql_query(conn_, sql.c_str()) != 0) {
            throw std::runtime_error("mysql_query failed [" + sql + "]: " + mysql_error(conn_));
        }
        MYSQL_RES* res = mysql_store_result(conn_);
        std::vector<std::vector<std::string>> rows;
        if (!res) return rows;
        unsigned nfields = mysql_num_fields(res);
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
            unsigned long* lens = mysql_fetch_lengths(res);
            std::vector<std::string> r;
            r.reserve(nfields);
            for (unsigned i = 0; i < nfields; i++) {
                r.emplace_back(row[i] ? std::string(row[i], lens[i]) : std::string());
            }
            rows.push_back(std::move(r));
        }
        mysql_free_result(res);
        return rows;
    }

    std::string escape(const std::string& s) {
        std::string out(s.size() * 2 + 1, '\0');
        unsigned long n = mysql_real_escape_string(conn_, &out[0], s.c_str(), s.size());
        out.resize(n);
        return out;
    }

    void begin() { exec("START TRANSACTION"); }
    void commit() { exec("COMMIT"); }
    void rollback() { exec("ROLLBACK"); }

    MYSQL* raw() { return conn_; }

private:
    MYSQL* conn_ = nullptr;
};

} // namespace assoc
