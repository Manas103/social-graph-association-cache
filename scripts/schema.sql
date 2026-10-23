-- Schema for the social-graph-association-cache project.
-- Run via scripts/setup_db.sh, which sources this once per shard and once per replica.
-- {SCHEMA} is substituted by setup_db.sh.

CREATE DATABASE IF NOT EXISTS {SCHEMA};

CREATE TABLE IF NOT EXISTS {SCHEMA}.objects (
    id BIGINT PRIMARY KEY,
    otype VARCHAR(32) NOT NULL,
    payload TEXT,
    created_at BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS {SCHEMA}.assoc (
    id1 BIGINT NOT NULL,
    atype VARCHAR(32) NOT NULL,
    id2 BIGINT NOT NULL,
    time_ms BIGINT NOT NULL,
    data TEXT,
    PRIMARY KEY (id1, atype, id2),
    INDEX idx_id1_atype_time (id1, atype, time_ms)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS {SCHEMA}.oplog (
    lsn BIGINT AUTO_INCREMENT PRIMARY KEY,
    id1 BIGINT NOT NULL,
    atype VARCHAR(32) NOT NULL,
    id2 BIGINT NOT NULL,
    time_ms BIGINT NOT NULL,
    data TEXT,
    applied TINYINT NOT NULL DEFAULT 0,
    INDEX idx_applied (applied)
) ENGINE=InnoDB;
