#!/usr/bin/env bash
# Truncates every shard's assoc/oplog/objects tables (leader and replica schemas), so
# a benchmark or test run starts from a clean, known-empty store. Safe to run anytime;
# nothing here is data anyone needs to keep.
set -euo pipefail

MYSQL_USER="${MYSQL_USER:-assoc_app}"
MYSQL_PASS="${MYSQL_PASS:-assoc_app_pw}"
NUM_SHARDS="${NUM_SHARDS:-4}"

for i in $(seq 0 $((NUM_SHARDS - 1))); do
    for suffix in "" "_replica"; do
        schema="shard_${i}${suffix}"
        mysql -u "${MYSQL_USER}" -p"${MYSQL_PASS}" -e \
            "TRUNCATE TABLE ${schema}.assoc; TRUNCATE TABLE ${schema}.objects;" 2>/dev/null || true
        if [ "${suffix}" = "" ]; then
            mysql -u "${MYSQL_USER}" -p"${MYSQL_PASS}" -e "TRUNCATE TABLE ${schema}.oplog;"
        fi
    done
done
echo "reset done"
