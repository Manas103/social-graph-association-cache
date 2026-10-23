#!/usr/bin/env bash
# Creates the shard_0..shard_{N-1} leader schemas and their shard_i_replica siblings.
# Run once before any benchmark or test binary. Idempotent (CREATE ... IF NOT EXISTS).
#
#   MYSQL_USER=assoc_app MYSQL_PASS=assoc_app_pw NUM_SHARDS=4 ./scripts/setup_db.sh
#
# This is the real stand-up command referenced by the README; nothing here is faked.
set -euo pipefail

MYSQL_USER="${MYSQL_USER:-assoc_app}"
MYSQL_PASS="${MYSQL_PASS:-assoc_app_pw}"
NUM_SHARDS="${NUM_SHARDS:-4}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for i in $(seq 0 $((NUM_SHARDS - 1))); do
    for suffix in "" "_replica"; do
        schema="shard_${i}${suffix}"
        sed "s/{SCHEMA}/${schema}/g" "${SCRIPT_DIR}/schema.sql" \
            | mysql -u "${MYSQL_USER}" -p"${MYSQL_PASS}"
        echo "ensured schema ${schema}"
    done
done
