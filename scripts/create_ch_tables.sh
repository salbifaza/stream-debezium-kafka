#!/usr/bin/env bash
# Generates the ClickHouse silver tables (debezium_cdc.*) from the live
# Postgres catalog and creates them. The ClickHouse sink never runs CREATE
# TABLE, so this has to run before register_connectors.sh.
#
# What gets generated, per table in the Debezium publication
# (publication.name in connectors/pg-source-connector.json):
#
#   columns     one per source column, type-mapped by ch_type() below.
#               NOT NULL -> plain type, nullable -> Nullable(T). A text
#               column with an `IN (...)` CHECK constraint becomes
#               LowCardinality(String): the constraint says it holds a
#               handful of distinct values.
#   _version    UInt64 -- injected by the sink (debeziumCDCEnabled=true):
#               the WAL LSN the change committed at (source.lsn), used as
#               ReplacingMergeTree's version column. LSN is strictly
#               monotonic per source, so two updates to the same row can
#               never tie the way two same-millisecond timestamps could.
#               Same role as PeerDB's `_peerdb_version`.
#   is_deleted  UInt8 -- injected by the sink: 1 for deletes, 0 otherwise.
#               Every read needs `FINAL WHERE is_deleted = 0`.
#   ORDER BY    the Postgres primary key, in key order. It is also
#               ReplacingMergeTree's dedup key, which is why nothing else
#               is ever prepended to it.
#   indexes     every other Postgres index (FK indexes, UNIQUE
#               constraints) becomes a data-skipping index of the same
#               name: bloom_filter for equality lookups on integer, string,
#               UUID and date columns, minmax for everything else. A
#               skipping index only lets ClickHouse skip granules; it
#               enforces nothing, so UNIQUE is not enforced. Expression and
#               partial indexes are skipped with a warning.
#
# Idempotent: existing tables are left as they are (the sink's auto.evolve
# adds new source columns), and indexes are only added -- and materialized
# over existing rows -- if missing, so an index added in Postgres later
# reaches ClickHouse on the next run.
#
# Usage: scripts/create_ch_tables.sh [--print]
#   --print   print the generated SQL instead of applying it
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; . ./.env; set +a
fi

PRINT_ONLY=0
[ "${1:-}" = "--print" ] && PRINT_ONLY=1

PUBLICATION=$(jq -r '.config["publication.name"]' connectors/pg-source-connector.json)
CH_DB=$(jq -r '.config.database' connectors/ch-sink-connector.json)

COMPOSE="docker compose"
# -h 127.0.0.1 connects over TCP. The image's init-time server listens on
# the unix socket only, so a TCP connection succeeding also means
# postgres/init/ has finished and the schema exists.
PG=($COMPOSE exec -T -e PGPASSWORD="${SOURCE_PG_PASSWORD:-ecommerce}" source-postgres
    psql -h 127.0.0.1 -U "${SOURCE_PG_USER:-ecommerce}" -d "${SOURCE_PG_DB:-ecommerce}"
    -tA -F '|' -v ON_ERROR_STOP=1 -v pub="$PUBLICATION")
CH=($COMPOSE exec -T clickhouse clickhouse-client
    --user "${CLICKHOUSE_USER:-ch_admin}" --password "${CLICKHOUSE_PASSWORD:-ch_admin_password}")

echo "Waiting for source Postgres and ClickHouse ..." >&2
deadline=$((SECONDS + 90))
until "${PG[@]}" -c 'SELECT 1' >/dev/null 2>&1 \
        && [ "$("${CH[@]}" -q "EXISTS DATABASE ${CH_DB}" 2>/dev/null)" = "1" ]; do
    if [ $SECONDS -ge $deadline ]; then
        echo "Postgres or ClickHouse did not become ready within 90s." >&2
        exit 1
    fi
    sleep 2
done

# Postgres type (as format_type() prints it) -> ClickHouse type, matching
# what Debezium's default converters emit and the sink accepts. $2 is 't'
# when the column has an IN-list CHECK constraint.
ch_type() {
    case "$1" in
        smallint)                      echo Int16 ;;
        integer)                       echo Int32 ;;
        bigint)                        echo Int64 ;;
        real)                          echo Float32 ;;
        "double precision")            echo Float64 ;;
        numeric\(*)                    local ps=${1#numeric(}; echo "Decimal(${ps%)})" ;;
        boolean)                       echo Bool ;;
        text|character*)               if [ "$2" = t ]; then echo "LowCardinality(String)"; else echo String; fi ;;
        uuid)                          echo UUID ;;
        date)                          echo Date32 ;;
        "timestamp with time zone"|"timestamp("*") with time zone") echo "DateTime64(6)" ;;
        json|jsonb)                    echo String ;;
        *)                             return 1 ;;
    esac
}

# bloom_filter supports these base types; anything else gets minmax.
skip_index_type() {
    local t=$1
    t=${t#Nullable(}; t=${t#LowCardinality(}; t=${t%%)*}
    case "$t" in
        Int*|String|UUID|Date|Date32) echo bloom_filter ;;
        *)                            echo minmax ;;
    esac
}

declare -A cols order_by ctype
tables=()
while IFS='|' read -r tbl col pgtype notnull in_list pk; do
    if [ -z "${cols[$tbl]+x}" ]; then
        tables+=("$tbl")
        if [ -z "$pk" ]; then
            echo "Table ${tbl} has no primary key; ReplacingMergeTree needs one for ORDER BY." >&2
            exit 1
        fi
        order_by[$tbl]=$pk
    fi
    if ! t=$(ch_type "$pgtype" "$in_list"); then
        echo "No ClickHouse type mapping for ${tbl}.${col} (${pgtype}); add one to ch_type() in $0." >&2
        exit 1
    fi
    [ "$notnull" = t ] || t="Nullable($t)"
    ctype[$tbl.$col]=$t
    cols[$tbl]+="    \`${col}\` ${t},"$'\n'
done < <("${PG[@]}" <<'SQL'
SELECT c.relname,
       a.attname,
       format_type(a.atttypid, a.atttypmod),
       a.attnotnull,
       EXISTS (SELECT 1 FROM pg_constraint k
                WHERE k.conrelid = c.oid AND k.contype = 'c'
                  AND k.conkey = ARRAY[a.attnum]
                  AND pg_get_constraintdef(k.oid) LIKE '%= ANY (ARRAY[%'),
       (SELECT string_agg(format('`%s`', pa.attname), ', ' ORDER BY k.ord)
          FROM pg_index p
         CROSS JOIN unnest(p.indkey::int2[]) WITH ORDINALITY k(attnum, ord)
          JOIN pg_attribute pa ON pa.attrelid = p.indrelid AND pa.attnum = k.attnum
         WHERE p.indrelid = c.oid AND p.indisprimary)
  FROM pg_publication_tables pt
  JOIN pg_namespace n ON n.nspname = pt.schemaname
  JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
 WHERE pt.pubname = :'pub'
 ORDER BY c.relname, a.attnum;
SQL
)

if [ ${#tables[@]} -eq 0 ]; then
    echo "Publication '${PUBLICATION}' has no tables -- nothing to create." >&2
    exit 1
fi

create_sql=""
for tbl in "${tables[@]}"; do
    create_sql+="CREATE TABLE IF NOT EXISTS ${CH_DB}.\`${tbl}\` (
${cols[$tbl]}    \`_version\` UInt64,
    \`is_deleted\` UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (${order_by[$tbl]});

"
done

idx_keys=(); idx_sql=()
while IFS='|' read -r tbl idx idx_cols unsupported; do
    if [ "$unsupported" = t ]; then
        echo "  skipping ${tbl}.${idx}: expression or partial index, no ClickHouse equivalent generated" >&2
        continue
    fi
    IFS=',' read -ra icols <<<"$idx_cols"
    # The sort key already serves lookups on its leading column.
    if [ "${order_by[$tbl]%%,*}" = "\`${icols[0]}\`" ]; then
        echo "  skipping ${tbl}.${idx}: leads with the ORDER BY key" >&2
        continue
    fi
    type=bloom_filter; quoted=()
    for c in "${icols[@]}"; do
        [ "$(skip_index_type "${ctype[$tbl.$c]}")" = minmax ] && type=minmax
        quoted+=("\`$c\`")
    done
    expr=$(IFS=','; echo "${quoted[*]}" | sed 's/,/, /g')
    [ ${#icols[@]} -gt 1 ] && expr="(${expr})"
    idx_keys+=("${tbl}.${idx}")
    idx_sql+=("ALTER TABLE ${CH_DB}.\`${tbl}\` ADD INDEX IF NOT EXISTS \`${idx}\` ${expr} TYPE ${type} GRANULARITY 1;")
done < <("${PG[@]}" <<'SQL'
SELECT t.relname,
       i.relname,
       (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
          FROM unnest(ix.indkey::int2[]) WITH ORDINALITY k(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum
         WHERE k.ord <= ix.indnkeyatts),
       ix.indexprs IS NOT NULL OR ix.indpred IS NOT NULL
  FROM pg_publication_tables pt
  JOIN pg_namespace n ON n.nspname = pt.schemaname
  JOIN pg_class t ON t.relnamespace = n.oid AND t.relname = pt.tablename
  JOIN pg_index ix ON ix.indrelid = t.oid AND NOT ix.indisprimary
  JOIN pg_class i ON i.oid = ix.indexrelid
 WHERE pt.pubname = :'pub'
 ORDER BY t.relname, i.relname;
SQL
)

if [ "$PRINT_ONLY" -eq 1 ]; then
    printf '%s' "$create_sql"
    printf '%s\n' "${idx_sql[@]}"
    exit 0
fi

existing_tables=$("${CH[@]}" -q "SELECT name FROM system.tables WHERE database = '${CH_DB}'")
echo "== Creating ClickHouse tables in ${CH_DB} (from publication ${PUBLICATION}) =="
"${CH[@]}" --multiquery <<<"$create_sql"
for tbl in "${tables[@]}"; do
    if grep -qx "$tbl" <<<"$existing_tables"; then
        printf "  %-14s exists, left as is\n" "$tbl"
    else
        printf "  %-14s created, ORDER BY %s\n" "$tbl" "${order_by[$tbl]//\`/}"
    fi
done

existing_idx=$("${CH[@]}" -q "SELECT concat(table, '.', name) FROM system.data_skipping_indices WHERE database = '${CH_DB}'")
echo "== Data-skipping indexes =="
for i in "${!idx_keys[@]}"; do
    key=${idx_keys[$i]}
    if grep -qx "$key" <<<"$existing_idx"; then
        printf "  %-36s exists\n" "$key"
        continue
    fi
    "${CH[@]}" -q "${idx_sql[$i]}"
    # ADD INDEX only covers parts written from now on; build it for existing rows too.
    "${CH[@]}" -q "ALTER TABLE ${CH_DB}.\`${key%%.*}\` MATERIALIZE INDEX \`${key#*.}\`"
    printf "  %-36s added: %s\n" "$key" "$(grep -o 'ADD INDEX.*' <<<"${idx_sql[$i]}" | sed 's/ADD INDEX IF NOT EXISTS //; s/`//g; s/;$//')"
done
