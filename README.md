# Postgres → ClickHouse CDC with Debezium + Kafka Connect

A local, reproducible change-data-capture pipeline: the same
e-commerce-shaped Postgres OLTP database as
[`stream-cdc-peerdb`](../stream-cdc-peerdb) streams inserts/updates/deletes
into ClickHouse in near real-time, this time via
[Debezium](https://debezium.io/)'s Postgres connector, Kafka, and the
official [ClickHouse Kafka Connect sink connector](https://github.com/ClickHouse/clickhouse-kafka-connect)
— assembled by hand from general-purpose parts, instead of PeerDB's single
purpose-built product.

## About this project

This is the other half of a comparison, not a standalone tutorial.
[`stream-cdc-peerdb`](../stream-cdc-peerdb) solves the identical problem
— same schema, same seed data, same ClickHouse destination — via PeerDB,
and its README argues for that choice against Debezium+Kafka Connect on
paper. This repo builds the Debezium+Kafka Connect side for real, so the
comparison is evidence, not assertion, on both sides. Same three pillars:

1. **A tool-selection decision with real tradeoffs stated** — see
   [Why Debezium + Kafka Connect](#why-debezium--kafka-connect-and-not-peerdb).
2. **Evidence over assertion.** Every specific claim below (row counts,
   propagation timing, failure behavior, exact grant sets, and — critically
   — two places where my initial design assumption turned out to be wrong)
   was reproduced against this project's own running stack — see
   [What this actually proves](#what-this-actually-proves).
3. **Named limitations, not implied completeness** — see
   [Production considerations](#production-considerations-what-id-change-for-real).

**If you have two minutes:** read this section, then
[Why Debezium + Kafka Connect](#why-debezium--kafka-connect-and-not-peerdb)
and [What this actually proves](#what-this-actually-proves).
**If you're doing technical diligence:** `Quickstart` below reproduces the
whole thing in one command; every technical claim links to the exact
script or doc section that proves it.

### Competencies this demonstrates

| Area | Where |
|---|---|
| Tool/architecture selection under real tradeoffs (not just implementation) | [Why Debezium + Kafka Connect](#why-debezium--kafka-connect-and-not-peerdb) |
| Assembling a CDC pipeline from independently-versioned parts (source connector, broker, sink connector) rather than one product | [What's running](#whats-running-and-why-its-shaped-this-way), [`kafka-connect/Dockerfile`](kafka-connect/Dockerfile) |
| Reading a connector's own source to verify behavior instead of assuming from docs | [Debezium's envelope → ClickHouse](docs/architecture.md#debeziums-envelope--clickhouse-no-hand-rolled-flattening-needed) |
| Operational maturity — monitoring, alerting signals, failure injection with evidence | [Stage 4](#stage-4-operations-monitoring-schema-evolution-failure-recovery) |
| Security by default (least-privilege access, credential separation) | [ClickHouse configuration](#clickhouse-configuration) |
| Data-integrity edge cases found by testing, both with a feature off and on | [schema evolution findings](docs/architecture.md#schema-evolution--tested-both-ways-not-assumed) |
| Honest scope framing — naming what isn't production-ready | [Production considerations](#production-considerations-what-id-change-for-real) |

See [`docs/architecture.md`](docs/architecture.md) for the full mechanics,
a data-flow diagram, and every failure-mode finding in detail.

**Contents:** [About](#about-this-project) ·
[Why Debezium + Kafka Connect](#why-debezium--kafka-connect-and-not-peerdb) ·
[Results](#what-this-actually-proves) ·
[Quickstart](#quickstart) ·
[What's running](#whats-running-and-why-its-shaped-this-way) ·
[Registering connectors](#stage-3-registering-the-connectors) ·
[Operations](#stage-4-operations-monitoring-schema-evolution-failure-recovery) ·
[Production considerations](#production-considerations-what-id-change-for-real) ·
[Repo layout](#repo-layout)

## Why Debezium + Kafka Connect (and not PeerDB)

This is the inverse of the table in `stream-cdc-peerdb`'s README — same
honest-tradeoffs framing, argued from the other side:

| Consideration | Debezium + Kafka Connect (this repo) | PeerDB (`stream-cdc-peerdb`) |
|---|---|---|
| **What it actually is** | Log-based CDC into Kafka topics, decoupled from any one sink by a durable, replayable event log | Purpose-built Postgres↔ClickHouse CDC shipping its own orchestration (Temporal) |
| **When it's the right call** | **Multiple heterogeneous consumers** need the same change stream — a warehouse, a search index, a cache invalidator, an event-driven service. Kafka's replay and fan-out are the entire point, and this setup is one new sink connector away from adding a second consumer without touching the source connector at all. | One sink, and the priority is understanding/testing internals rather than assembling infrastructure that mostly sits idle for a single-consumer workload. |
| **Operational surface, measured not guessed** | 5 containers this repo actually runs (Kafka, Kafka Connect, kafka-ui, source, destination), one of which (`kafka-connect`) is a custom-built image combining two independently-versioned connectors — see [`kafka-connect/Dockerfile`](kafka-connect/Dockerfile). | 11 containers (per `stream-cdc-peerdb`'s README), but all vendored as PeerDB's own unmodified control plane — one product, not an assembly. |
| **Schema management** | Fully manual: destination DDL is hand-written ([`clickhouse/init/02_destination_tables.sql`](clickhouse/init/02_destination_tables.sql)), and automatic `ADD COLUMN` propagation exists (`auto.evolve=true`) but is opt-in *and* requires a grant most least-privilege setups won't have by default — [tested both ways](docs/architecture.md#schema-evolution--tested-both-ways-not-assumed). | Automatic: PeerDB generates destination DDL from the source table and propagates `ADD COLUMN` by default. |
| **What's genuinely open source, no per-row metering** | Debezium, Kafka, and the ClickHouse sink connector are all Apache-2.0/open source. | PeerDB OSS, also open source and self-hostable. |
| **Honest cost of this repo's specific choices** | No schema registry/Avro (JSON with embedded schema instead) — a real size/throughput tradeoff, named in Production considerations, not hidden. Debugging surface spans three independently-operated systems (Postgres, Kafka Connect, ClickHouse) instead of one. | Smaller, younger project than Debezium/Kafka, smaller community, less multi-decade track record — `stream-cdc-peerdb`'s own named tradeoff. |

**The honest summary**: neither README's argument was wrong. For *this*
specific problem — one source, one sink — PeerDB's integrated approach
does less incidental work for the same result, which is exactly what
`stream-cdc-peerdb` claimed. Debezium+Kafka Connect earns its complexity
back the moment a second consumer shows up, which this repo doesn't
have — and the schema-evolution and worker-recovery findings below show
concretely *where* that extra assembly cost actually shows up in practice,
not just in principle.

## What this actually proves

Everything below was run against this project's own stack, including two
places where my own design assumption was wrong going in and testing
caught it:

- **Initial snapshot**: 6 tables, exact row-count match between Postgres
  and ClickHouse immediately after registering both connectors.
- **Live CDC**: an insert, update, and delete against `source-postgres`
  each land in ClickHouse within one poll cycle — see
  `scripts/verify_cdc.sh`.
- **The ClickHouse sink connector didn't need a hand-rolled flattening
  transform.** The original design assumed one would (mirroring how a
  generic JDBC sink typically works). Reading the connector's own source
  showed it has a native Debezium CDC mode (`debeziumCDCEnabled=true`)
  that consumes the raw envelope directly and injects `_version` (from the
  Postgres WAL LSN) and `is_deleted` — a cleaner design than the original
  plan, adopted after verifying it against the connector's actual code,
  not its marketing docs. See
  [docs/architecture.md](docs/architecture.md#debeziums-envelope--clickhouse-no-hand-rolled-flattening-needed).
- **Schema evolution, tested both off and on**: with `auto.evolve=false`
  (this pipeline's default), an added source column is silently dropped —
  no error, task stays healthy, easy to miss. With `auto.evolve=true`, the
  connector genuinely does run `ALTER TABLE ... ADD COLUMN` automatically,
  matching PeerDB — but the first attempt failed loudly on a missing
  ClickHouse grant, and data dropped before enabling it stayed
  permanently lost even after fixing the grant. Full writeup in
  `docs/architecture.md`.
- **Failure recovery**: hard-killing `kafka-connect` mid-batch (2,000 rows
  in flight) recovered with the exact row count — no loss, no
  duplicates — once the container came back, and both connectors resumed
  automatically without re-registration (their config lives in a
  Kafka-backed topic, not a one-time script run). Same Docker gotcha as
  `stream-cdc-peerdb`: `restart: unless-stopped` does not fire on
  `docker kill`.
- **Pausing the source connector measurably grows the source's retained
  WAL** (~506 kB → ~629 kB after 500 rows while paused) — same physical
  mechanism `stream-cdc-peerdb` demonstrated for PeerDB. A subtlety this
  test surfaced that the PeerDB test didn't need to: the reported slot size
  stayed flat for a full 10 minutes after resuming — across two forced
  `CHECKPOINT`s and past Kafka Connect's offset-flush interval — and only
  drained (629 kB → 17 kB, instantly) once one more trivial write hit the
  otherwise-idle source. A logical slot's `restart_lsn` needs a fresh
  `xl_running_xacts` WAL record to compute a new restart candidate;
  confirmation + checkpoints alone aren't sufficient on a quiet source.
- **The ClickHouse sink connector runs under a least-privilege user**, not
  a shared admin credential — `INSERT, SELECT` only by default, with the
  exact `ALTER ADD COLUMN` grant PeerDB's own docs require added and
  tested only for the `auto.evolve=true` experiment, then left out of the
  default setup.

## Status

- [x] Stage 1 — Architecture
- [x] Stage 2 — Minimal setup
- [x] Stage 3 — Registering connectors, verifying insert/update/delete flow
- [x] Stage 4 — Monitoring, schema evolution (both modes), failure
      recovery, WAL retention

## Prerequisites

- Docker + Docker Compose v2 (`docker compose version`)
- ~3 GB free RAM for the stack (5 containers: Kafka, Kafka Connect,
  kafka-ui, source Postgres, ClickHouse)
- Ports free on the host: `5433, 8124, 9010, 9094, 8087, 8086` — all
  chosen to avoid every port `stream-cdc-peerdb` uses, so **both stacks
  can run at the same time** for a direct side-by-side comparison

## Quickstart

```bash
cp .env.example .env
docker compose up -d --build
./scripts/register_connectors.sh
./scripts/verify_cdc.sh
```

First run pulls Kafka, Kafka Connect's base image, ClickHouse, and Postgres,
and builds the custom `kafka-connect` image (downloads the ClickHouse sink
connector plugin) — expect a few minutes depending on your connection.

Check that the stack is healthy:

```bash
docker compose ps
```

You should see `dbz-kafka`, `dbz-source-postgres`, `dbz-clickhouse`,
`dbz-kafka-connect`, and `dbz-kafka-ui` all `Up (healthy)`.

## What's running, and why it's shaped this way

| Service | Role | Host port(s) |
|---|---|---|
| `dbz-kafka` | Single-node Kafka broker, KRaft mode (no ZooKeeper) | `9094` (external listener) |
| `dbz-kafka-connect` | Custom image: Debezium's Connect image + the ClickHouse sink connector plugin (`kafka-connect/Dockerfile`) | `8087` (REST API) |
| `dbz-kafka-ui` | Topics, consumer lag, connector/task status in one dashboard | `8086` |
| `dbz-source-postgres` | The OLTP source being captured — same schema as `stream-cdc-peerdb` | `5433` |
| `dbz-clickhouse` | The OLAP destination | `8124` (HTTP), `9010` (native) |

Full mechanics, including *why* the ClickHouse sink connector doesn't need
a flattening transform and the KRaft env var setup, are in
[`docs/architecture.md`](docs/architecture.md).

### Source Postgres configuration

Same three `wal_level=logical` / `max_wal_senders` / `max_replication_slots`
flags as `stream-cdc-peerdb`, same reasoning. `postgres/init/01_schema.sql`
creates an explicit publication:

```sql
CREATE PUBLICATION dbz_pub FOR TABLE
    categories, customers, products, orders, order_items, payments;
```

`publication.autocreate.mode=disabled` in
`connectors/pg-source-connector.json` means the Debezium connector refuses
to start rather than creating a broader publication of its own if this one
is missing.

### ClickHouse configuration

Same least-privilege pattern as `stream-cdc-peerdb`: a bootstrap `ch_admin`
user (`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`) used only to provision a
scoped `clickhouse_etl` user
([`clickhouse/init/01_clickhouse_etl_user.sh`](clickhouse/init/01_clickhouse_etl_user.sh)),
which is what the sink connector actually authenticates as:

```sql
GRANT INSERT, SELECT ON debezium_cdc.* TO clickhouse_etl;
```

Deliberately narrower than PeerDB's grant set — this connector never runs
`CREATE TABLE`, and only needs `ALTER ADD COLUMN` if `auto.evolve=true` is
turned on (see the schema-evolution findings above). Destination tables
are pre-created by hand in
[`clickhouse/init/02_destination_tables.sql`](clickhouse/init/02_destination_tables.sql)
— unlike PeerDB, the sink connector never creates a table that doesn't
already exist.

## Stage 3: registering the connectors

Kafka Connect's REST API (`localhost:8087`) is the equivalent of PeerDB's
`CREATE MIRROR` — both connector configs are checked-in JSON
(`connectors/pg-source-connector.json`,
`connectors/ch-sink-connector.json`), applied by:

```bash
./scripts/register_connectors.sh
```

This `PUT`s each config to `/connectors/<name>/config`, which is
idempotent (creates or updates in place) — same re-runnable-script
discipline as `stream-cdc-peerdb`'s `create_mirror.sh`. It waits for the
REST API to become reachable, registers both connectors, and prints their
status.

### Verifying the pipeline

```bash
./scripts/verify_cdc.sh
```

Same shape as `stream-cdc-peerdb`'s script: row-count comparison, then a
live insert/update/delete against the source polled against ClickHouse
(up to 60s). Sample output from an actual run:

```
== Step 1: row counts, source vs. ClickHouse (FINAL, excluding soft-deletes) ==
  categories     source=5      clickhouse=5      OK
  customers      source=15     clickhouse=15     OK
  products       source=20     clickhouse=20     OK
  orders         source=30     clickhouse=30     OK
  order_items    source=35     clickhouse=35     OK
  payments       source=30     clickhouse=30     OK

== Step 2: live CDC test (insert / update / delete) ==
  insert propagated: yes
  update propagated: yes
  delete propagated (soft-delete tombstone): yes

CDC verification PASSED.
```

**The same gotcha as `stream-cdc-peerdb` applies here**: deletes are a
soft-delete tombstone (`is_deleted`), not a physical `DELETE`, because
`ReplacingMergeTree`'s merge-time dedup can't delete a single row out of an
existing part. Every query against a mirrored table needs:

```sql
SELECT * FROM debezium_cdc.order_items FINAL WHERE is_deleted = 0;
```

### Watching it work

- **kafka-ui** (`localhost:8086`) — topics, per-partition offsets/lag, and
  both connectors' task status in one place.
- **`scripts/connector_status.sh`** — the CLI equivalent: connector/task
  status via the Connect REST API, consumer lag via
  `kafka-consumer-groups.sh --describe`, replication slot size via
  `pg_replication_slots`, and current row counts in ClickHouse. Three
  different systems queried directly, where `stream-cdc-peerdb`'s
  `mirror_status.sh` queries one (`peerdb_stats`) — a concrete
  illustration of the "one product vs. assembled parts" tradeoff from the
  comparison table above.

## Stage 4: operations (monitoring, schema evolution, failure recovery)

Full detail, evidence, and exact reproduction commands for everything below
are in
[`docs/architecture.md`](docs/architecture.md#failure-modes-and-recovery-tested-against-this-projects-live-stack).

### Schema evolution

Tested with the feature both off and on, not assumed either way:

- **`auto.evolve=false`** (this pipeline's default): an added source
  column is silently dropped for any row synced while the destination
  table doesn't have it — no error, connector task stays `RUNNING`.
- **`auto.evolve=true`**: the connector runs `ALTER TABLE ... ADD COLUMN`
  automatically, genuinely matching PeerDB — but it requires an explicit
  `ALTER ADD COLUMN` grant this pipeline's default least-privilege user
  doesn't have, and fails the task loudly (not silently) when that grant
  is missing.

### Failure and recovery

- **A paused source connector doesn't degrade gracefully on the source**
  — the replication slot retains WAL until it's consumed, identical
  mechanism to PeerDB. Measured: ~506 kB → ~629 kB retained after 500 rows
  written while paused, and it stayed elevated for 10+ minutes after
  resuming on this otherwise-idle source until one more small write let
  Postgres compute a new restart position — see `docs/architecture.md`
  for the mechanism.
- **A hard-killed `kafka-connect` recovers with exact data integrity** — a
  2,000-row insert issued right as the container was `SIGKILL`'d landed at
  exactly the expected count once it came back, no loss or duplication,
  and both connectors resumed on their own.
- **`restart: unless-stopped` did not auto-restart the killed
  container** — same Docker behavior `stream-cdc-peerdb` found: `docker
  kill` is treated as intentional operator action, not a crash.

### ClickHouse table engine choice

Same `ReplacingMergeTree(_version)` reasoning as `stream-cdc-peerdb` — see
[`docs/architecture.md`](docs/architecture.md#clickhouse-table-engine-choice-why-replacingmergetree)
for the full comparison against `MergeTree`/`CollapsingMergeTree`/
`AggregatingMergeTree`.

## Production considerations (what I'd change for real)

- **No TLS anywhere** — Kafka's PLAINTEXT listeners, the ClickHouse native
  protocol, and every inter-service connection run unencrypted. Acceptable
  on a single Docker network on localhost; not acceptable across any real
  network boundary.
- **No schema registry / no Avro** — both connectors ship the full schema
  embedded in every JSON message (`schemas.enable=true`), rather than a
  compact Avro payload with a schema registry doing compatibility
  checking. A real production Debezium+Kafka deployment would very likely
  use Avro+Schema Registry; skipped here specifically to control scope for
  this project's Postgres→ClickHouse comparison, not because it doesn't
  matter — named explicitly rather than glossed over.
- **`auto.evolve=false` by default is a real operational gap**, not just a
  demo simplification — the tested finding above (an added column silently
  dropped, task health unaffected) means schema drift needs to be a
  coordinated migration (pause connectors, alter the destination DDL by
  hand or turn on `auto.evolve` + the grant it needs, resume), exactly the
  same discipline `stream-cdc-peerdb` recommends for PeerDB's own drop/
  rename gap.
- **Single Kafka broker, single Postgres instance, no HA** — a real
  deployment needs a multi-broker Kafka cluster (this project's KRaft setup
  is single-node by design, for local reproducibility) and a Postgres
  primary with standbys, with the same logical-slot-failover caveat
  `stream-cdc-peerdb` names.
- **No alerting wired up** — `scripts/connector_status.sh` is a
  manual/cron tool. A real deployment would ship Kafka Connect REST
  status, consumer lag, and `pg_replication_slots` size to
  Prometheus/Grafana with real paging thresholds.
- **Table volumes here are tiny by design** (tens to low thousands of
  rows) — this proves correctness and behavior, not throughput. Kafka
  partition count, Connect task parallelism (`tasks.max`), and ClickHouse
  part-merge tuning are all real levers a production initial-load and
  steady-state throughput would need that aren't exercised at this scale.

## Tearing down

```bash
docker compose down          # stop, keep data volumes
docker compose down -v       # stop and wipe all data (start fresh)
```

## Repo layout

```
docker-compose.yml            # full stack: Kafka (KRaft) + Kafka Connect (custom image)
                               # + kafka-ui + source Postgres + ClickHouse
.env.example                  # copy to .env
kafka-connect/
  Dockerfile                  # Debezium's Connect image + ClickHouse sink connector plugin
postgres/
  init/00_pg-hba-replication.sh # allows replication connections from Kafka Connect
  init/01_schema.sql            # e-commerce schema + publication (same data as stream-cdc-peerdb)
  init/02_seed.sql              # identical seed data to stream-cdc-peerdb
clickhouse/
  init/01_clickhouse_etl_user.sh # provisions the least-privilege clickhouse_etl user
  init/02_destination_tables.sql # hand-written ReplacingMergeTree DDL, one per table
connectors/
  pg-source-connector.json      # Debezium Postgres source connector config
  ch-sink-connector.json        # ClickHouse sink connector config (debeziumCDCEnabled)
scripts/
  register_connectors.sh        # applies both connector configs via Connect's REST API
  verify_cdc.sh                 # row-count check + live insert/update/delete test
  connector_status.sh           # monitoring: connector/task status, consumer lag,
                                 # replication slot size, ClickHouse row counts
docs/
  architecture.md                # CDC mechanics, diagram, engine rationale,
                                  # failure-mode evidence, comparison notes vs. stream-cdc-peerdb
```
