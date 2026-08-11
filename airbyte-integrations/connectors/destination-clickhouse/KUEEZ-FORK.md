# destination-clickhouse — Kueez fork: what it changes, how to build and ship it

This branch carries our changes to the ClickHouse destination so it works against a
ClickHouse cluster whose databases use the **`Replicated` database engine** (multi-replica,
Keeper-coordinated DDL). Upstream `destination-clickhouse` assumes a single node, and several
of its DDL paths are either wrong or pathologically expensive there.

There is no upstream PR for any of this yet. The image is built and pushed by hand — this
document is the whole pipeline.

---

## 1. Why the fork exists

Upstream behaviour vs what a Replicated cluster needs:

| Upstream does | On a Replicated cluster that means | We changed it to |
|---|---|---|
| `CREATE OR REPLACE TABLE` | Can half-commit behind a non-deterministic LB: the staged `_tmp_replace_<hash>` table's Keeper metadata node goes missing and the table becomes **un-droppable** (`DROP`, `DETACH PERMANENTLY`, `CREATE OR REPLACE` all fail `Transaction failed (No node)`) | explicit `DROP … SYNC` + plain `CREATE [IF NOT EXISTS]`, plus the server settings those paths need |
| `createNamespace` once **per stream** | `CREATE DATABASE` is issued `ON CLUSTER` (a database cannot self-propagate its own creation, unlike table DDL inside it), so each call costs a `/clickhouse/task_queue/ddl` entry processed by every replica at `distributed_ddl.pool_size = 1` | one `CREATE DATABASE` per **namespace** per sync (`.distinct()`) |
| `ON CLUSTER` on table DDL | Redundant inside a Replicated database — the DB replicates its own table DDL | skipped for table DDL, kept for `CREATE DATABASE` (that one is required) |
| `getGenerationId` on an empty table | NPE / Micronaut null-check failure | null-safe |
| Data copy on `overwrite` / `clear` syncs | Copies rows that are about to be thrown away | skipped |

Plus CDK-level fixes we needed in production: force-emit unflushed states after retries so a
CDC position isn't lost, and a Jackson max-string-length bump for large rows.

Read the commit messages — each one states the measurement or failure that motivated it.
Two worth knowing by heart:

- `fix(destination-clickhouse): avoid CREATE OR REPLACE TABLE on Replicated DB`
- `fix(destination-clickhouse): create each namespace once per sync`

### Config flag

The Replicated-specific behaviour is gated on the connector config option
**`use_replicated_engine`**. Leave it off and the connector behaves like upstream.

---

## 2. Build and push an image

Everything is manual. There is no GitHub Action, no Jenkins job, no Argo hook for this
connector — the only automation is the tag convention.

### 2.1 Tag convention

```
<registry>/destination-clickhouse:<upstream-version>-replicated-<n>
```

e.g. `…:2.1.23-replicated-20`. `<upstream-version>` is `dockerImageTag` from
[`metadata.yaml`](metadata.yaml); `<n>` is a monotonic counter for *our* builds off that
version. Bump `<n>` on every push — never overwrite a tag that a running Airbyte connection
points at, or you lose your rollback target.

### 2.2 Test before you build

```bash
# from the repo root
./gradlew :airbyte-integrations:connectors:destination-clickhouse:test
```

Unit tests cover the fork's DDL behaviour directly — `ClickhouseSqlGeneratorTest`,
`ClickhouseAirbyteClientTest`, `ClickHouseWriterTest`. If you change a DDL path, add a test
there; the regression tests for the two fixes above are written so that they **fail** if the
fix is reverted.

For an end-to-end run against real MySQL and ClickHouse without any Airbyte control plane,
[`local-pipeline.sh`](local-pipeline.sh) pipes `source-mysql` STDOUT into this connector's
STDIN exactly as the k8s replication job does:

```bash
MYSQL_HOST=… MYSQL_USER=… MYSQL_PASSWORD=… MYSQL_DATABASE=… \
CLICKHOUSE_HOST=… CLICKHOUSE_USERNAME=… CLICKHOUSE_PASSWORD=… \
DEST_IMAGE=<registry>/destination-clickhouse:<tag> \
./local-pipeline.sh run --tables my_table --mode full_refresh
```

### 2.3 Build the distribution, then the image

```bash
# 1. connector tarball (skip tests here only if you already ran them)
./gradlew :airbyte-integrations:connectors:destination-clickhouse:distTar

# 2. the Dockerfile expects build/distributions/airbyte-app.tar.
#    If your build emits destination-clickhouse-<version>.tar instead, copy it into place:
cd airbyte-integrations/connectors/destination-clickhouse
ls build/distributions/
# cp build/distributions/destination-clickhouse-*.tar build/distributions/airbyte-app.tar

# 3. amd64 image — the OKE nodes are x86; building on an Apple Silicon Mac
#    without --platform produces an arm64 image that will CrashLoopBackOff.
docker buildx build --platform linux/amd64 \
  -t <registry>/destination-clickhouse:2.1.23-replicated-<n> \
  --push .
```

[`Dockerfile`](Dockerfile) is runtime-only: it layers the tarball onto
`airbyte/java-connector-base` (pinned by digest in `metadata.yaml` →
`connectorBuildOptions.baseImage`) and sets the standard Airbyte entrypoint. It does not
compile anything.

`--push` writes straight to the registry, so log in first (`docker login <registry>`).
Building without `--push` and pushing later is fine too — just don't forget the platform flag.

---

## 3. Register the new tag in Airbyte

The connector is installed as a **custom destination definition** (`custom: true`), which is
why it does not follow upstream's release channel. Two ways to move a workspace onto a new tag:

**UI** — Settings → Destinations → the custom ClickHouse entry → change version → the new tag.

**API** — against the Airbyte server (in-cluster, port 8001):

```bash
# read current state, including which tag is live
curl -s -X POST localhost:8001/api/v1/destination_definitions/list \
  -H 'Content-Type: application/json' -d '{}' | jq '.destinationDefinitions[]
  | select(.custom == true) | {destinationDefinitionId, name, dockerRepository, dockerImageTag}'

# move it to the new tag
curl -s -X POST localhost:8001/api/v1/destination_definitions/update \
  -H 'Content-Type: application/json' \
  -d '{"destinationDefinitionId":"<id>","dockerImageTag":"2.1.23-replicated-<n>"}'
```

Running syncs finish on the old image; the next replication job launches on the new one.
Confirm what is actually running rather than what the UI claims:

```bash
kubectl -n airbyte get pods -o json | jq -r '.items[]
  | select(.metadata.name|startswith("replication-job"))
  | .spec.containers[].image' | grep clickhouse | sort -u
```

### 3.1 The `supports_refreshes` trap — check this on every re-registration

`metadata.yaml` sets `supportsRefreshes: true`, but that flag has **failed to propagate into
the platform's config DB at registration time**, landing as
`actor_definition_version.supports_refreshes = FALSE`. When it is false the platform sends
`generationId = minimumGenerationId = 0`, the CDK picks `AppendStreamLoader` (no-op teardown),
and so:

- `overwrite` **never truncates** → the destination table grows forever, one generation per
  sync (we reached 1.12B rows in a table that should hold ~100k, ~110 GiB of bloat);
- `POST /api/v1/connections/refresh` returns `200 {}` and **silently no-ops**.

After registering a new version, verify on the destination cluster:

```sql
SELECT count() AS rows, uniqExact(_airbyte_generation_id) AS gens,
       max(_airbyte_generation_id) AS gen, max(_airbyte_extracted_at) AS fresh
FROM <db>.<an_overwrite_table>;
```

`gens = 1` and a moving `gen` = healthy. `gens > 1`, or `gen` stuck at `0`, = the flag did not
propagate. Fix it on `actor_definition_version` for that definition in the Airbyte config DB.

---

## 4. Post-deploy verification

Run these against the destination cluster after a version bump.

```sql
-- 1. CREATE DATABASE volume per sync round. per_round should equal the number of
--    DATABASES a connection writes to, NOT its stream count.
SELECT query, count() AS n,
       uniqExact(toStartOfFiveMinute(event_time)) AS windows,
       round(n / windows, 1) AS per_round
FROM system.query_log
WHERE event_time > now() - INTERVAL 1 HOUR AND type = 1 AND query ILIKE 'CREATE DATABASE%'
GROUP BY query ORDER BY n DESC;

-- 2. Keeper DDL queue: if this is pinned at distributed_ddl.max_tasks_in_queue
--    (default 1000) over a short time span, something is flooding it.
SELECT count() AS entries, min(query_create_time), max(query_create_time)
FROM system.distributed_ddl_queue;

-- 3. No un-droppable staging orphans left by the overwrite path.
SELECT name FROM system.tables WHERE name LIKE '_tmp_replace%';

-- 4. Overwrite truncation still works (see 3.1).
SELECT count(), uniqExact(_airbyte_generation_id) FROM <db>.<an_overwrite_table>;

-- 5. Errors attributable to the connector.
SELECT exception_code, count() FROM system.query_log
WHERE event_time > now() - INTERVAL 3 HOUR AND type > 1 AND user = '<airbyte_ch_user>'
GROUP BY exception_code ORDER BY 2 DESC;
```

On (5), expect a steady stream of `exception_code = 60` (`UNKNOWN_TABLE`). That is a **known
open issue, not a regression**: per stream per sync the destination probes for a leftover temp
table with `SELECT count(1) FROM <db>.<squeezed_name><md5>` and reads the resulting error as
"no leftover table" — see `countTable` in
[`ClickhouseSqlGenerator.kt`](src/main/kotlin/io/airbyte/integrations/destination/clickhouse/client/ClickhouseSqlGenerator.kt),
whose caller swallows the exception. Data is unaffected; the cost is that ClickHouse logs each
miss with a ~30-frame stack trace **twice**, which is hundreds of thousands of log lines a day
burying real errors. The real fix is `EXISTS TABLE` (or a `system.tables` lookup) instead of a
counting query on the error path.

**Rollback** is re-registering the previous `-replicated-<n-1>` tag. Nothing else to undo:
these changes are DDL-path behaviour, not schema migrations.

---

## 5. Known gaps in this pipeline

Worth knowing before you rely on it:

1. **No CI.** Nothing builds, tests, scans or pushes this image automatically. A build happens
   because a human ran the commands in §2.
2. **No upstream PR.** The fork drifts from `airbytehq/airbyte` (fork point: Feb 2026). Rebasing
   onto a newer upstream is a real merge exercise, particularly around the CDK commits.
3. **Image provenance is a local machine.** Tags do not carry a source-commit label. Record which
   commit produced which `-replicated-<n>` tag when you push, or add
   `--label org.opencontainers.image.revision=$(git rev-parse HEAD)` to the buildx command and
   make that the habit.
4. **Image pull auth is worth verifying** before you scale or replace nodes. Confirm the cluster
   can actually pull the tag onto a node that has never run it, rather than assuming a cached
   image means a working pull path.
