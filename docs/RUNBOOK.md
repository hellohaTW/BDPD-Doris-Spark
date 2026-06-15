# Runbook — running the job on a Spark Standalone cluster

How to deploy and operate the Kafka → Doris ingestion job with `spark-submit` on a **Spark
Standalone** cluster. Background: [docs/DESIGN.md](DESIGN.md); build/dev: [SETUP.md](../SETUP.md).

> One fat jar, many jobs — behaviour comes entirely from the YAML config you pass as the single
> argument. To run a different topic/table, change the YAML, not the jar.

---

## 1. Prerequisites

| Requirement | Notes |
|---|---|
| **Spark Standalone 3.5.1** | master + workers. Spark core/sql are `provided`, so the **cluster supplies them** — the jar must run on 3.5.1 (Scala 2.12). |
| **JDK 17** on every node | driver and executors. Spark on Java 17 needs the `--add-opens` flags below. |
| **The fat jar** | `target/spark-doris-ingestion.jar` from `mvn -B clean package` (Main-Class is `com.yourteam.ingestion.IngestionJob`). |
| **Kafka** reachable | from driver and executors (executors read the source). |
| **Doris** reachable | FE http port from driver and executors (the connector writes from executors). The target table must already exist with the fixed 7-column layout (§3). |
| **Durable checkpoint location** | HDFS / S3 / shared NFS reachable by all nodes. A local path only works on a single-node cluster. Required for restart/resume. |

The jar bundles the Kafka and Doris connectors, so **no `--packages` is needed**.

---

## 2. Build the artifact

```bash
source dev-env.sh            # JDK 17 + the Spark Java-17 module flags
mvn -B clean package         # -> target/spark-doris-ingestion.jar (~86 MB), 33 tests green
```

Copy the jar and your `job-config.yaml` to the host you will submit from.

---

## 3. Create the Doris target table (once per job)

Every target table shares the same seven columns, in this order. Example DDL (adjust model /
bucketing / replication for your cluster):

```sql
CREATE TABLE ods.orders_raw (
    kafka_timestamp  DATETIME,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    kafka_key        STRING,
    kafka_value      STRING,      -- the raw payload, verbatim
    kafka_headers    STRING,      -- JSON: [{"key":..,"value":..}, ...]
    ingestion_time   DATETIME     -- set by the job at write time
)
DUPLICATE KEY(kafka_timestamp, kafka_partition, kafka_offset)
DISTRIBUTED BY HASH(kafka_offset) BUCKETS 10
PROPERTIES ("replication_num" = "3");
```

> `DUPLICATE KEY` keeps every row (append-only, schema-agnostic). If you need dedup on
> partition+offset, switch to `UNIQUE KEY(kafka_partition, kafka_offset)` — but note Structured
> Streaming + checkpoint already gives at-least-once with resume, and a UNIQUE key makes re-delivered
> rows idempotent.

---

## 4. The config file

Copy [`examples/job-config.yaml`](../examples/job-config.yaml) and edit it. For a cluster submit,
**omit `spark.master`** under `spark.extra_conf` — `--master` on the command line supplies it
(`spark.master` in the YAML is only for off-cluster `java -jar` local runs). Key sections:

```yaml
kafka:   { bootstrap_servers: "...", topic: "orders", starting_offsets: "latest" }
doris:   { fenodes: "doris-fe:8030", database: "ods", table: "orders_raw",
           user: "ingest", password_env: "DORIS_PASSWORD" }   # password NEVER in YAML
spark:   { checkpoint_location: "hdfs:///ckpt/orders", trigger: "30 seconds" }
retry:   { max_restarts: 5, initial_backoff_seconds: 5, max_backoff_seconds: 300 }
```

The **password is read on the driver** from the env var named by `doris.password_env`, then handed
to the writer — so only the driver needs it in its environment (see §6). If `password_env` is
omitted or the env var is unset, the password defaults to empty (`""`) and the driver logs a WARN.

### Where the config file can live

The config-file argument may be a **local path** or a **Hadoop-FileSystem URI**:

```bash
spark-submit ... target/spark-doris-ingestion.jar /path/to/job-config.yaml          # local
spark-submit ... target/spark-doris-ingestion.jar s3a://my-bucket/job-config.yaml   # S3
spark-submit ... target/spark-doris-ingestion.jar hdfs:///configs/job-config.yaml   # HDFS
```

- `s3a://` / `hdfs://` are read via the Hadoop `FileSystem` (the same mechanism the checkpoint uses).
  For `s3a://` the runtime needs `hadoop-aws` + the AWS SDK and S3 credentials — on most clusters
  these are already configured (instance profile / `fs.s3a.*`) because the checkpoint also lives on
  S3. If `hadoop-aws` is missing, add `--packages org.apache.hadoop:hadoop-aws:3.3.4`.
- **Zero-dependency fallback** (works in any mode, no S3 setup needed): stage the file locally first.
  ```bash
  aws s3 cp s3://my-bucket/job-config.yaml /tmp/job-config.yaml
  spark-submit ... target/spark-doris-ingestion.jar /tmp/job-config.yaml
  ```
- Note: a bare `s3://...` (no `a`) is **not** supported by the bundled connector; use `s3a://`.

---

## 5. Submit (Standalone, client mode — recommended)

Client mode runs the driver on the host you submit from, so the `DORIS_PASSWORD` env var is trivially
available to it and the job's stdout (incl. the JSON metrics) is right in your terminal.

```bash
# Spark's full Java-17 module-access flags (same set as dev-env.sh / surefire).
ADD_OPENS="-XX:+IgnoreUnrecognizedVMOptions \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

export DORIS_PASSWORD='********'         # driver env; never in the YAML or the command line

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name spark-doris-ingestion \
  --conf "spark.driver.extraJavaOptions=$ADD_OPENS" \
  --conf "spark.executor.extraJavaOptions=$ADD_OPENS" \
  --total-executor-cores 4 \
  --executor-cores 2 \
  --executor-memory 4g \
  --driver-memory 2g \
  target/spark-doris-ingestion.jar \
  job-config.yaml
```

Notes:
- The app jar is followed by **one argument**: the path to the YAML. `--class` is optional (the
  Main-Class is in the manifest).
- No args → prints usage and exits **2**. Bad path / invalid YAML / missing field / unset
  `DORIS_PASSWORD` → logs the problem and exits **2**. A query failure that exhausts the restart
  budget → exits **1**.

### Cluster mode (driver runs on a worker)

Use `--deploy-mode cluster` when you want the driver supervised by the cluster. Two changes:
- **Ship the config**: add `--files job-config.yaml` and pass just the filename as the arg (Spark
  places `--files` next to the driver working dir).
- **Secret**: the driver no longer inherits your shell env. Provide `DORIS_PASSWORD` to the driver
  process out-of-band — e.g. a wrapper that exports it before the driver starts, or a node-level
  secret mechanism. Standalone has no per-driver env conf like YARN, so **client mode is simpler for
  secret handling** and is the recommended default here.

---

## 6. Secrets

- The Doris password is **only** ever read from the env var named by `doris.password_env`, on the
  **driver**, at startup. It is never stored in the YAML and never a command-line argument.
- It is then passed to the Doris writer as the `doris.password` option. Treat the Spark UI / event
  logs as sensitive if your environment exposes data-source options there, and restrict access
  accordingly.

---

## 7. Operating the job

**Monitoring** — the job emits structured JSON on the driver, one event per line:
```bash
# tail the driver output and pick out throughput per micro-batch
... | grep '"event":"batch_progress"'
# {"event":"batch_progress","name":null,"batch_id":12,"timestamp":"...","num_input_rows":4200,
#  "input_rows_per_second":1400.0,"processed_rows_per_second":1380.5}
```
Also `query_started` / `query_terminated` (with `exception` on failure) and a `job_started` line.
The Spark UI is on the driver at `http://<driver-host>:4040` while the job runs.

**Trigger** (`spark.trigger`): `"30 seconds"` (micro-batch every 30 s), omit for as-fast-as-possible,
`"once"` / `"availableNow"` to drain what's there and stop (good for backfills/cron).

**Restart / resume** — relaunch the **same command**; Structured Streaming resumes from the
`checkpoint_location`, so no data is lost and nothing is reprocessed. Keep the checkpoint durable and
**do not** delete it between runs unless you intend to re-read from `starting_offsets`.

**Automatic retry** — on a transient query failure the job restarts itself with exponential backoff
(the `retry:` section) before giving up; restarts resume from the checkpoint. Tune `max_restarts`
(`-1` = unlimited) and the backoff for your SLA.

**Graceful shutdown** — send SIGTERM (client mode: `Ctrl-C` / `kill <spark-submit-pid>`; cluster
mode: `spark-submit --master spark://spark-master:7077 --kill <driver-id>`). The shutdown hook
stops the active query, draining the in-flight batch and leaving the checkpoint consistent.

---

## 8. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `InaccessibleObjectException` / `IllegalAccessError` at startup | Missing Java-17 `--add-opens` on driver/executors. Set both `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` (§5). |
| Exit code 2 immediately | Usage or config error — bad YAML path, missing required field, or `DORIS_PASSWORD` not set in the driver env. |
| `WARN NetworkClient ... LEADER_NOT_AVAILABLE` on first batch | Benign — Kafka topic just being discovered; self-heals on the next fetch. |
| Job keeps restarting then exits 1 | Transient failures exhausted `retry.max_restarts`, or a real, persistent fault (Doris down, bad table). Check the `query_terminated` event's `exception` and the connector logs. |
| `ClassNotFound` / wrong Scala version | Cluster isn't Spark **3.5.1 / Scala 2.12**, or someone added a Scala 2.13 dep. The jar is `provided`-Spark; the cluster must match. |
| Offsets reset / re-reading from the start | The `checkpoint_location` was deleted or changed. Use a stable, durable path. |
| Checkpoint incompatibility after a code/config change | Structured Streaming checkpoints are tied to the query plan; an incompatible change needs a new checkpoint location (and re-reads per `starting_offsets`). |

---

## 9. Quick reference

```bash
# build
source dev-env.sh && mvn -B clean package

# run (client mode)
export DORIS_PASSWORD='********'
spark-submit --master spark://spark-master:7077 --deploy-mode client \
  --conf "spark.driver.extraJavaOptions=$ADD_OPENS" \
  --conf "spark.executor.extraJavaOptions=$ADD_OPENS" \
  target/spark-doris-ingestion.jar job-config.yaml

# graceful stop: Ctrl-C (client) or `spark-submit --master ... --kill <driver-id>` (cluster)
```
