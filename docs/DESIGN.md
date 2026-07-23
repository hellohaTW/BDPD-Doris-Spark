# Design — Spark → Doris generic ingestion

## Goal

One fat jar, many jobs. A single Spark Structured Streaming application reads a Kafka topic
and writes its messages **verbatim** into a Doris table. The job is:

- **Schema-agnostic** — the message payload is never parsed or validated. `key`/`value` are
  carried through as strings. The same jar works for any topic regardless of payload shape.
- **Config-driven** — all behaviour (which topic, which table, which cluster, tuning) comes
  from an external YAML file passed at launch. No code change per job.

## Fixed Doris target layout

Every target table has the same seven columns, in this DDL order:

```
kafka_timestamp   TIMESTAMP
kafka_partition   INT
kafka_offset      BIGINT
kafka_key         STRING / VARCHAR
kafka_value       STRING / VARCHAR (the raw payload, verbatim)
kafka_headers     STRING (JSON: [{"key":..,"value":..}, ...])
ingestion_time    TIMESTAMP (set by the job at write time)
```

The transform that produces these columns is
[`MessageTransform.toDorisColumns`](../src/main/java/com/yourteam/ingestion/transform/MessageTransform.java).
Header values are cast to string before `to_json` so the output is readable JSON rather than
base64 bytes. The Kafka reader must set `includeHeaders=true`.

## Component flow (target state, Task 3)

```
YAML config ──► JobConfig ──► SparkSession (+ spark.extra_conf)
                                   │
                                   ▼
                Kafka readStream (bootstrap, subscribe, startingOffsets,
                                  maxOffsetsPerTrigger, includeHeaders=true)
                                   │
                                   ▼
                MessageTransform.toDorisColumns  (the 7 columns)
                                   │
                                   ▼
                Doris writeStream (fenodes, db.table, user, password,
                                   extra options) + checkpoint/trigger/outputMode
```

## Config model (Task 2)

Immutable Lombok `@Value` classes, parsed from YAML via `jackson-dataformat-yaml`:

- `JobConfig` — root; holds the sections below + validation.
- `KafkaConfig` — `bootstrap_servers`, `topic`, `starting_offsets`, `max_offsets_per_trigger`.
- `DorisConfig` — `fenodes`, `database`, `table`, `user`, `password_env` (optional: name of the env
  var holding the password — never store the password in YAML), extra options.
- `SparkStreamingConfig` — `checkpoint_location`, `trigger`, `output_mode`, `extra_conf` map.

Validation fails fast on missing required fields. The Doris password is read from the env var named
by `password_env`; if that is omitted or the env var is absent, the password **defaults to empty**
(`""`) so a passwordless Doris user works out of the box (the job logs a WARN in that case). The
config-file argument is a local path read via NIO.

## Validation strategy (no Docker, no Doris)

- **Step A** ([MessageTransformTest](../src/test/java/com/yourteam/ingestion/transform/MessageTransformTest.java)):
  build a DataFrame with the exact Kafka source schema, run the transform, assert the 7 columns.
- **Step B** ([KafkaToDorisStreamTest](../src/test/java/com/yourteam/ingestion/KafkaToDorisStreamTest.java)):
  embedded Kafka broker → real Spark Kafka source → transform → `memory` sink (Doris stand-in).

Doris itself is heavy; keep the `memory` sink in tests. A live Doris write is exercised only
manually / in a real environment.

## Task plan

1. **Task 1 — Skeleton & build** ✅ pom + fat jar + logback; `MessageTransform` + both
   in-JVM tests green. `IngestionJob.main` is a placeholder.
2. **Task 2 — Config model & loading** ✅ `@Value` `@Jacksonized` classes
   ([config package](../src/main/java/com/yourteam/ingestion/config)) + `ConfigLoader`
   (snake_case YAML via jackson-dataformat-yaml) + fail-fast validation collecting every
   missing field + password read from the env var named by `password_env`. See
   [examples/job-config.yaml](../examples/job-config.yaml). Unit tests in `ConfigLoaderTest`.
3. **Task 3 — Wire `IngestionJob.main`** ✅ end to end (see component flow). Logic factored
   into [`IngestionPipeline`](../src/main/java/com/yourteam/ingestion/IngestionPipeline.java)
   (`buildSession` applies `spark.extra_conf`; `readKafkaStream`; `dorisOptions`; `parseTrigger`;
   `dorisWriter`). `IngestionJob.main` loads config, resolves the password, starts the query,
   adds a shutdown hook, and awaits. Config/usage errors exit cleanly (code 2). Run:
   `DORIS_PASSWORD=... java -jar target/spark-doris-ingestion.jar examples/job-config.yaml`
   (set `spark.master` in `extra_conf` for off-cluster local runs). Tests in
   `IngestionPipelineTest`.
4. **Task 4 — Error handling / retry** ✅ A restart supervisor wraps the query: on a transient
   failure it restarts with exponential backoff (resuming from the checkpoint, so no data loss)
   instead of dying. New optional `retry:` config section ([`RetryConfig`](../src/main/java/com/yourteam/ingestion/config/RetryConfig.java),
   defaults applied when omitted) drives [`RetryPolicy`](../src/main/java/com/yourteam/ingestion/retry/RetryPolicy.java)
   (backoff maths / give-up / counter-reset, pure & unit-tested) and
   [`RetrySupervisor`](../src/main/java/com/yourteam/ingestion/retry/RetrySupervisor.java) (the
   restart loop, run/sleep/clock injected so it's tested in-JVM without Spark). `IngestionJob.main`
   runs the query through the supervisor; the shutdown hook drains & stops whichever query is
   active (graceful drain). Schema-agnostic ⇒ no parse-failure poison messages; bad writes are
   covered by the connector's `doris.sink.max-retries` plus the restart loop (a per-record DLQ is
   intentionally out of scope). Tests: `RetryPolicyTest`, `RetrySupervisorTest`, `ConfigLoaderTest`.
5. **Task 5 — Metrics + structured (JSON) logging** ✅ A
   [`StreamingMetricsListener`](../src/main/java/com/yourteam/ingestion/metrics/StreamingMetricsListener.java)
   (registered on the SparkSession) logs the query lifecycle as single-line JSON: `query_started`,
   a `batch_progress` per micro-batch (batch id, num input rows, input/processed rows-per-second),
   and `query_terminated`. Formatting is pure & unit-tested in
   [`StreamingMetrics`](../src/main/java/com/yourteam/ingestion/metrics/StreamingMetrics.java) /
   [`JsonEvents`](../src/main/java/com/yourteam/ingestion/metrics/JsonEvents.java) (compact JSON via
   the pinned Jackson; non-finite rates on empty batches render as `null`, not `NaN`). The JSON is
   the log *message*, so it survives whichever binding is active (logback locally, log4j2 on a
   cluster) — more portable than reconfiguring logback, which a cluster ignores. The listener's
   JSON sink is injectable, so it's also exercised on a real Spark query
   ([`StreamingMetricsListenerTest`](../src/test/java/com/yourteam/ingestion/metrics/StreamingMetricsListenerTest.java)).
   `IngestionJob.main` also emits a `job_started` event. Tests: `StreamingMetricsTest`,
   `StreamingMetricsListenerTest`.
6. **Task 6 — Integration tests** ✅ (embedded-kafka; stub Doris). `EndToEndIngestionTest` runs
   the real production wiring (`IngestionPipeline.buildSession` / `readKafkaStream` /
   `MessageTransform.toDorisColumns`, with a real checkpoint and `AvailableNow` trigger) from an
   embedded Kafka broker into a [`StubDorisSink`](../src/test/java/com/yourteam/ingestion/StubDorisSink.java)
   (a `foreachBatch` writer capturing the rows that would be written — no Docker, no real Doris).
   It asserts the fixed 7-column contract / verbatim payload (JSON and non-JSON alike) / clean
   header JSON end to end, the `doris.*` option contract via `dorisOptions`, and that a restart
   resumes from the checkpoint without reprocessing (the no-data-loss property the Task 4 supervisor
   relies on). The literal `.format("doris")` write is the only line still exercised solely on a
   real cluster.

**All six tasks are complete; the full suite is 29 tests green.**

## One-shot Iceberg → Doris migration (batch, separate job)

A second entry point in the same fat jar,
[`IcebergMigrationJob`](../src/main/java/com/yourteam/ingestion/IcebergMigrationJob.java), does a
**one-time batch** copy of an Iceberg table into a Doris table. It is distinct from the streaming
ingestion job in three ways: the source is an **Iceberg table** (not Kafka), the **source schema is
preserved verbatim** (a 1:1 column copy — *not* the fixed 7-column Kafka layout), and it is
**batch**, running once and exiting (no checkpoint/trigger/retry-supervisor).

```
migration-config.yaml ──► MigrationConfig ──► SparkSession (+ spark.sql.catalog.<name>.*, extra_conf)
                                                    │
                                                    ▼
                          spark.table("catalog.db.table")   (Iceberg source, schema verbatim)
                                                    │
                                                    ▼
                          df.write.format("doris").mode(Overwrite)  (+ doris.* options)
```

- **Config** ([`MigrationConfig`](../src/main/java/com/yourteam/ingestion/config/MigrationConfig.java)):
  a new root type — `iceberg` ([`IcebergConfig`](../src/main/java/com/yourteam/ingestion/config/IcebergConfig.java))
  + reuse of `DorisConfig` (same `password_env` handling) + a batch-only
  [`SparkBatchConfig`](../src/main/java/com/yourteam/ingestion/config/SparkBatchConfig.java) (`extra_conf`
  only, no checkpoint/trigger). Both roots implement
  [`Validatable`](../src/main/java/com/yourteam/ingestion/config/Validatable.java), so `ConfigLoader`
  parses/validates either through one generic `load(path, type)`. Example:
  [examples/migration-config.yaml](../examples/migration-config.yaml).
- **Iceberg source**: read through a Spark v2 catalog. `catalog_type` defaults to **`hive`** (Hive
  Metastore, `uri: thrift://…`); `hadoop` (needs `warehouse`) and `rest` are also accepted. The
  `spark.sql.catalog.<name>.*` conf is built by
  [`IcebergToDorisMigrationPipeline.icebergCatalogConf`](../src/main/java/com/yourteam/ingestion/IcebergToDorisMigrationPipeline.java)
  (pure, unit-tested). `iceberg-spark-runtime-3.5_2.12` is **bundled** into the fat jar (like the
  Doris connector), so `spark-submit` needs no extra packages flag.
- **Doris sink**: `SaveMode.Overwrite` — the connector **replaces the whole target table** (truncate
  then load; not an atomic swap, so a mid-write failure leaves the table empty/partial — re-run to
  recover, which is safe because Overwrite is idempotent). The **target table must already exist**
  with a compatible schema; this job issues **no DDL**. Columns match **by name**, so Iceberg and
  Doris column names must line up and the Iceberg types must map to compatible Doris types.
- **Run**:
  ```
  DORIS_PASSWORD=… spark-submit --class com.yourteam.ingestion.IcebergMigrationJob \
    target/spark-doris-ingestion.jar examples/migration-config.yaml
  ```
- **Validation (no Docker / no Doris / no metastore)**: the Iceberg read path is exercised in-JVM
  against a real **HadoopCatalog** over a temp warehouse
  ([`IcebergMigrationReadTest`](../src/test/java/com/yourteam/ingestion/IcebergMigrationReadTest.java)),
  asserting the source schema/rows survive the read verbatim; the catalog conf and the Doris
  option/mode wiring are asserted purely
  ([`IcebergToDorisMigrationPipelineTest`](../src/test/java/com/yourteam/ingestion/IcebergToDorisMigrationPipelineTest.java),
  [`MigrationConfigLoaderTest`](../src/test/java/com/yourteam/ingestion/config/MigrationConfigLoaderTest.java)).
  The literal `.format("doris").save()` is the only line exercised solely on a real cluster.

**Suite is 42 tests green** (9 new: 4 config-loader/validation, 3 pure catalog-conf, 2 in-JVM
HadoopCatalog read).
