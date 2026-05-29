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
- `DorisConfig` — `fenodes`, `database`, `table`, `user`, `password_env` (name of the env var
  holding the password — never store the password in YAML), extra options.
- `SparkStreamingConfig` — `checkpoint_location`, `trigger`, `output_mode`, `extra_conf` map.

Validation fails fast on missing required fields; the Doris password is read from the env var
named by `password_env`.

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
3. **Task 3 — Wire `IngestionJob.main`** end to end (see component flow).
4. **Task 4 — Error handling / retry.**
5. **Task 5 — Metrics + structured (JSON) logging.**
6. **Task 6 — Integration tests** (embedded-kafka; mock/stub Doris).
