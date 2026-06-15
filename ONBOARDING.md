# Spark → Doris Ingestion — Onboarding / Resume Point

This guide lets a fresh Claude Code session recreate and continue this project from
its current state. It captures the design, the exact versions that work together, the
environment gotchas, and the in-JVM validation approach (no Docker, no real Doris).

---

## 1. What the project is

A **generic Spark Structured Streaming job** that reads Kafka messages and writes them
**verbatim** (schema-agnostic — the payload is never parsed) into Doris. One fat jar,
many jobs: behaviour is driven entirely by an external YAML config (topic / table /
cluster). All Doris tables share a fixed layout: six `kafka_*` columns + `ingestion_time`.

Full design lives in `docs/DESIGN.md`; build/process rules in `CLAUDE.md`.
Develop **one Task at a time** (Task 1→6), confirming with the user between tasks.

Doris columns (target layout, in DDL order):
`kafka_timestamp, kafka_partition, kafka_offset, kafka_key, kafka_value, kafka_headers, ingestion_time`

---

## 2. Environment gotchas (the hard-won bits)

- **No `mvn` on PATH.** Use IntelliJ's bundled Maven 3.9.6:
  ```bash
  export PATH="/opt/idea-IC-241.17011.79/plugins/maven/lib/maven3/bin:$PATH"
  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
  ```
- **No Docker** — do not use Testcontainers. Simulate Kafka in-JVM with `embedded-kafka`.
- **Spark on Java 17 needs `--add-opens`** or tests/jobs hit `InaccessibleObjectException`.
  Use Spark's full JavaModuleOptions set (java 17 is stricter than 11):
  ```bash
  export MAVEN_OPTS="-XX:+IgnoreUnrecognizedVMOptions \
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
  ```
- **`provided` Spark deps are on the TEST classpath.** This is why we can run real Spark
  (`local[*]`) inside JUnit without packaging or `spark-submit`.
- Maven Central is reachable, but **`search.maven.org` solrsearch times out** — query
  `repo1.maven.org/.../maven-metadata.xml` directly to discover versions.
- In Spark tests, call `spark.sparkContext().setLogLevel("WARN")` so `show()` output is
  readable (the logback/log4j2 dual-binding otherwise floods stdout with INFO).

---

## 3. Locked-in versions (verified to build & run together)

| Dependency | Version | Scope | Notes |
|---|---|---|---|
| Java | 17 | — | `maven.compiler.release=17` |
| Spark core/sql `_2.12` | 3.5.1 | **provided** | cluster supplies it |
| spark-sql-kafka-0-10 `_2.12` | 3.5.1 | compile | bundled into fat jar |
| **spark-doris-connector-spark-3.5** | **25.1.0** | compile | groupId `org.apache.doris`; old `*-3.5_2.12` no longer exists on Central. Latest line is 24/25/26.x |
| jackson-dataformat-yaml + databind | 2.15.2 | compile | **pinned to Spark 3.5's Jackson** to avoid conflicts |
| Lombok | 1.18.32 | provided | |
| slf4j-api / logback-classic | 2.0.7 / 1.4.14 | compile | see logging caveat |
| JUnit Jupiter | 5.11.0-M2 | test | |
| **embedded-kafka `_2.12`** | **3.4.1** | test | in-JVM Kafka broker; pinned to Kafka 3.4.1 because spark-sql-kafka 3.5.1 bundles **kafka-clients 3.4.1** + **scala-library 2.12.18** → broker/clients/Scala all aligned, minimal conflict |

Scala alignment matters: everything is **2.12**. Do NOT pull `spring-kafka-test` — it drags
in `kafka_2.13` (Scala 2.13) and clashes with Spark's 2.12.

---

## 4. Build / packaging

- `groupId=com.yourteam`, `artifactId=spark-doris-ingestion`, package `com.yourteam.ingestion`.
- Maven layout: `src/main/java`, `src/main/resources`, `src/test/java`.
- **maven-shade-plugin 3.6.0** (fat jar), bound to `package`:
  - `ManifestResourceTransformer` → `Main-Class: com.yourteam.ingestion.IngestionJob`
  - **`ServicesResourceTransformer`** — REQUIRED so Spark/Kafka/Doris DataSource registration
    in `META-INF/services` survives shading.
  - Filter out `META-INF/*.SF|*.DSA|*.RSA` and `module-info.class` (else "Invalid signature file").
  - `minimizeJar=false` (Spark uses heavy reflection).
- Result: `target/spark-doris-ingestion.jar` (~86 MB — the Doris connector is itself an
  uber-jar bundling hadoop/guava/gson; the shade "overlapping resource" warnings come from
  that and are harmless).
- `logback.xml`: console appender; `org.apache.spark` / `org.apache.kafka` at WARN.
  (Structured JSON logging is deferred to Task 5; on a real cluster log4j2 binds instead.)

Verify build: `mvn -B clean package` → BUILD SUCCESS, then `java -jar target/spark-doris-ingestion.jar`
runs the placeholder main.

---

## 5. The transformation (core of Task 3) — already implemented

`com.yourteam.ingestion.transform.MessageTransform.toDorisColumns(Dataset<Row> kafka)`:

```java
Column headersAsJson = to_json(
    expr("transform(headers, h -> struct(h.key AS key, CAST(h.value AS STRING) AS value))"));
return kafka.select(
    col("timestamp").alias("kafka_timestamp"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("kafka_value"),
    headersAsJson.alias("kafka_headers"),
    current_timestamp().alias("ingestion_time"));
```

Casting each header value to string first yields clean JSON
(`[{"key":"trace-id","value":"abc-123"}]`) instead of base64 bytes. Reader must set
`includeHeaders=true` so the `headers` column exists.

---

## 6. In-JVM validation (no Docker, no Doris) — PASSING

**Step A — Spark only** (`MessageTransformTest`): build a DataFrame with the exact Kafka
source schema (`key/value` binary, topic, partition, offset, timestamp, timestampType,
`headers` array<struct<key:string,value:binary>>), run `MessageTransform`, assert the
seven Doris columns. Proves Spark runs in-JVM and the transform is correct.

**Step B — embedded Kafka end-to-end** (`KafkaToDorisStreamTest`): in-JVM broker →
`KafkaProducer` publishes records w/ headers → Spark `readStream.format("kafka")` →
`MessageTransform` → **`memory` sink as a Doris stand-in** → assert rows. Proves the real
Kafka source path works without Docker. Key embedded-kafka (Scala) calls from Java:

```java
EmbeddedKafkaConfig cfg = EmbeddedKafkaConfig$.MODULE$.defaultConfig();
EmbeddedKafka$.MODULE$.start(cfg);                  // starts ZK + Kafka in-process
String bootstrap = "localhost:" + cfg.kafkaPort();  // default 6001
// ... produce with plain kafka-clients KafkaProducer, then readStream from bootstrap ...
EmbeddedKafka$.MODULE$.stop();                       // teardown (ignore ZK EndOfStreamException noise)
```

Use a `memory` sink + `query.processAllAvailable()` then `SELECT * FROM <queryName>` to
assert deterministically. A harmless `EndOfStreamException` from ZooKeeper appears at
teardown — tests still pass. A first-batch `WARN NetworkClient ... LEADER_NOT_AVAILABLE` is
also normal (broker just started, topic auto-creating) and self-heals on the next fetch.

**Step C — demo / "run it and see"** (`FakeDataConsumeDemoTest`): same path as Step B but it
publishes a few fake order events and **prints** the seven Doris columns (`result.show(false)`
+ field-by-field), so you can eyeball what the job reads from Kafka before `IngestionJob.main`
is wired (Task 3). Proves `kafka_value` is carried verbatim and `kafka_headers` is clean JSON
(`[{"key":"trace-id","value":"t-aaa"}]`, not base64).

Run (after `source dev-env.sh`): `mvn -B test` (all 8) or one class via `-Dtest=<ClassName>`,
e.g. `mvn -B -Dtest=FakeDataConsumeDemoTest test`.

---

## 7. Status & next steps

**Done:** Task 1 (pom + fat jar + logback, `mvn clean package` green). Plus a spike that
proves the front half (Kafka → Spark → transform → sink) end-to-end in-JVM. `MessageTransform`
is real and reusable; `IngestionJob.main` is still a placeholder.

> **2026-05-29 — recreated on macOS (arm64).** The repo had only this guide; the code was
> rebuilt from it here. Toolchain is now **Homebrew** (not the Linux/IntelliJ paths in §2):
> `openjdk@11` (`JAVA_HOME=/opt/homebrew/opt/openjdk@11`, runtime 11.0.31) + Maven 3.9.16.
> Just `source dev-env.sh` then `mvn …` — it is **cross-platform** (macOS Intel/ARM + Linux):
> sets `MAVEN_OPTS` and auto-detects a JDK 11 for `JAVA_HOME`. From-zero bootstrap: `SETUP.md`.
> Confirmed the question left open last session: multiple test classes each starting a
> SparkSession run fine together in one `mvn test` (same JVM) — `getOrCreate()` reuses the
> first session. Fat jar = 104 MB as before.
>
> **Full suite now `Tests run: 11, Failures: 0`** across 5 classes: `MessageTransformTest`,
> `KafkaToDorisStreamTest`, `FakeDataConsumeDemoTest`, `ConfigLoaderTest`, `IngestionPipelineTest`.
>
> Git: work lives on branch **`task1-skeleton`** — `1739574` Task 1, `9f4831f` Task 2,
> `68a15fc` docs+cross-platform dev-env, `65e5796` Task 3. `target/`, `.idea/`,
> `dependency-reduced-pom.xml` are gitignored. **Pushing is done by the human** — this
> sandboxed shell can't reach the git credential store (HTTPS + osxkeychain).

**Task 2 — DONE (2026-05-29):** immutable `@Value @Jacksonized` classes (`JobConfig`,
`KafkaConfig`, `DorisConfig`, `SparkStreamingConfig`) in `com.yourteam.ingestion.config`;
`ConfigLoader` parses snake_case YAML via `jackson-dataformat-yaml` (SNAKE_CASE naming
strategy, unknown keys ignored); `JobConfig.validate()` fails fast collecting *every* missing
field; `DorisConfig.resolvePassword(env)` reads the password from the env var named by
`password_env` (never in YAML). Example: `examples/job-config.yaml`. `ConfigLoaderTest` (5
tests) green; full suite now 8 tests green.

**Task 3 — DONE (2026-05-29):** `IngestionJob.main` is wired end to end. Logic lives in
`IngestionPipeline`: `buildSession` (applies `spark.extra_conf`; master comes from conf or
spark-submit), `readKafkaStream` (`includeHeaders=true`, optional `maxOffsetsPerTrigger`),
`dorisOptions` (4 core `doris.*` opts + verbatim extras), `parseTrigger`
(`once`/`availableNow`/`"N seconds"`/null), `dorisWriter`. `main` loads config → resolves
password → starts query → shutdown hook → `awaitTermination`; config/usage errors exit code 2
(verified via `java -jar`). `IngestionPipelineTest` (3 tests: in-JVM Kafka read, doris option
mapping, trigger parsing). Run a real job:
`DORIS_PASSWORD=... java -jar target/spark-doris-ingestion.jar examples/job-config.yaml`
(put `spark.master: "local[*]"` in `extra_conf` for off-cluster local runs).

**Task 4 — DONE (2026-06-01):** restart/retry on transient failures. New optional `retry:`
config section (`RetryConfig`, all defaults so existing configs are unchanged; range-validated)
drives `com.yourteam.ingestion.retry.RetryPolicy` (exponential backoff, cap, give-up after
`max_restarts`, counter reset after a healthy `reset_after_seconds` run — pure, unit-tested) and
`RetrySupervisor` (the start→await→restart loop; the query lifecycle, sleep and clock are injected
as functional interfaces so it's tested in-JVM with plain lambdas, no Spark/Doris). `IngestionJob.main`
now runs the query through the supervisor and the shutdown hook drains & stops whichever query is
active (graceful drain). Restarts resume from the checkpoint → no data loss. Schema-agnostic ⇒ no
parse-failure poison messages; bad Doris writes are handled by the connector's `doris.sink.max-retries`
(pass-through) plus the restart loop — a per-record DLQ is intentionally out of scope. New tests:
`RetryPolicyTest` (5), `RetrySupervisorTest` (4), `ConfigLoaderTest` +1. **Full suite now 21 tests green.**

**Task 5 — DONE (2026-06-01):** metrics + structured (JSON) logging. A `StreamingMetricsListener`
(`com.yourteam.ingestion.metrics`, registered via `spark.streams().addListener`) logs the query
lifecycle as single-line JSON — `query_started`, a `batch_progress` per micro-batch (batch id, num
input rows, input/processed rows-per-second), `query_terminated`. The JSON is the log *message*, so
it works under whichever binding is active (logback locally, log4j2 on a cluster) — chosen over a
full logback-JSON reconfig, which a cluster ignores. Formatting is pure & unit-tested in
`StreamingMetrics`/`JsonEvents` (compact Jackson JSON; empty-batch NaN rates render as `null`, not the
invalid token `NaN`). The listener's JSON sink is injectable, so `StreamingMetricsListenerTest` also
drives it on a real Spark `rate` query (no Kafka/Doris) and asserts a captured `batch_progress` line.
`IngestionJob.main` registers the listener and emits a `job_started` event. New tests:
`StreamingMetricsTest` (5), `StreamingMetricsListenerTest` (1). **Full suite now 27 tests green.**

**Task 6 — DONE (2026-06-01):** end-to-end integration test. `EndToEndIngestionTest` runs the real
production wiring (`IngestionPipeline.buildSession`/`readKafkaStream` + `MessageTransform`, real
checkpoint + `AvailableNow` trigger) from an embedded Kafka broker into `StubDorisSink` — a
`foreachBatch` writer that captures the rows that would be written (no Docker, no real Doris). It
asserts the fixed 7-column contract / verbatim payload (JSON and non-JSON) / clean header JSON end
to end, the `doris.*` option contract via `dorisOptions`, and that a restart resumes from the
checkpoint without reprocessing (the no-data-loss property the Task 4 supervisor relies on). The
only line still exercised solely on a real cluster is the literal `.format("doris")` write. New
tests: `EndToEndIngestionTest` (2). **Full suite now 29 tests green.**

**All six tasks are complete.**

---

## 8. Resume point — 2026-06-01 (Claude Code on the web session)

Stopped here for the day; pick up from this list next time.

**Done this session (beyond Tasks 1–6, all pushed):**
- **Toolchain/dep bump** → Java **17**, Spark **3.5.1**, spark-doris-connector **25.1.0**, Lombok
  **1.18.32**, JUnit **5.11.0-M2** (embedded-kafka kept at **3.4.1** — 3.9.0 breaks tests because
  Spark 3.5.1 bundles kafka-clients 3.4.1). `pom.xml`, `dev-env.sh`, surefire `argLine` and all docs
  updated; the Java-17 `--add-opens` is now Spark's full JavaModuleOptions set.
- **SessionStart hook** for Claude Code on the web: `.claude/hooks/session-start.sh` +
  `.claude/settings.json`. Remote-only, idempotent — installs JDK 17 (the web container ships 21)
  and persists `JAVA_HOME` + `MAVEN_OPTS` via `$CLAUDE_ENV_FILE` so `mvn` just works. **Synchronous.**
- **`docs/RUNBOOK.md`** — `spark-submit` runbook for **Spark Standalone** (client-mode submit with the
  Java-17 flags on driver+executors, Doris table DDL, secrets, monitoring via the JSON metrics,
  restart/resume, graceful shutdown, troubleshooting).
- **Doris password now defaults to empty.** `doris.password_env` is optional; `resolvePassword`
  returns `""` (no throw) when it's omitted or the env var is absent. `main` logs a WARN in that
  case. ⚠️ This removed the old fail-fast safety — revisit if prod should require a password.
- **Config file can live on S3/HDFS.** `ConfigLoader.load(String)` now accepts a Hadoop URI
  (`s3a://`, `hdfs://`, `file://`) and reads it via the Hadoop `FileSystem`; plain paths still use
  NIO. Added `hadoop-client-api` 3.3.4 as a **provided** dep (compile only; the cluster/connector
  supplies it at runtime) — this also stopped Hadoop being bundled, so the fat jar is now **~86 MB**
  (was ~104). `s3a://` needs `hadoop-aws` + creds at runtime; RUNBOOK §4 also documents the
  `aws s3 cp` pre-stage fallback.

- **Kafka SASL/SCRAM wired into the source.** `IngestionPipeline.readKafkaStream` (via the new
  pure `kafkaOptions`) hardcodes `kafka.security.protocol=SASL_PLAINTEXT` +
  `kafka.sasl.mechanism=SCRAM-SHA-512` + an **empty `kafka.sasl.jaas.config`** (TODO: fill the JAAS
  line before running against real Kafka). Because this can't talk to the PLAINTEXT embedded broker,
  `IngestionPipelineTest` and `EndToEndIngestionTest` now read inline-PLAINTEXT, and `kafkaOptions`
  is asserted by pure unit tests.

**Full suite: 35 tests green** (`mvn -B clean package`, fat jar ~86 MB).

**Branch state (IMPORTANT for next session):**
- Work lives on **`task1-skeleton`** and **`claude/brave-wozniak-qQJmR`** — both at the same commit.
- **The default branch `main` is still essentially empty** (just `README.md` + `.gitignore`). When
  starting a new web session, **base it on `task1-skeleton`**, NOT `main`. The SessionStart hook only
  applies to every branch once `task1-skeleton` is **merged into `main`** — that merge + any path
  adjustments are the user's to do.

**Open follow-ups (not started):**
1. Merge `task1-skeleton` → `main` (PR), so the hook + code are on the default branch.
2. Packaging/release wiring (GitHub Actions CI + a tag-driven release that attaches the jar).
3. Live smoke test against a real staging Doris (needs FE/Kafka endpoints + creds + network).

---

Build process reminder: `source dev-env.sh` (cross-platform; sets `JAVA_HOME` + `MAVEN_OPTS`)
then `mvn …`. From-zero setup on a new machine (incl. `git clone`) is in `SETUP.md`. The
original Linux/IntelliJ sandbox paths in §2 are historical.
