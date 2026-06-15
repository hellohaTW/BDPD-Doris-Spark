# SETUP — From zero on a fresh machine

> **Audience:** a Claude Code session (or a human) on a brand-new, empty environment that
> needs to get this project building and continue development. Follow the steps top to bottom;
> each step says what to run and what success looks like. When in doubt, prefer the exact
> versions and commands here — they are known to work together.

This is the bootstrap guide. Once the project builds, the deeper context lives in:
- [ONBOARDING.md](ONBOARDING.md) — design, locked-in versions, gotchas, current status & next task.
- [CLAUDE.md](CLAUDE.md) — build/process rules and non-negotiables.
- [docs/DESIGN.md](docs/DESIGN.md) — architecture and the Task 1→6 plan.
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — running on a real Spark Standalone cluster (`spark-submit`).

---

## What this project is (one paragraph)

A generic **Spark Structured Streaming** job: reads a Kafka topic and writes messages
**verbatim** (schema-agnostic — payload never parsed) into **Doris**. One fat jar, many jobs;
behaviour comes entirely from an external YAML config. Every Doris table shares a fixed
7-column layout (`kafka_timestamp, kafka_partition, kafka_offset, kafka_key, kafka_value,
kafka_headers, ingestion_time`). Built and tested **in-JVM** — no Docker, no real Doris.

---

## Step 0 — Clone the repo

```bash
git clone https://github.com/hellohaTW/BDPD-Doris-Spark.git
cd BDPD-Doris-Spark
```

Development happens on the **`task1-skeleton`** branch (Task 1 + Task 2 are committed there;
`main` may still be the initial commit). Check it out:

```bash
git checkout task1-skeleton    # if it 404s, the work was merged to main — stay on main
git log --oneline -3           # expect: Task 2 ... / Task 1 ... / Initial commit
```

---

## Step 1 — Install the toolchain (Java 17 + Maven)

**Hard requirement: JDK 17.** `maven.compiler.release=17`. Other JDKs may fail or need extra
flags. Maven 3.9.x is fine.

### macOS (Homebrew)

```bash
# Install Homebrew first if missing: https://brew.sh
brew install openjdk@17 maven        # openjdk@17 is keg-only (no sudo, doesn't touch system Java)
```

`JAVA_HOME` for this install is `/opt/homebrew/opt/openjdk@17` (Apple Silicon) or
`/usr/local/opt/openjdk@17` (Intel). You don't need to set it by hand — [dev-env.sh](dev-env.sh)
auto-detects it in Step 2 (it finds brew even if it isn't on your PATH yet).

### Linux (Debian/Ubuntu)

```bash
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk maven
```

`dev-env.sh` (Step 2) auto-detects this JDK from `javac` on PATH, so you normally don't need
to set `JAVA_HOME` by hand. (RHEL/Fedora: `sudo dnf install java-17-openjdk-devel maven`.)

### Verify

```bash
"$JAVA_HOME/bin/java" -version    # expect: openjdk version "17.0.x"
mvn -version                      # expect: Apache Maven 3.9.x, Java version: 17.0.x
```

If `mvn -version` reports a Java other than 17, fix `JAVA_HOME` before going on.

---

## Step 2 — Set the build environment

Spark on Java 17 needs `--add-opens` flags or it throws `InaccessibleObjectException`. The repo
ships a **cross-platform** helper — source it once per shell:

```bash
source dev-env.sh
```

Works on **macOS (Intel or Apple Silicon) and Linux**. It sets `MAVEN_OPTS` (the `--add-opens`
flags) and auto-detects a JDK 17 for `JAVA_HOME` — in this order: a `JAVA_HOME` you've already
exported → macOS `java_home`/Homebrew → `javac` on PATH → common Linux JVM dirs. It prints the
resolved `JAVA_HOME` and `java -version`. To force a specific JDK, `export JAVA_HOME=/path/to/jdk17`
before sourcing. If it warns it can't find JDK 17, do Step 1 first.

Prefer not to source it? Set the two vars yourself (this is Spark's full JavaModuleOptions set):

```bash
export JAVA_HOME=/path/to/jdk-17                 # e.g. /usr/lib/jvm/java-17-openjdk-amd64
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

(The surefire plugin also injects the `--add-opens` flags for tests via `argLine`, so test runs
work even without `MAVEN_OPTS`; the env var matters for any direct `spark-submit`/`java` runs.)

---

## Step 3 — Build and run the tests (the real verification)

```bash
mvn -B clean package
```

Expected:
- First run downloads a lot (Spark, the Doris connector uber-jar). Subsequent runs are seconds.
- `BUILD SUCCESS`.
- `target/spark-doris-ingestion.jar` (~86 MB fat jar).
- `Tests run: 29, Failures: 0, Errors: 0, Skipped: 0` across 10 test classes.
- A wall of `overlapping resource`/`overlapping classes` **WARNINGs** from the shade plugin —
  **these are harmless** (the Doris connector bundles hadoop/guava/gson). Do not "fix" them.

Run a subset while iterating:

```bash
mvn -B test                                    # all tests
mvn -B -Dtest=ConfigLoaderTest test            # just the config tests
mvn -B -Dtest=FakeDataConsumeDemoTest test     # the "see it work" demo (below)
```

---

## Step 4 — See it actually consume Kafka

There is no live Kafka/Doris. Validation runs **in-JVM**: real Spark `local[*]` (Spark deps are
`provided`, so they're on the test classpath) + an embedded Kafka broker + a `memory` sink as a
Doris stand-in. The demo test publishes fake events and **prints** the result:

```bash
mvn -B -Dtest=FakeDataConsumeDemoTest test
```

You should see a table of the 7 Doris columns, e.g.:

```
|kafka_offset|kafka_key |kafka_value                             |kafka_headers                       |
|0           |order-1001|{"orderId":1001,"item":"coffee","qty":2}|[{"key":"trace-id","value":"t-aaa"}]|
```

That proves the Kafka → Spark → transform path: `kafka_value` carried verbatim, `kafka_headers`
rendered as clean JSON (not base64). A first-batch `WARN ... LEADER_NOT_AVAILABLE` and a
teardown `EndOfStreamException` from ZooKeeper are both normal noise.

> `IngestionJob.main` is wired (Task 3), so you can also launch the real job against your own
> Kafka + Doris:
> ```bash
> DORIS_PASSWORD=... java -jar target/spark-doris-ingestion.jar examples/job-config.yaml
> ```
> With no args it prints usage; a bad/invalid config exits cleanly (code 2). For off-cluster
> local runs, set `spark.master: "local[*]"` under `spark.extra_conf` in the YAML.

---

## Step 5 — Where to continue

Read [ONBOARDING.md §7](ONBOARDING.md) for the live status. As of this writing:

- **Task 1 (skeleton + fat jar + in-JVM tests)** — done.
- **Task 2 (config model + YAML loader + validation + password-from-env)** — done; see
  `com.yourteam.ingestion.config` and [examples/job-config.yaml](examples/job-config.yaml).
- **Task 3 (wire `IngestionJob.main` end to end)** — done; logic in
  `com.yourteam.ingestion.IngestionPipeline` (`buildSession` / `readKafkaStream` /
  `dorisOptions` / `parseTrigger` / `dorisWriter`), `main` loads config → starts query →
  shutdown hook → awaits. Launch a real job with the `java -jar … examples/job-config.yaml`
  command above.
- **Task 4 (error handling / retry)** — done; optional `retry:` config drives a restart
  supervisor that restarts the query with exponential backoff on transient failures (resuming
  from the checkpoint, no data loss). See `com.yourteam.ingestion.retry`.
- **Task 5 (metrics + structured JSON logging)** — done; a `StreamingMetricsListener` logs the
  query lifecycle + per-batch throughput as single-line JSON. See `com.yourteam.ingestion.metrics`.
- **Task 6 (integration tests)** — done; `EndToEndIngestionTest` drives the real production wiring
  from embedded Kafka into a stub Doris sink, asserting the 7-column contract and checkpoint resume.

**All six planned tasks are complete.**

**Workflow rule:** develop **one Task at a time**, confirming with the user between tasks.
**Pushing:** commits are made locally; the human pushes (this sandbox can't reach the git
credential store).

---

## Troubleshooting / gotchas (the time-savers)

| Symptom | Cause / fix |
|---|---|
| `InaccessibleObjectException` in tests/run | Missing `--add-opens` — `source dev-env.sh` or export `MAVEN_OPTS` (Step 2). |
| `mvn` uses the wrong Java | `JAVA_HOME` not pointing at JDK 17. Re-check Step 1 "Verify". |
| Shade "overlapping resource/classes" warnings | Harmless; the Doris connector is an uber-jar. Leave them. |
| `LEADER_NOT_AVAILABLE` warning in a Kafka test | Broker just started / topic auto-creating; self-heals next fetch. |
| Want to add a Kafka test lib | Use `embedded-kafka_2.12` **3.4.1** only. Do **not** add `spring-kafka-test` (pulls Scala 2.13 and clashes with Spark's 2.12). |
| No internet for Maven Central | First build needs it (downloads Spark/Doris). After that the local `~/.m2` cache suffices. |

Everything is **Scala 2.12**, **Jackson 2.15.2**, **Spark 3.5.1**, **Doris connector 25.1.0**.
See the full locked-version table in [ONBOARDING.md §3](ONBOARDING.md).
