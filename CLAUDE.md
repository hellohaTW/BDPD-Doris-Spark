# CLAUDE.md — build & process rules

Working notes for any Claude Code session on this repo. Full design is in
[docs/DESIGN.md](docs/DESIGN.md); the resume guide is [ONBOARDING.md](ONBOARDING.md).

## Project in one line

A generic Spark Structured Streaming job: Kafka → Doris, **schema-agnostic** (payload
never parsed, carried verbatim), **config-driven** (one fat jar, behaviour from external
YAML). Every Doris table shares a fixed 7-column layout.

## Build / test

Need **JDK 17** + Maven (install per OS — see [SETUP.md](SETUP.md) Step 1).

```bash
source dev-env.sh          # cross-platform (macOS/Linux): auto-detects JDK 17 -> JAVA_HOME, sets MAVEN_OPTS
mvn -B clean package       # fat jar -> target/spark-doris-ingestion.jar, runs all tests
mvn -B -Dtest=ConfigLoaderTest test
mvn -B -Dtest=FakeDataConsumeDemoTest test
```

Full from-zero bootstrap (incl. `git clone`) is in [SETUP.md](SETUP.md). The original
Linux/IntelliJ sandbox paths are in ONBOARDING.md §2 (historical).

## Non-negotiables

- **Java 17.** `maven.compiler.release=17`. Spark on Java 17 needs the `--add-opens` flags
  (Spark's full JavaModuleOptions set is already in `dev-env.sh` and in surefire's `argLine`).
- **No Docker / no Testcontainers / no real Doris.** Validate in-JVM: real Spark `local[*]`
  (provided deps are on the test classpath) + `embedded-kafka` broker + `memory` sink as a
  Doris stand-in.
- **Everything is Scala 2.12.** Do not pull `spring-kafka-test` (drags in Scala 2.13).
- Jackson pinned to **2.15.2** (Spark 3.5's version). embedded-kafka pinned to **3.4.1**
  (matches Spark 3.5.1's bundled kafka-clients 3.4.1).
- Shade plugin **must** keep `ServicesResourceTransformer` (DataSource registration) and
  filter `META-INF/*.SF|*.DSA|*.RSA` + `module-info.class`.

## Workflow

Develop **one Task at a time** (Task 1→6), confirm with the user between tasks. Current
status and the Task list live in ONBOARDING.md §7 and docs/DESIGN.md.
