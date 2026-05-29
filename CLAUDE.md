# CLAUDE.md — build & process rules

Working notes for any Claude Code session on this repo. Full design is in
[docs/DESIGN.md](docs/DESIGN.md); the resume guide is [ONBOARDING.md](ONBOARDING.md).

## Project in one line

A generic Spark Structured Streaming job: Kafka → Doris, **schema-agnostic** (payload
never parsed, carried verbatim), **config-driven** (one fat jar, behaviour from external
YAML). Every Doris table shares a fixed 7-column layout.

## Build / test (macOS, Homebrew toolchain)

```bash
source dev-env.sh          # sets brew env, JAVA_HOME=openjdk@11, MAVEN_OPTS (--add-opens)
mvn -B clean package       # fat jar -> target/spark-doris-ingestion.jar, runs tests
mvn -B -Dtest=MessageTransformTest test
mvn -B -Dtest=KafkaToDorisStreamTest test
```

On the original Linux/IntelliJ sandbox the toolchain differs — see ONBOARDING.md §2.

## Non-negotiables

- **Java 11.** `maven.compiler.release=11`. Spark on Java 11 needs the `--add-opens` flags
  (already in `dev-env.sh` and in surefire's `argLine`).
- **No Docker / no Testcontainers / no real Doris.** Validate in-JVM: real Spark `local[*]`
  (provided deps are on the test classpath) + `embedded-kafka` broker + `memory` sink as a
  Doris stand-in.
- **Everything is Scala 2.12.** Do not pull `spring-kafka-test` (drags in Scala 2.13).
- Jackson pinned to **2.15.2** (Spark 3.5's version). embedded-kafka pinned to **3.4.1**
  (matches Spark's bundled kafka-clients 3.4.1).
- Shade plugin **must** keep `ServicesResourceTransformer` (DataSource registration) and
  filter `META-INF/*.SF|*.DSA|*.RSA` + `module-info.class`.

## Workflow

Develop **one Task at a time** (Task 1→6), confirm with the user between tasks. Current
status and the Task list live in ONBOARDING.md §7 and docs/DESIGN.md.
