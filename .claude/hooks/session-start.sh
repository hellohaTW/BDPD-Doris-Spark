#!/bin/bash
# SessionStart hook for Claude Code on the web.
#
# This project requires JDK 17 (Spark 3.5.1 needs Java 17 + a specific set of
# --add-opens flags). The remote container ships JDK 21 by default and Maven is
# already on PATH (/opt/maven), so the only setup needed is:
#   1. install JDK 17 if it isn't there yet, and
#   2. point JAVA_HOME / PATH at it and export Spark's Java-17 module flags
#      for the whole session (so `mvn` just works without `source dev-env.sh`).
set -euo pipefail

# Only run in the remote (Claude Code on the web) environment.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

JAVA17_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Idempotent: install JDK 17 only if it's missing.
if [ ! -x "$JAVA17_HOME/bin/javac" ]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y --no-install-recommends openjdk-17-jdk-headless
fi

# Persist JAVA_HOME + Spark's Java-17 module flags for every command this session.
# Mirrors dev-env.sh (Spark's full JavaModuleOptions set; Java 17 is stricter than 11).
if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  {
    echo "export JAVA_HOME=$JAVA17_HOME"
    echo "export PATH=$JAVA17_HOME/bin:\$PATH"
    echo 'export MAVEN_OPTS="-XX:+IgnoreUnrecognizedVMOptions \
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
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"'
  } >> "$CLAUDE_ENV_FILE"
fi
