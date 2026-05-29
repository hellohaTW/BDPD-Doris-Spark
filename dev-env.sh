#!/usr/bin/env bash
# Source this before running mvn:  source dev-env.sh
# macOS toolchain (Homebrew). On the original Linux/IntelliJ box, see ONBOARDING.md §2.
eval "$(/opt/homebrew/bin/brew shellenv)"
export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
# Spark on Java 11 needs these or it hits InaccessibleObjectException.
export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED"
echo "JAVA_HOME=$JAVA_HOME"
java -version
