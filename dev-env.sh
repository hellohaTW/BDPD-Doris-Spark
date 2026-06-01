#!/usr/bin/env bash
# Source this before running mvn:  source dev-env.sh
#
# Cross-platform (Linux + macOS, Intel or Apple Silicon). It:
#   1. sets MAVEN_OPTS so Spark works on Java 17 (avoids InaccessibleObjectException),
#   2. auto-detects a JDK 17 and exports JAVA_HOME / prepends it to PATH.
#
# It does NOT install anything — see SETUP.md Step 1 for installing JDK 17 + Maven.
# Override detection by exporting a valid JAVA_HOME before sourcing.

# 1. Spark on Java 17 needs these add-opens or it hits InaccessibleObjectException.
#    This mirrors Spark's own JavaModuleOptions set (java 17 is stricter than 11).
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

# Locate the brew binary even when it isn't on PATH yet (common in non-login shells).
_DEV_BREW="$(command -v brew 2>/dev/null || true)"
[ -z "$_DEV_BREW" ] && [ -x /opt/homebrew/bin/brew ] && _DEV_BREW=/opt/homebrew/bin/brew   # macOS Apple Silicon
[ -z "$_DEV_BREW" ] && [ -x /usr/local/bin/brew ] && _DEV_BREW=/usr/local/bin/brew         # macOS Intel
# Put brew (and thus mvn) on PATH if we found it.
[ -n "$_DEV_BREW" ] && eval "$("$_DEV_BREW" shellenv)"

# Returns 0 if $1 looks like a JDK 17 home.
_dev_is_java17() {
  [ -x "$1/bin/java" ] && "$1/bin/java" -version 2>&1 | grep -q 'version "17'
}

# Echoes the first JDK 17 home it can find (or nothing).
_dev_find_java17() {
  # a) Respect a pre-set, valid JAVA_HOME.
  if [ -n "$JAVA_HOME" ] && _dev_is_java17 "$JAVA_HOME"; then echo "$JAVA_HOME"; return; fi
  # b) macOS helper.
  if [ -x /usr/libexec/java_home ]; then
    _dev_mac="$(/usr/libexec/java_home -v 17 2>/dev/null)"
    if [ -n "$_dev_mac" ] && _dev_is_java17 "$_dev_mac"; then echo "$_dev_mac"; return; fi
  fi
  # c) Homebrew openjdk@17 (macOS Intel or Apple Silicon).
  if [ -n "$_DEV_BREW" ]; then
    _dev_brew="$("$_DEV_BREW" --prefix openjdk@17 2>/dev/null)"
    if [ -n "$_dev_brew" ] && _dev_is_java17 "$_dev_brew"; then echo "$_dev_brew"; return; fi
  fi
  # d) Derive from javac on PATH (covers Linux apt/alternatives, sdkman current, etc.).
  if command -v javac >/dev/null 2>&1; then
    _dev_jc="$(command -v javac)"
    if command -v readlink >/dev/null 2>&1; then
      _dev_real="$(readlink -f "$_dev_jc" 2>/dev/null)"
      [ -n "$_dev_real" ] && _dev_jc="$_dev_real"
    fi
    _dev_home="${_dev_jc%/bin/javac}"
    if [ -n "$_dev_home" ] && _dev_is_java17 "$_dev_home"; then echo "$_dev_home"; return; fi
  fi
  # e) Last resort: search common Linux JVM dirs (find does the globbing — safe under zsh).
  _dev_hit="$(find /usr/lib/jvm "$HOME/.sdkman/candidates/java" -maxdepth 2 -name '*17*' -type d 2>/dev/null | head -1)"
  if [ -n "$_dev_hit" ] && _dev_is_java17 "$_dev_hit"; then echo "$_dev_hit"; return; fi
}

_dev_jh="$(_dev_find_java17)"
if [ -n "$_dev_jh" ]; then
  export JAVA_HOME="$_dev_jh"
  case ":$PATH:" in
    *":$JAVA_HOME/bin:"*) ;;
    *) export PATH="$JAVA_HOME/bin:$PATH" ;;
  esac
  echo "JAVA_HOME=$JAVA_HOME"
  java -version
else
  echo "dev-env.sh: WARNING — no JDK 17 found. Install it (SETUP.md Step 1) or export JAVA_HOME." >&2
fi

unset -f _dev_is_java17 _dev_find_java17 2>/dev/null
unset _DEV_BREW _dev_jh _dev_mac _dev_brew _dev_jc _dev_real _dev_home _dev_hit 2>/dev/null
