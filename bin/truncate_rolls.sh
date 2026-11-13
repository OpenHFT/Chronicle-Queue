#!/bin/bash
#
# Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
#

set -euo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
shopt -s nullglob
jars=("$SCRIPT_DIR"/../target/chronicle-queue-*-all.jar)
if ((  $# < 1 || ${#jars[@]} != 1 )); then
    echo "Usage: $0 <root-directory> [--offline] [--min-age-seconds=N] (requires one packaged *-all.jar)" >&2
    exit 2
fi
# Keep the launcher usable on Java 8 and supply the Queue access flags on JPMS JVMs.
QUEUE_JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
QUEUE_JAVA_VERSION=$("$QUEUE_JAVA_BIN" -version 2>&1)
if [[ $QUEUE_JAVA_VERSION =~ version[[:space:]]+\"([0-9]+)(\.([0-9]+))? ]]; then
    QUEUE_JAVA_MAJOR=${BASH_REMATCH[1]}
    if [[ $QUEUE_JAVA_MAJOR == 1 ]]; then
        QUEUE_JAVA_MAJOR=${BASH_REMATCH[3]}
    fi
else
    echo "Cannot determine Java version; set JAVA_HOME to a supported JDK" >&2
    exit 2
fi
QUEUE_JVM_ARGS=()
if (( QUEUE_JAVA_MAJOR >= 9 )); then
    QUEUE_JVM_ARGS+=(
        --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
        --add-opens=java.base/java.lang=ALL-UNNAMED
        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
        --add-opens=java.base/java.io=ALL-UNNAMED
        --add-opens=java.base/java.nio=ALL-UNNAMED
        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    )
fi
# JAVA_TOOL_OPTIONS remains available for additional installation-specific settings.
exec "$QUEUE_JAVA_BIN" "${QUEUE_JVM_ARGS[@]}" -cp "${jars[0]}" net.openhft.chronicle.queue.main.TruncateRollFilesMain "$@"
