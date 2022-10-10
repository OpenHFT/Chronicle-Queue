#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
PROJECT_DIR="$SCRIPT_DIR/.."

UBER_JAR=$(find ${PROJECT_DIR}/target/chronicle-queue-*-all.jar | tail -n1)

if [[ "$?" != "0" ]]; then
    echo "Could not find uber-jar, please run 'mvn package' in the project root"
    exit 1
fi

java -cp "$UBER_JAR" net.openhft.chronicle.queue.ChronicleHistoryReaderMain "$@"
