#!/bin/bash
#
# Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
#


SCRIPT_DIR=$(dirname "$0")
PROJECT_DIR="$SCRIPT_DIR/.."

UBER_JAR=$(find ${PROJECT_DIR}/target/chronicle-queue-*-all.jar | tail -n1)

if [[ "$?" != "0" ]]; then
    echo "Could not find uber-jar, please run 'mvn package' in the project root"
    exit 1
fi

java -cp "$UBER_JAR" net.openhft.chronicle.queue.ChronicleReaderMain "$@"

# if running this in CQ source dir, beware of the default system.properties which is for unit testing,
# and enables resource tracing. This will lead to this tool dying after a while with OOME
