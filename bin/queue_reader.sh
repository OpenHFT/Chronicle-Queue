#!/bin/bash
#
# Copyright 2016-2025 chronicle.software
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
