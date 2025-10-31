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
TOOL_ARCHIVE=/tmp/chronicle-queue-tools.tar

UBER_JAR=$(find ${PROJECT_DIR}/target/chronicle-queue-*-all.jar)

if [[ "$?" != "0" ]]; then
    echo "Could not find uber-jar, please run 'mvn package' in the project root"
    exit 1
fi

TMP_DIR=$(mktemp -d)

JAR_FILE=$(echo ${UBER_JAR} | awk -F/ '{print $NF}')

cp ${UBER_JAR} ${TMP_DIR}/

echo "java -cp $JAR_FILE \\" > ${TMP_DIR}/dump_queue.sh
echo 'net.openhft.chronicle.queue.DumpQueueMain "$1"' >> ${TMP_DIR}/dump_queue.sh
chmod +x ${TMP_DIR}/dump_queue.sh

echo "java -cp $JAR_FILE \\" > ${TMP_DIR}/queue_reader.sh
echo 'net.openhft.chronicle.queue.ChronicleReaderMain "$@"' >> ${TMP_DIR}/queue_reader.sh
chmod +x ${TMP_DIR}/queue_reader.sh

echo "java -cp $JAR_FILE \\" > ${TMP_DIR}/history_reader.sh
echo 'net.openhft.chronicle.queue.ChronicleHistoryReaderMain "$@"' >> ${TMP_DIR}/history_reader.sh
chmod +x ${TMP_DIR}/queue_reader.sh

cd ${TMP_DIR}
tar cf ${TOOL_ARCHIVE} *.*
cd ${OLD_PWD}

echo "Created $TOOL_ARCHIVE"
