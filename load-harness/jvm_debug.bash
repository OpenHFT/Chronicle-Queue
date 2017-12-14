#!/bin/bash

JSTACK_FILE="/tmp/jstacks.txt"
PERF_MAP_SCRIPT="$HOME/code/perf-map-agent/bin/create-java-perf-map.sh"

rm $JSTACK_FILE

for i in $(jps | grep Main | awk '{print $1}'); do jstack $i >> $JSTACK_FILE; done

echo "Recorded jstack output"

for i in $(jps | grep Main | awk '{print $1}'); do bash $PERF_MAP_SCRIPT $i >> $JSTACK_FILE; done

echo "Created stack maps"