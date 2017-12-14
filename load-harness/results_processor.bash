#!/bin/bash

VMSTAT_LOG="vmstat.log"
RESULTS_TXT="results.txt"
VMSTAT_DAT="vmstat.dat"

function require_file() {
    local FILENAME="$1"
    if [[ ! -f $FILENAME ]]; then
        echo "expected $FILENAME"
        exit 1
    fi
}

require_file $VMSTAT_LOG
require_file $RESULTS_TXT

cat $VMSTAT_LOG | grep -v "swpd" | grep -v "swap" | awk '{print $19" "$4" "$6" "$9" "$10" "$2}'> $VMSTAT_DAT
