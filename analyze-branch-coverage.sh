#!/bin/bash
# analyze-branch-coverage.sh
# Analyzes incremental branch coverage contribution of each test class
# Processes tests from oldest to newest based on modification date
# Batches tests with same timestamp (by minute) to reduce Maven invocations
#
# Usage: ./analyze-branch-coverage.sh [-v] [-l N] [--solo-only]
#   -v, --verbose    Show detailed output
#   -l, --limit N    Only analyze first N tests
#   --solo-only      Skip cumulative runs (much faster, no incremental calc)

set -e

MODULE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$MODULE_DIR"

OUTPUT_DIR="$MODULE_DIR/target/coverage-analysis"
REPORT_FILE="$OUTPUT_DIR/branch-coverage-report.csv"
PROGRESS_LOG="$OUTPUT_DIR/progress.log"
SUMMARY_FILE="$OUTPUT_DIR/summary.txt"

# Parse command line args
VERBOSE=false
LIMIT=""
CUSTOM_LOG=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose) VERBOSE=true; shift ;;
        -l|--limit) LIMIT="$2"; shift 2 ;;
        -o|--output) CUSTOM_LOG="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [-v] [-l N] [-o logfile]"
            echo "  -v, --verbose    Show detailed output"
            echo "  -l, --limit N    Only analyze first N tests"
            echo "  -o, --output F   Output CSV report to file F"
            echo ""
            echo "Tracks incremental branch coverage - shows new branches each batch adds."
            echo "Results are saved incrementally - safe to stop anytime."
            echo "Batches tests with same modification minute for efficiency."
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log() {
    if [ "$VERBOSE" = true ]; then
        echo "    $@"
    fi
}

timer_start() {
    TIMER_START=$(date +%s)
}

timer_elapsed() {
    local now=$(date +%s)
    echo $((now - TIMER_START))
}

# Log to both stdout and progress file
log_progress() {
    echo "$@" | tee -a "$PROGRESS_LOG"
}

mkdir -p "$OUTPUT_DIR"

# Apply custom output file if specified
if [ -n "$CUSTOM_LOG" ]; then
    REPORT_FILE="$CUSTOM_LOG"
    PROGRESS_LOG="${CUSTOM_LOG%.csv}.log"
    SUMMARY_FILE="${CUSTOM_LOG%.csv}-summary.txt"
fi

# Initialize progress log
echo "=== Branch Coverage Analysis Started: $(date) ===" > "$PROGRESS_LOG"

echo "=== Test Branch Coverage Analysis ==="
echo "Module: $(basename "$MODULE_DIR")"
echo "Tracking incremental branch coverage (new branches per batch)"
echo "Results saved incrementally to: $REPORT_FILE"
echo ""

# Step 1: Get test files ordered by modification date (oldest first)
echo "Step 1: Finding test files by modification date..."
timer_start

TEST_SRC_DIR="src/test/java"
TESTS_BY_DATE_FILE="$OUTPUT_DIR/tests-by-date.txt"
BATCHED_TESTS_FILE="$OUTPUT_DIR/tests-batched.txt"

# Use filesystem modification time - fast and simple
# Format: timestamp filepath
find "$TEST_SRC_DIR" -name "*Test.java" -type f ! -path "*/resources/*" -printf "%T@ %p\n" 2>/dev/null | \
    sort -n > "$TESTS_BY_DATE_FILE"

# Fallback for systems without GNU find -printf
if [ ! -s "$TESTS_BY_DATE_FILE" ]; then
    for f in $(find "$TEST_SRC_DIR" -name "*Test.java" -type f 2>/dev/null | grep -v "/resources/"); do
        stat -c "%Y %n" "$f" 2>/dev/null || stat -f "%m %N" "$f" 2>/dev/null
    done | sort -n > "$TESTS_BY_DATE_FILE"
fi

TOTAL_TESTS=$(wc -l < "$TESTS_BY_DATE_FILE")
echo "  Found $TOTAL_TESTS test classes ($(timer_elapsed)s)"

if [ -n "$LIMIT" ]; then
    head -n "$LIMIT" "$TESTS_BY_DATE_FILE" > "$TESTS_BY_DATE_FILE.tmp"
    mv "$TESTS_BY_DATE_FILE.tmp" "$TESTS_BY_DATE_FILE"
    TOTAL_TESTS=$(wc -l < "$TESTS_BY_DATE_FILE")
    echo "  Limited to first $TOTAL_TESTS tests"
fi

# Group tests by minute timestamp for batching
echo "  Grouping tests by modification minute..."
> "$BATCHED_TESTS_FILE"
current_minute=""
current_batch=""
batch_count=0

while read -r line; do
    timestamp=$(echo "$line" | cut -d' ' -f1 | cut -d'.' -f1)
    test_file=$(echo "$line" | cut -d' ' -f2-)
    test_name=$(basename "$test_file" .java)

    # Round to minute
    minute=$((timestamp / 60))

    if [ "$minute" != "$current_minute" ]; then
        # Save previous batch if exists
        if [ -n "$current_batch" ]; then
            echo "$current_minute $current_batch" >> "$BATCHED_TESTS_FILE"
            batch_count=$((batch_count + 1))
        fi
        current_minute="$minute"
        current_batch="$test_name"
    else
        current_batch="$current_batch,$test_name"
    fi
done < "$TESTS_BY_DATE_FILE"

# Save last batch
if [ -n "$current_batch" ]; then
    echo "$current_minute $current_batch" >> "$BATCHED_TESTS_FILE"
    batch_count=$((batch_count + 1))
fi

echo "  Created $batch_count batches from $TOTAL_TESTS tests"
echo ""

# Use mvnd if available, otherwise fall back to mvn
MVN_CMD="mvn"
# Check standard path first
if command -v mvnd &> /dev/null; then
    MVN_CMD="mvnd"
# Check SDKMAN installation
elif [ -x "$HOME/.sdkman/candidates/mvnd/current/bin/mvnd" ]; then
    MVN_CMD="$HOME/.sdkman/candidates/mvnd/current/bin/mvnd"
# Check /opt installation
elif [ -x "/opt/mvnd/bin/mvnd" ]; then
    MVN_CMD="/opt/mvnd/bin/mvnd"
fi

if [ "$MVN_CMD" != "mvn" ]; then
    echo "Using Maven Daemon: $MVN_CMD"
fi

# Step 2: Compile
echo "Step 2: Compiling project..."
timer_start
$MVN_CMD -q -o compile test-compile -DskipTests -Dcheckstyle.skip=true -Dpmd.skip=true -Dspotbugs.skip=true 2>&1 | grep -v "^WARNING:" || true
echo "  Compiled ($(timer_elapsed)s)"
echo ""

# Step 3: Initialize report (with header)
echo "Step 3: Analyzing branch coverage per batch..."
echo "Batch,Tests,Modified,Batch Branches,Incremental,Cumulative,Test Count,Time(s)" > "$REPORT_FILE"
echo ""

# Function to extract covered branches from JaCoCo XML
get_covered_branches() {
    local xml_file="$1"
    if [ ! -f "$xml_file" ]; then
        echo "0"
        return
    fi
    local result=$(tr '<' '\n' < "$xml_file" | grep 'counter type="BRANCH"' | tail -1 | grep -oP 'covered="\K[0-9]+')
    echo "${result:-0}"
}

PREV_CUMULATIVE=0
CUMULATIVE_EXEC="$OUTPUT_DIR/cumulative.exec"
rm -f "$CUMULATIVE_EXEC"
BATCH_NUM=0
TOTAL_BATCHES=$batch_count
TEST_COUNT=0
TOTAL_START=$(date +%s)

while read -r line; do
    minute=$(echo "$line" | cut -d' ' -f1)
    test_list=$(echo "$line" | cut -d' ' -f2-)

    BATCH_NUM=$((BATCH_NUM + 1))
    timestamp=$((minute * 60))
    mod_date=$(date -d "@$timestamp" "+%Y-%m-%d %H:%M" 2>/dev/null || date -r "$timestamp" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "unknown")

    # Count tests in this batch
    batch_test_count=$(echo "$test_list" | tr ',' '\n' | wc -l)
    TEST_COUNT=$((TEST_COUNT + batch_test_count))

    # Truncate display if too long
    if [ ${#test_list} -gt 50 ]; then
        display_tests="${test_list:0:47}..."
    else
        display_tests="$test_list"
    fi

    printf "[%2d/%d] %-50s (%d tests) " "$BATCH_NUM" "$TOTAL_BATCHES" "$display_tests" "$batch_test_count"

    # --- Run batch ---
    BATCH_EXEC="$OUTPUT_DIR/batch-${BATCH_NUM}.exec"
    rm -f "$BATCH_EXEC" target/jacoco.exec

    timer_start
    echo -n "running..."

    if ! $MVN_CMD -o jacoco:prepare-agent surefire:test jacoco:report \
        -Dtest="$test_list" \
        -Djacoco.destFile="$BATCH_EXEC" \
        -Djacoco.dataFile="$BATCH_EXEC" \
        -Djacoco.propertyName=surefireArgLine \
        -Dcheckstyle.skip=true -Dpmd.skip=true -Dspotbugs.skip=true \
        -Denforcer.skip=true -Dlicense.skip=true \
        -DfailIfNoTests=false \
        > "$OUTPUT_DIR/mvn-batch-${BATCH_NUM}.log" 2>&1; then

        ELAPSED=$(timer_elapsed)
        printf "\r[%2d/%d] %-50s FAILED (%ds)\n" "$BATCH_NUM" "$TOTAL_BATCHES" "$display_tests" "$ELAPSED"
        log_progress "Batch $BATCH_NUM FAILED: $test_list"

        # Extract failure details for triage
        FAILURE_LOG="$OUTPUT_DIR/failures/batch-${BATCH_NUM}-failure.txt"
        mkdir -p "$OUTPUT_DIR/failures"
        {
            echo "=== Batch $BATCH_NUM Failure Details ==="
            echo "Tests: $test_list"
            echo "Time: $(date)"
            echo ""
            echo "--- Test Failures ---"
            grep -A 20 "<<< FAILURE!" "$OUTPUT_DIR/mvn-batch-${BATCH_NUM}.log" 2>/dev/null || true
            grep -A 20 "<<< ERROR!" "$OUTPUT_DIR/mvn-batch-${BATCH_NUM}.log" 2>/dev/null || true
            echo ""
            echo "--- Failed Tests Summary ---"
            grep -E "(Tests run:.*Failures: [1-9]|Tests run:.*Errors: [1-9])" "$OUTPUT_DIR/mvn-batch-${BATCH_NUM}.log" 2>/dev/null || true
            echo ""
            echo "--- Exception Stack Traces ---"
            grep -B 2 -A 15 "^\tat " "$OUTPUT_DIR/mvn-batch-${BATCH_NUM}.log" 2>/dev/null | head -100 || true
        } > "$FAILURE_LOG"

        # Log brief failure reason
        FAILURE_REASON=$(grep -oE "(AssertionError|NullPointerException|IllegalStateException|TimeoutException|OutOfMemoryError|Exception: .*)" "$OUTPUT_DIR/mvn-batch-${BATCH_NUM}.log" 2>/dev/null | head -1 || echo "Unknown")
        log_progress "  Failure reason: $FAILURE_REASON"
        log_progress "  Details: $FAILURE_LOG"

        if [ "$SOLO_ONLY" = true ]; then
            echo "$BATCH_NUM,\"$test_list\",$mod_date,FAILED,$batch_test_count,$ELAPSED" >> "$REPORT_FILE"
        else
            echo "$BATCH_NUM,\"$test_list\",$mod_date,FAILED,FAILED,FAILED,$batch_test_count,$ELAPSED" >> "$REPORT_FILE"
        fi
        continue
    fi

    ELAPSED=$(timer_elapsed)

    BATCH_XML="target/site/jacoco/jacoco.xml"
    BATCH_BRANCHES=$(get_covered_branches "$BATCH_XML")
    cp "$BATCH_XML" "$OUTPUT_DIR/batch-${BATCH_NUM}-jacoco.xml" 2>/dev/null || true

    # Merge batch exec into cumulative exec file
    if [ -f "$CUMULATIVE_EXEC" ]; then
        # Append batch exec to cumulative
        cat "$BATCH_EXEC" >> "$CUMULATIVE_EXEC"
    else
        cp "$BATCH_EXEC" "$CUMULATIVE_EXEC"
    fi

    # Generate cumulative report
    $MVN_CMD -q -o jacoco:report \
        -Djacoco.dataFile="$CUMULATIVE_EXEC" \
        -Dcheckstyle.skip=true -Dpmd.skip=true -Dspotbugs.skip=true \
        > "$OUTPUT_DIR/mvn-cumulative-${BATCH_NUM}.log" 2>&1 || true

    CUM_XML="target/site/jacoco/jacoco.xml"
    CUM_BRANCHES=$(get_covered_branches "$CUM_XML")
    INCREMENTAL=$((CUM_BRANCHES - PREV_CUMULATIVE))

    printf "\r[%2d/%d] %-50s batch=%4d  +%4d  cum=%4d  (%ds)\n" \
        "$BATCH_NUM" "$TOTAL_BATCHES" "$display_tests" "$BATCH_BRANCHES" "$INCREMENTAL" "$CUM_BRANCHES" "$ELAPSED"

    # Write to CSV immediately
    echo "$BATCH_NUM,\"$test_list\",$mod_date,$BATCH_BRANCHES,$INCREMENTAL,$CUM_BRANCHES,$batch_test_count,$ELAPSED" >> "$REPORT_FILE"

    # Log progress with full test list
    log_progress "Batch $BATCH_NUM: $batch_test_count tests, batch=$BATCH_BRANCHES, +$INCREMENTAL new, cumulative=$CUM_BRANCHES, ${ELAPSED}s"
    log_progress "  Tests: $test_list"

    PREV_CUMULATIVE=$CUM_BRANCHES

done < "$BATCHED_TESTS_FILE"

TOTAL_TIME=$(($(date +%s) - TOTAL_START))

# Step 4: Summary
echo ""
echo "=== Analysis Complete (${TOTAL_TIME}s total) ==="
log_progress "=== Completed: $(date), ${TOTAL_TIME}s total ==="
echo ""

cat > "$SUMMARY_FILE" << EOF
Test Branch Coverage Analysis
=============================
Generated: $(date)
Module: $(basename "$MODULE_DIR")
Tests Analyzed: $TEST_COUNT (in $TOTAL_BATCHES batches)
Total Time: ${TOTAL_TIME}s
Final Cumulative Branches: $PREV_CUMULATIVE
EOF

echo "" >> "$SUMMARY_FILE"
echo "Top 10 Batches by Incremental Branch Contribution:" >> "$SUMMARY_FILE"
tail -n +2 "$REPORT_FILE" | grep -v "FAILED" | sort -t',' -k5 -rn | head -10 | \
while IFS=',' read -r batch tests date batch_br incr cum count time; do
    tests=$(echo "$tests" | tr -d '"')
    if [ ${#tests} -gt 40 ]; then
        tests="${tests:0:37}..."
    fi
    printf "  %2d. %-42s +%4s new branches (%s tests)\n" "$batch" "$tests" "$incr" "$count"
done | tee -a "$SUMMARY_FILE"

echo "" >> "$SUMMARY_FILE"
echo "Batches Adding Zero New Branches:" >> "$SUMMARY_FILE"
ZERO_COUNT=$(tail -n +2 "$REPORT_FILE" | grep -v "FAILED" | awk -F',' '$5 == 0' | wc -l)
tail -n +2 "$REPORT_FILE" | grep -v "FAILED" | awk -F',' '$5 == 0' | \
while IFS=',' read -r batch tests date batch_br incr cum count time; do
    tests=$(echo "$tests" | tr -d '"')
    if [ ${#tests} -gt 50 ]; then
        tests="${tests:0:47}..."
    fi
    printf "  %2d. %-50s (batch=%s, %s tests)\n" "$batch" "$tests" "$batch_br" "$count"
done | tee -a "$SUMMARY_FILE"
echo "" | tee -a "$SUMMARY_FILE"
echo "Zero-contribution batches: $ZERO_COUNT / $TOTAL_BATCHES" | tee -a "$SUMMARY_FILE"

echo ""
echo "Reports saved to:"
echo "  CSV:      $REPORT_FILE"
echo "  Progress: $PROGRESS_LOG"
echo "  Summary:  $SUMMARY_FILE"
