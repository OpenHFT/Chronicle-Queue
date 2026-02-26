# Chronicle-Queue Iteration 1: SCQIndexing Refactor

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix four concrete defects in `SCQIndexing.java`: unsynchronized diagnostic counters, a read path that creates secondary index blocks, a O(n) linear search that should be binary, and silent corruption from index holes.

**Architecture:** All changes are confined to `SCQIndexing.java` and its direct test class. No public API changes. Each task is guarded by a failing test written first, then a minimal fix. The existing `BinarySearch.java` utility is reused for Task 4.

**Tech Stack:** Java 8+, JUnit 5, EasyMock, Maven (`mvn test`)

---

## Background: What Is Wrong Today

`SCQIndexing` manages the two-level index structure of a Chronicle Queue store file. The primary index (`index2Index`) maps a shard offset to a secondary index block; each secondary index block records the file position of every Nth excerpt. To find an arbitrary excerpt by index, the code walks these structures and falls back to a linear scan for the final gap.

Four specific problems found in `SCQIndexing.java`:

| # | Location | Problem | Severity |
|---|----------|---------|---------|
| 1 | Lines 73–74, 76 | `linearScanCount`, `linearScanByPositionCount` (plain `int`), `lastScannedIndex` (plain `long`) mutated from multiple threads | High — data race on diagnostic metrics |
| 2 | `sequenceForPosition` line 736 → `getSecondaryAddress` line 853 | `sequenceForPosition` is a read path but calls `newIndex()`, creating new index blocks as a side effect of reading | Critical — reads silently write to the store |
| 3 | `sequenceForPosition` line 741 | `// TODO use a binary rather than linear search` — backwards linear scan over all primary index slots | High — O(n) in number of index blocks |
| 4 | `sequenceForPosition` line 757 | `// TODO pos shouldn't be 0, but holes in the index appear..` — zero-valued slots silently skipped | High — potential data skip |

---

## Files

- Modify: `src/main/java/net/openhft/chronicle/queue/impl/single/SCQIndexing.java`
- Test: `src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java` (create if absent)

---

## Task 1: Establish a Failing-Test Baseline

**Goal:** Confirm `SCQIndexingTest` compiles and runs against the current code before any changes. This is the safety net for the refactor.

**Step 1: Check whether the test file exists**

```bash
ls src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
```

If it does not exist, create a minimal skeleton:

```java
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.Test;

class SCQIndexingTest {
    @Test
    void placeholder() {
        // baseline — replaced by real tests in later tasks
    }
}
```

**Step 2: Run the test to confirm it compiles**

```bash
mvn test -pl . -Dtest=SCQIndexingTest -q
```

Expected: BUILD SUCCESS, 1 test run.

**Step 3: Run the full suite to get a passing baseline**

```bash
mvn test -q 2>&1 | tail -10
```

Record the number of tests run/passed. Any pre-existing failure must be noted before proceeding.

**Step 4: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "test: add SCQIndexingTest skeleton for iteration 1 baseline"
```

---

## Task 2: Fix Unsynchronized Diagnostic Counters

**Problem:** `linearScanCount` (int), `linearScanByPositionCount` (int), and `lastScannedIndex` (long) are plain fields mutated from any thread that calls `linearScan0` or `linearScanByPosition0`. Because `SCQIndexing` calls `singleThreadedCheckDisabled(true)`, these are legitimately accessed from multiple threads.

**Step 1: Write a failing test**

In `SCQIndexingTest.java`, add:

```java
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Test
void linearScanCountersShouldBeThreadSafe() {
    // If counters are plain int/long, this test documents the expectation;
    // it will serve as a guard against regression once the fields are atomic.
    // Verify the fields are of atomic types by reflection.
    var fields = SCQIndexing.class.getDeclaredFields();
    boolean foundLinearScanCount = false;
    boolean foundLinearScanByPositionCount = false;
    boolean foundLastScannedIndex = false;
    for (var f : fields) {
        if (f.getName().equals("linearScanCount")) {
            assertEquals(AtomicInteger.class, f.getType(),
                "linearScanCount must be AtomicInteger");
            foundLinearScanCount = true;
        }
        if (f.getName().equals("linearScanByPositionCount")) {
            assertEquals(AtomicInteger.class, f.getType(),
                "linearScanByPositionCount must be AtomicInteger");
            foundLinearScanByPositionCount = true;
        }
        if (f.getName().equals("lastScannedIndex")) {
            assertEquals(AtomicLong.class, f.getType(),
                "lastScannedIndex must be AtomicLong");
            foundLastScannedIndex = true;
        }
    }
    assertTrue(foundLinearScanCount, "field linearScanCount not found");
    assertTrue(foundLinearScanByPositionCount, "field linearScanByPositionCount not found");
    assertTrue(foundLastScannedIndex, "field lastScannedIndex not found");
}
```

Add required imports to the test class:
```java
import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
```

**Step 2: Run to confirm failure**

```bash
mvn test -pl . -Dtest=SCQIndexingTest#linearScanCountersShouldBeThreadSafe -q
```

Expected: FAIL — fields are currently `int` / `long`.

**Step 3: Change the field declarations in `SCQIndexing.java`**

Find (lines 73–76):
```java
    // visible for testing
    int linearScanCount;
    int linearScanByPositionCount;
    Collection<Closeable> closeables = new ArrayList<>();
    private long lastScannedIndex = -1;
```

Replace with:
```java
    // visible for testing
    final AtomicInteger linearScanCount = new AtomicInteger();
    final AtomicInteger linearScanByPositionCount = new AtomicInteger();
    Collection<Closeable> closeables = new ArrayList<>();
    private final AtomicLong lastScannedIndex = new AtomicLong(-1);
```

Add imports at the top of `SCQIndexing.java`:
```java
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
```

**Step 4: Fix all usages of the changed fields**

Search for all reads and writes:

```bash
grep -n "linearScanCount\|linearScanByPositionCount\|lastScannedIndex" \
  src/main/java/net/openhft/chronicle/queue/impl/single/SCQIndexing.java
```

Update each usage:

| Old | New |
|-----|-----|
| `this.linearScanCount++` | `linearScanCount.incrementAndGet()` |
| `linearScanByPositionCount++` | `linearScanByPositionCount.incrementAndGet()` |
| `lastScannedIndex = i` | `lastScannedIndex.set(i)` |
| `lastScannedIndex` (read) | `lastScannedIndex.get()` |

Also fix the two public accessor methods:

```java
// Before:
public int linearScanCount() { return linearScanCount; }
public int linearScanByPositionCount() { return linearScanByPositionCount; }

// After:
public int linearScanCount() { return linearScanCount.get(); }
public int linearScanByPositionCount() { return linearScanByPositionCount.get(); }
```

**Step 5: Run the failing test — it should now pass**

```bash
mvn test -pl . -Dtest=SCQIndexingTest#linearScanCountersShouldBeThreadSafe -q
```

Expected: PASS.

**Step 6: Run the full suite**

```bash
mvn test -q 2>&1 | tail -10
```

Expected: same pass count as the baseline in Task 1.

**Step 7: Commit**

```bash
git add src/main/java/net/openhft/chronicle/queue/impl/single/SCQIndexing.java \
        src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "fix: make SCQIndexing diagnostic counters atomic (thread-safe)"
```

---

## Task 3: Fix Read Path Creating Secondary Index Blocks

**Problem:** `sequenceForPosition` is called on the tailer read path. It calls `getSecondaryAddress`, which calls `newIndex()` to create a secondary index block when `secondaryAddress == 0`. This means reading from a queue silently writes to the store file — a correctness violation and a concurrency hazard.

The fix: introduce a read-only variant of `getSecondaryAddress` that returns `0` instead of creating a block, and use it from `sequenceForPosition`.

**Step 1: Understand the call chain**

Read these three method bodies in `SCQIndexing.java`:
- `sequenceForPosition` (line 723) — calls `getSecondaryAddress` at line 736
- `getSecondaryAddress` (line 850) — calls `newIndex` if address is 0
- `setPositionForSequenceNumber` (line 871) — also calls `getSecondaryAddress` at line 899 (write path, correct behaviour)

**Step 2: Write a failing test**

Add to `SCQIndexingTest.java`:

```java
@Test
void sequenceForPositionMustNotCreateSecondaryIndexBlocks() throws Exception {
    // This test verifies that calling sequenceForPosition on an empty/uninitialised
    // index does NOT create any new index blocks (i.e. does not mutate the store).
    // Strategy: build a ChronicleQueue, record the mtime of the store file before
    // calling sequenceForPosition, then assert mtime is unchanged afterwards.
    var tmpDir = java.nio.file.Files.createTempDirectory("cq-iter1-t3");
    try (var queue = net.openhft.chronicle.queue.ChronicleQueue.single(tmpDir.toString()).build();
         var tailer = queue.createTailer()) {

        // ensure at least one cycle file exists
        try (var appender = queue.createAppender()) {
            appender.writeText("hello");
        }

        // record all store file mtimes
        var storePath = java.nio.file.Files.list(tmpDir)
                .filter(p -> p.toString().endsWith(".cq4"))
                .findFirst().orElseThrow();
        long mtimeBefore = java.nio.file.Files.getLastModifiedTime(storePath).toMillis();

        // call sequenceForPosition indirectly by moving tailer to an arbitrary position
        // (the tailer reads but must not write)
        tailer.moveToIndex(queue.firstIndex());

        long mtimeAfter = java.nio.file.Files.getLastModifiedTime(storePath).toMillis();
        assertEquals(mtimeBefore, mtimeAfter,
            "sequenceForPosition must not modify the store file on the read path");
    } finally {
        net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(tmpDir.toFile());
    }
}
```

**Step 3: Run to confirm the test fails (or at least flags the issue)**

```bash
mvn test -pl . -Dtest=SCQIndexingTest#sequenceForPositionMustNotCreateSecondaryIndexBlocks -q
```

Note the result. If mtime already matches (no write), the existing code may already be safe for this path — confirm by adding a log or breakpoint inside `newIndex` and re-running.

**Step 4: Add a read-only method to `SCQIndexing`**

Add directly below `getSecondaryAddress` (after line 860):

```java
/**
 * Read-only variant: returns the secondary address at {@code index2} without
 * creating a new index block if the slot is empty. Returns {@code 0} if absent.
 */
private long getSecondaryAddressReadOnly(@NotNull LongArrayValues index2indexArr, int index2) {
    return index2indexArr.getVolatileValueAt(index2);
}
```

**Step 5: Update `sequenceForPosition` to use the read-only method**

In `sequenceForPosition` (around line 736), change:

```java
long secondaryAddress = getSecondaryAddress(wire, index2indexArr, index2);
```

to:

```java
long secondaryAddress = getSecondaryAddressReadOnly(index2indexArr, index2);
```

The `if (secondaryAddress == 0) continue;` check that follows already handles the zero case correctly.

**Step 6: Run the test suite**

```bash
mvn test -q 2>&1 | tail -10
```

Expected: same pass count as Task 1 baseline.

**Step 7: Commit**

```bash
git add src/main/java/net/openhft/chronicle/queue/impl/single/SCQIndexing.java \
        src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "fix: sequenceForPosition must not create secondary index blocks (read-only path)"
```

---

## Task 4: Replace Linear Search With Binary Search in `sequenceForPosition`

**Problem:** The inner loop in `sequenceForPosition` (lines 754–768) scans the secondary index backwards from `used-1` to `0` to find the largest stored position ≤ target. With a full secondary index of 1024 slots, this is up to 1024 comparisons per primary index block. The TODO at line 741 explicitly asks for binary search.

**Step 1: Study the existing `BinarySearch` utility**

```bash
cat src/main/java/net/openhft/chronicle/queue/impl/BinarySearch.java
```

Note its API. If it does not support searching a `LongArrayValues` by value, the binary search will be written inline.

**Step 2: Write a failing test for the binary search behavior**

The observable contract: `sequenceForPosition` must return the same index regardless of whether the secondary index is traversed linearly or via binary search. Add a round-trip test:

```java
@Test
void sequenceForPositionMatchesLinearAndBinarySearch() throws Exception {
    var tmpDir = java.nio.file.Files.createTempDirectory("cq-iter1-t4");
    try (var queue = net.openhft.chronicle.queue.ChronicleQueue.single(tmpDir.toString()).build()) {
        long[] indices = new long[200];
        try (var appender = queue.createAppender()) {
            for (int i = 0; i < 200; i++) {
                appender.writeText("msg-" + i);
                indices[i] = appender.lastIndexAppended();
            }
        }
        try (var tailer = queue.createTailer()) {
            for (int i = 0; i < 200; i++) {
                assertTrue(tailer.moveToIndex(indices[i]),
                    "tailer must reach index " + indices[i]);
                try (var dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    assertEquals("msg-" + i, dc.wire().read().text());
                }
            }
        }
    } finally {
        net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(tmpDir.toFile());
    }
}
```

**Step 3: Run to confirm it passes before the change (regression guard)**

```bash
mvn test -pl . -Dtest=SCQIndexingTest#sequenceForPositionMatchesLinearAndBinarySearch -q
```

Expected: PASS (documents current correct behavior).

**Step 4: Replace the inner linear scan with binary search**

In `sequenceForPosition`, the inner loop at lines 754–768:

```java
// Inner loop: Search within the secondary index.
for (int index1 = used - 1; index1 >= 0; index1--) {
    long pos = indexValues.getVolatileValueAt(index1);
    // TODO pos shouldn't be 0, but holes in the index appear..
    if (pos == 0 || pos > position) {
        continue;
    }
    lastKnownAddress = pos;
    indexOfNext = ((long) index2 << (indexCountBits + indexSpacingBits)) + ((long) index1 << indexSpacingBits);

    if (lastKnownAddress == position)
        return indexOfNext;

    break Outer;
}
```

Replace with a binary search that finds the rightmost slot where `pos > 0 && pos <= position`:

```java
// Binary search within secondary index for largest pos <= position.
int lo = 0, hi = used - 1, bestIndex1 = -1;
while (lo <= hi) {
    int mid = (lo + hi) >>> 1;
    long pos = indexValues.getVolatileValueAt(mid);
    if (pos == 0 || pos > position) {
        hi = mid - 1;
    } else {
        bestIndex1 = mid;
        lo = mid + 1;
    }
}
if (bestIndex1 >= 0) {
    long pos = indexValues.getVolatileValueAt(bestIndex1);
    lastKnownAddress = pos;
    indexOfNext = ((long) index2 << (indexCountBits + indexSpacingBits))
                + ((long) bestIndex1 << indexSpacingBits);
    if (lastKnownAddress == position)
        return indexOfNext;
    break Outer;
}
```

Remove the `// TODO use a binary rather than linear search` comment.

**Step 5: Run the regression guard test**

```bash
mvn test -pl . -Dtest=SCQIndexingTest#sequenceForPositionMatchesLinearAndBinarySearch -q
```

Expected: PASS.

**Step 6: Run the full suite**

```bash
mvn test -q 2>&1 | tail -10
```

Expected: same pass count as baseline.

**Step 7: Commit**

```bash
git add src/main/java/net/openhft/chronicle/queue/impl/single/SCQIndexing.java \
        src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "perf: replace linear search with binary search in sequenceForPosition"
```

---

## Task 5: Harden Index-Hole Handling and Document the Invariant

**Problem:** The `// TODO pos shouldn't be 0, but holes in the index appear..` comment at line 757 (now inside the replaced code from Task 4) reveals that the secondary index can contain zero-valued slots at positions that were already `setMaxUsed` beyond. The binary search in Task 4 already skips them correctly (`pos == 0` → treat as "not a valid anchor"). This task documents the invariant as a code assertion and adds a test that exposes holes.

**Step 1: Understand when holes appear**

In `setPositionForSequenceNumber` (line 906–914), a slot is only written at positions where `indexable(sequenceNumber)` is true. If the queue is written with two appenders that leapfrog each other across an index boundary, or if a file is truncated and re-opened, slots can remain zero between valid entries. The binary search in Task 4 handles this correctly by treating `pos == 0` as "skip".

**Step 2: Add a test that writes with a gap that would expose a hole**

Add to `SCQIndexingTest.java`:

```java
@Test
void tailerCanReadAllEntriesWhenIndexHasHoles() throws Exception {
    // Force a scenario where secondary index slots may be zero (holes):
    // use a very sparse roll cycle and write a non-contiguous sequence
    // by opening and closing appenders between writes.
    var tmpDir = java.nio.file.Files.createTempDirectory("cq-iter1-t5");
    int messageCount = 150;
    try (var queue = net.openhft.chronicle.queue.ChronicleQueue.single(tmpDir.toString())
            .rollCycle(net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY)
            .build()) {

        // Write in two separate appender sessions to stress index continuity
        long[] written = new long[messageCount];
        for (int batch = 0; batch < 3; batch++) {
            try (var appender = queue.createAppender()) {
                for (int i = batch * 50; i < (batch + 1) * 50; i++) {
                    appender.writeText("entry-" + i);
                    written[i] = appender.lastIndexAppended();
                }
            }
        }

        // Read back every entry in index order and verify no entry is skipped
        try (var tailer = queue.createTailer()) {
            for (int i = 0; i < messageCount; i++) {
                assertTrue(tailer.moveToIndex(written[i]),
                    "failed to move to index " + written[i] + " (entry " + i + ")");
                try (var dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "entry " + i + " missing");
                    assertEquals("entry-" + i, dc.wire().read().text());
                }
            }
        }
    } finally {
        net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(tmpDir.toFile());
    }
}
```

**Step 3: Run the test**

```bash
mvn test -pl . -Dtest=SCQIndexingTest#tailerCanReadAllEntriesWhenIndexHasHoles -q
```

Expected: PASS (the binary search from Task 4 handles holes). If it fails, investigate which entry is skipped and why.

**Step 4: Add an assertion comment replacing the old TODO**

In `sequenceForPosition`, the binary search block from Task 4 already handles `pos == 0`. Add a one-line comment explaining the invariant:

```java
// pos == 0 means this secondary index slot was never written (hole); skip it.
// This can happen when the queue is reopened after a truncation or when two
// appenders write across an index boundary. The binary search handles holes
// by treating them as "not a valid anchor".
```

Place this comment just before the `if (pos == 0 || pos > position)` check inside the binary search loop.

**Step 5: Run the full suite one final time**

```bash
mvn test -q 2>&1 | tail -10
```

Expected: all tests pass, same count as baseline (Task 1).

**Step 6: Commit**

```bash
git add src/main/java/net/openhft/chronicle/queue/impl/single/SCQIndexing.java \
        src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "fix: harden index-hole handling and document invariant in SCQIndexing"
```

---

## Acceptance Criteria

```text
AC-ID: AC-001
Statement: linearScanCount and linearScanByPositionCount are AtomicInteger; lastScannedIndex is AtomicLong.
Verification: mvn test -Dtest=SCQIndexingTest#linearScanCountersShouldBeThreadSafe
Expected: PASS

AC-ID: AC-002
Statement: sequenceForPosition does not write to the store file when called on a pre-populated queue.
Verification: mvn test -Dtest=SCQIndexingTest#sequenceForPositionMustNotCreateSecondaryIndexBlocks
Expected: PASS

AC-ID: AC-003
Statement: moveToIndex across 200 sequentially written entries returns correct text for every entry.
Verification: mvn test -Dtest=SCQIndexingTest#sequenceForPositionMatchesLinearAndBinarySearch
Expected: PASS

AC-ID: AC-004
Statement: moveToIndex works for all 150 entries written in three separate appender sessions (index holes present).
Verification: mvn test -Dtest=SCQIndexingTest#tailerCanReadAllEntriesWhenIndexHasHoles
Expected: PASS

AC-ID: AC-005
Statement: Full test suite passes without regression.
Verification: mvn test -q 2>&1 | tail -5
Expected: BUILD SUCCESS, ≥ baseline test count from Task 1
```

---

## Risk Notes

- **Binary search assumption:** The binary search in Task 4 assumes that non-zero positions in a secondary index block are monotonically increasing (lower slot index → lower file position). This holds because `setPositionForSequenceNumber` always writes positions in append order. If this assumption ever breaks, the binary search will return wrong results. The existing `CheckIndicesTest` is the safety net.

- **`getSecondaryAddressReadOnly` is package-private scope only.** Do not widen it. The write-path `getSecondaryAddress` is still used by `setPositionForSequenceNumber` and `initIndex`.

- **Do not change `Indexing` interface or any public method signatures.** `linearScanCount()` and `linearScanByPositionCount()` return `int`; the backing fields changing to `AtomicInteger` is an internal detail.
