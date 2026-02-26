# Chronicle-Queue Iteration 2: SCQIndexing Hardening and Validation

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add regression guards, architectural rules, and stress tests to ensure the Iteration 1 SCQIndexing improvements (atomic counters, read-only index path, binary anchor selection with hole-aware fallback, and readability helper extraction) cannot silently regress. Carry forward the `TableStoreWriteLockTest` timeout fix and harden it against flakiness. Ensure companion changes in `SingleChronicleQueueStoreTest` are preserved.

**Architecture:** All changes are test-only — no production code modifications. Each task adds a specific guard that catches a different class of regression. ArchUnit rules enforce structural invariants at the bytecode level. Stress tests validate thread-safety under contention.

**Tech Stack:** Java 21, JUnit 5, ArchUnit 1.2.1 (already in test dependencies), Maven (`mvn test`)

---

## Iteration 1 Outcomes (Baseline)

Iteration 1 landed the following in `SCQIndexing.java`:

| # | Change | Lines | Detail |
|---|--------|-------|--------|
| 1 | Thread-safe diagnostic counters | 75-78 | `linearScanCount`, `linearScanByPositionCount` → `final AtomicInteger`; `lastScannedIndex` → `private final AtomicLong` |
| 2 | Read-only secondary address lookup | 738, 810-812 | Added `getSecondaryAddressReadOnly`; `sequenceForPosition` uses it instead of `getSecondaryAddress` (which calls `newIndex()` on miss) |
| 3 | Binary anchor selection with hole-aware fallback | 755-821 | Uses binary search for fast anchor lookup and falls back to reverse scan when hole slots are observed to preserve correctness. |
| 4 | Readability helper extraction | 782-826 | Anchor lookup and sequence-index composition moved into focused helpers (`findBestSecondarySlotForPosition`, `findBestSecondarySlotByReverseScan`, `toSequenceIndex`). |

Additionally:
- `TableStoreWriteLockTest` timeouts for 4 subprocess-spawning tests were increased from 5s to 15s (cold JVM startup takes ~5.5s): `unlockWillNotUnlockAndWarnIfLockedByAnotherProcess`, `forceUnlockWillUnlockAndWarnIfLockedByAnotherProcess`, `forceUnlockIfProcessIsDeadWillFailWhenLockingProcessIsAlive`, `forceUnlockIfProcessIsDeadWillSucceedWhenLockingProcessIsDead`.
- `SingleChronicleQueueStoreTest` was updated to call `.linearScanCount.get()` instead of direct field access (3 sites), required to compile against the atomic counter fields.

### Iteration 1 Review Findings

The review (opus, iteration 1) evaluated three independent implementations. Key divergences and decisions:

| Aspect | Codex (ranked #1) | Sonnet (ranked #2) | Dex (vetoed) |
|--------|-------------------|-------------------|--------------|
| Search algorithm | Binary search with `sawHole` fallback to reverse scan — O(log n) common case | Forward linear scan with early break — O(n), simpler but does not address the TODO | Byte-identical copy of codex |
| Helper extraction | `findBestSecondarySlotForPosition`, `findBestSecondarySlotByReverseScan`, `toSequenceIndex` | Inline replacement, no helper extraction | Same as codex |
| Variable naming | Renamed to `primarySlot`, `secondarySlot`, `usedPrimarySlots` | Kept original names | Same as codex |
| Hole test rigor | Deterministic hole injection via `setPositionForSequenceNumber(..., 0)` | Relies on batched writes creating holes naturally (less reliable) | Same as codex |
| Validation breadth | Targeted tests only (SCQIndexingTest, SingleChronicleQueueStoreTest, ToEndInvalidIndexTest) | Full suite (1117 tests, 0 failures, 4 pre-existing timeout errors) | Same as codex |

The iteration 2 regression contract must protect whichever implementation is merged. Tests should be implementation-agnostic (verify observable behavior, not internal structure) except where ArchUnit guards enforce specific structural properties.

Required Iteration 1 regression contract tests in `SCQIndexingTest.java` (must exist by end of Task 0):
- `linearScanCountersShouldBeThreadSafe` — reflection check of field types
- `sequenceForPositionMustNotCreateSecondaryIndexBlocks` — mtime unchanged after read
- `sequenceForPositionMatchesLinearAndBinarySearch` — 200-entry round-trip
- `tailerCanReadAllEntriesWhenIndexHasHoles` — 150 entries across 3 appender sessions
- `sequenceForPositionWithInteriorHoleStillFindsCorrectAnchor` — explicit interior-hole injection to guard binary-search fallback correctness

If `SCQIndexingTest.java` does not currently exist on this branch, it must be created and populated with the Iteration 1 contract tests before any Iteration 2-only tests are added.

---

## Files In Scope

- Create: `src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingArchTest.java`
- Create/Modify: `src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java`
- Verify/Preserve: `src/test/java/net/openhft/chronicle/queue/impl/single/SingleChronicleQueueStoreTest.java` (atomic accessor companion change)
- Verify/Preserve: `src/test/java/net/openhft/chronicle/queue/impl/single/TableStoreWriteLockTest.java` (timeout fix for flakiness)

Out of scope:
- Production code changes (`SCQIndexing.java` is not modified in this iteration)
- Wire format/schema changes
- Roll-cycle encoding changes

---

## Task 0: Re-anchor Iteration 1 Regression Contract

**Goal:** Ensure the Iteration 1 SCQIndexing protections are present on this branch before adding new hardening tests. This prevents silent loss of prior work when branching or cherry-picking. Also verify that companion changes in `SingleChronicleQueueStoreTest` and `TableStoreWriteLockTest` are present.

**Step 1: Verify baseline test file exists**

```bash
ls src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
```

If missing, create it and add the five Iteration 1 contract tests listed above.

**Step 2: Verify SingleChronicleQueueStoreTest companion change**

Check that `SingleChronicleQueueStoreTest.java` uses `.linearScanCount.get()` (not direct `.linearScanCount` field access):

```bash
grep -n "linearScanCount" src/test/java/net/openhft/chronicle/queue/impl/single/SingleChronicleQueueStoreTest.java
```

Expected: all 3 references use `.linearScanCount.get()`. If any use bare `.linearScanCount`, update them — the code will not compile against the atomic fields otherwise.

**Step 3: Verify TableStoreWriteLockTest timeout fix**

Check that the 4 subprocess-spawning tests use 15s timeouts (not 5s):

```bash
grep -n "timeout" src/test/java/net/openhft/chronicle/queue/impl/single/TableStoreWriteLockTest.java
```

Expected: `unlockWillNotUnlockAndWarnIfLockedByAnotherProcess`, `forceUnlockWillUnlockAndWarnIfLockedByAnotherProcess`, `forceUnlockIfProcessIsDeadWillFailWhenLockingProcessIsAlive`, `forceUnlockIfProcessIsDeadWillSucceedWhenLockingProcessIsDead` all show `timeout = 15_000`. If any show `5_000`, update to `15_000`.

**Step 4: Run baseline contract tests**

```bash
mvn test -Dtest=SCQIndexingTest#linearScanCountersShouldBeThreadSafe+sequenceForPositionMustNotCreateSecondaryIndexBlocks+sequenceForPositionMatchesLinearAndBinarySearch+tailerCanReadAllEntriesWhenIndexHasHoles+sequenceForPositionWithInteriorHoleStillFindsCorrectAnchor -q
```

Expected: PASS.

**Step 5: Run companion test classes to confirm they compile and pass**

```bash
mvn test -Dtest=SingleChronicleQueueStoreTest -q
mvn test -Dtest=TableStoreWriteLockTest -q
```

Expected: both PASS. `TableStoreWriteLockTest` must show 0 timeout errors (the 15s timeout eliminates the pre-existing flakiness).

**Step 6: Commit baseline backfill if any file was created/updated**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java \
        src/test/java/net/openhft/chronicle/queue/impl/single/SingleChronicleQueueStoreTest.java \
        src/test/java/net/openhft/chronicle/queue/impl/single/TableStoreWriteLockTest.java
git commit -m "test: restore iteration 1 SCQIndexing regression contract and companion fixes"
```

---

## Task 1: ArchUnit Guard — Atomic Counter Types Must Not Regress

**Goal:** An ArchUnit rule that fails the build if `linearScanCount`, `linearScanByPositionCount`, or `lastScannedIndex` are declared as plain `int` or `long` instead of their atomic types. This catches "simplification" refactors that silently reintroduce data races.

**Step 1: Create `SCQIndexingArchTest.java`**

```java
package net.openhft.chronicle.queue.impl.single;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

class SCQIndexingArchTest {

    private static final Map<String, Class<?>> REQUIRED_ATOMIC_FIELDS = Map.of(
            "linearScanCount", AtomicInteger.class,
            "linearScanByPositionCount", AtomicInteger.class,
            "lastScannedIndex", AtomicLong.class
    );

    @Test
    void diagnosticCountersMustBeAtomic() {
        var importedClasses = new ClassFileImporter().importClasses(SCQIndexing.class);

        classes().that().haveSimpleName("SCQIndexing")
                .should(new ArchCondition<>("have atomic diagnostic counters") {
                    @Override
                    public void check(JavaClass javaClass, ConditionEvents events) {
                        for (var entry : REQUIRED_ATOMIC_FIELDS.entrySet()) {
                            String fieldName = entry.getKey();
                            Class<?> expectedType = entry.getValue();
                            try {
                                JavaField field = javaClass.getField(fieldName);
                                if (!field.getRawType().isEquivalentTo(expectedType)) {
                                    events.add(SimpleConditionEvent.violated(field,
                                            fieldName + " must be " + expectedType.getSimpleName()
                                                    + " but was " + field.getRawType().getName()));
                                }
                            } catch (IllegalArgumentException e) {
                                events.add(SimpleConditionEvent.violated(javaClass,
                                        "field " + fieldName + " not found in SCQIndexing"));
                            }
                        }
                    }
                }).check(importedClasses);
    }
}
```

**Step 2: Run to confirm it passes**

```bash
mvn test -Dtest=SCQIndexingArchTest -q
```

Expected: PASS.

**Step 3: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingArchTest.java
git commit -m "test: add ArchUnit guard for SCQIndexing atomic counter types"
```

---

## Task 2: ArchUnit Guard — Read Path Must Not Call getSecondaryAddress

**Goal:** An ArchUnit rule that ensures `sequenceForPosition` does not call `getSecondaryAddress` (the write-capable variant that calls `newIndex()`). This prevents future refactors from accidentally re-introducing the write-on-read bug.

**Step 1: Add rule to `SCQIndexingArchTest.java`**

```java
@Test
void sequenceForPositionMustNotCallGetSecondaryAddress() {
    var importedClasses = new ClassFileImporter().importClasses(SCQIndexing.class);

    classes().that().haveSimpleName("SCQIndexing")
            .should(new ArchCondition<>("not call getSecondaryAddress from sequenceForPosition") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents events) {
                    javaClass.getMethods().stream()
                            .filter(m -> m.getName().equals("sequenceForPosition"))
                            .flatMap(m -> m.getMethodCallsFromSelf().stream())
                            .filter(call -> call.getTarget().getName().equals("getSecondaryAddress"))
                            .forEach(call -> events.add(SimpleConditionEvent.violated(call,
                                    "sequenceForPosition must use getSecondaryAddressReadOnly, " +
                                    "not getSecondaryAddress (write-on-read bug)")));
                }
            }).check(importedClasses);
}
```

**Step 2: Run to confirm it passes**

```bash
mvn test -Dtest=SCQIndexingArchTest -q
```

Expected: PASS (2 rules, both green).

**Step 3: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingArchTest.java
git commit -m "test: add ArchUnit guard preventing getSecondaryAddress in sequenceForPosition"
```

---

## Task 3: ArchUnit Guard — Helper Method Extraction Must Not Regress

**Goal:** An ArchUnit rule that verifies the extracted helper methods (`findBestSecondarySlotForPosition`, `findBestSecondarySlotByReverseScan`, `toSequenceIndex`) exist in `SCQIndexing`. This prevents future refactors from inlining the search logic back into `sequenceForPosition`, which was a major readability improvement from Iteration 1.

**Note:** If the merged implementation uses sonnet's approach (forward linear scan without extracted helpers), skip this task — the helpers will not exist. This guard is only applicable when codex's implementation is adopted.

**Step 1: Add rule to `SCQIndexingArchTest.java`**

```java
@Test
void searchHelperMethodsMustExist() {
    var importedClasses = new ClassFileImporter().importClasses(SCQIndexing.class);
    var requiredHelpers = java.util.List.of(
            "findBestSecondarySlotForPosition",
            "findBestSecondarySlotByReverseScan",
            "toSequenceIndex"
    );

    classes().that().haveSimpleName("SCQIndexing")
            .should(new ArchCondition<>("have extracted search helper methods") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents events) {
                    var methodNames = javaClass.getMethods().stream()
                            .map(m -> m.getName())
                            .collect(java.util.stream.Collectors.toSet());
                    for (String helper : requiredHelpers) {
                        if (!methodNames.contains(helper)) {
                            events.add(SimpleConditionEvent.violated(javaClass,
                                    "SCQIndexing must contain helper method " + helper));
                        }
                    }
                }
            }).check(importedClasses);
}
```

**Step 2: Run to confirm it passes**

```bash
mvn test -Dtest=SCQIndexingArchTest -q
```

Expected: PASS (3 rules, all green).

**Step 3: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingArchTest.java
git commit -m "test: add ArchUnit guard for SCQIndexing search helper method extraction"
```

---

## Task 4: Multi-Block Read-Only Path Regression Guard

**Goal:** The Iteration 1 read-only path test used a single entry (or a small number of entries in a default roll cycle). Extend coverage to a queue spanning multiple secondary index blocks (via `TEST4_DAILY` with indexSpacing=4), verifying no store file growth on read. This catches regressions where `getSecondaryAddress` is restored for only some code paths.

**Step 1: Add test to `SCQIndexingTest.java`**

```java
@Test
void sequenceForPositionMustNotCreateSecondaryIndexBlocksWithLargeQueue() throws Exception {
    java.nio.file.Path tmpDir = java.nio.file.Files.createTempDirectory("cq-iter2-t3");
    try (net.openhft.chronicle.queue.ChronicleQueue queue =
            net.openhft.chronicle.queue.ChronicleQueue.singleBuilder(tmpDir.toFile())
                .rollCycle(net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY)
                .build()) {

        // TEST4_DAILY: indexCount=4, indexSpacing=4 → 100 entries spans multiple secondary blocks
        long[] indices = new long[100];
        try (net.openhft.chronicle.queue.ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < 100; i++) {
                appender.writeText("readonly-" + i);
                indices[i] = appender.lastIndexAppended();
            }
        }

        // Record store file sizes (more reliable than mtime for detecting new blocks)
        java.util.Map<java.nio.file.Path, Long> sizesBefore = new java.util.HashMap<>();
        try (java.util.stream.Stream<java.nio.file.Path> listing = java.nio.file.Files.list(tmpDir)) {
            listing.filter(p -> p.toString().endsWith(".cq4"))
                   .forEach(p -> {
                       try { sizesBefore.put(p, java.nio.file.Files.size(p)); }
                       catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                   });
        }

        // Read every entry — must not grow the store file
        try (net.openhft.chronicle.queue.ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < 100; i++) {
                assertTrue(tailer.moveToIndex(indices[i]));
                try (net.openhft.chronicle.wire.DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    assertEquals("readonly-" + i, dc.wire().read().text());
                }
            }
        }

        // Verify store file sizes unchanged
        try (java.util.stream.Stream<java.nio.file.Path> listing = java.nio.file.Files.list(tmpDir)) {
            listing.filter(p -> p.toString().endsWith(".cq4"))
                   .forEach(p -> {
                       try {
                           long sizeBefore = sizesBefore.getOrDefault(p, -1L);
                           long sizeAfter = java.nio.file.Files.size(p);
                           assertEquals(sizeBefore, sizeAfter,
                               "store file " + p.getFileName() + " must not grow on read path");
                       } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                   });
        }
    } finally {
        net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(tmpDir.toFile());
    }
}
```

**Step 2: Run to confirm it passes**

```bash
mvn test -Dtest=SCQIndexingTest#sequenceForPositionMustNotCreateSecondaryIndexBlocksWithLargeQueue -q
```

Expected: PASS.

**Step 3: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "test: add multi-block read-only path regression guard"
```

---

## Task 5: Interior-Hole Anchor Regression Guard

**Goal:** Verify that `sequenceForPosition` remains correct when a secondary index contains an interior hole (`pos == 0` in the middle of used slots). This protects the binary-search path and the hole-aware reverse-scan fallback from future regressions.

**Step 1: Add test to `SCQIndexingTest.java`**

```java
@Test
void sequenceForPositionWithInteriorHoleStillFindsCorrectAnchor() throws Exception {
    java.nio.file.Path tmpDir = java.nio.file.Files.createTempDirectory("cq-iter2-t4");
    try (net.openhft.chronicle.queue.ChronicleQueue queue =
            net.openhft.chronicle.queue.ChronicleQueue.singleBuilder(tmpDir.toFile())
                .indexSpacing(1)
                .testBlockSize()
                .build()) {

        int messageCount = 200;
        long[] indices = new long[messageCount];
        try (net.openhft.chronicle.queue.ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < messageCount; i++) {
                appender.writeText("hole-" + i);
                indices[i] = appender.lastIndexAppended();
            }

            // Inject an interior hole in the secondary index.
            net.openhft.chronicle.queue.impl.single.StoreAppender storeAppender =
                    (net.openhft.chronicle.queue.impl.single.StoreAppender) appender;
            net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore store = storeAppender.store;
            net.openhft.chronicle.queue.impl.single.SingleChronicleQueue scq =
                    (net.openhft.chronicle.queue.impl.single.SingleChronicleQueue) queue;

            long holeSequence = scq.rollCycle().toSequenceNumber(indices[80]);
            store.indexing.setPositionForSequenceNumber(storeAppender, holeSequence, 0);
        }

        // Verify seeking before/around/after the hole still returns correct entries.
        try (net.openhft.chronicle.queue.ExcerptTailer tailer = queue.createTailer()) {
            for (int i : new int[]{60, 79, 80, 81, 120, 160, 199}) {
                org.junit.jupiter.api.Assertions.assertTrue(tailer.moveToIndex(indices[i]));
                try (net.openhft.chronicle.wire.DocumentContext dc = tailer.readingDocument()) {
                    org.junit.jupiter.api.Assertions.assertTrue(dc.isPresent());
                    org.junit.jupiter.api.Assertions.assertEquals("hole-" + i, dc.wire().read().text());
                }
            }
        }
    } finally {
        net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(tmpDir.toFile());
    }
}
```

**Step 2: Run to confirm it passes**

```bash
mvn test -Dtest=SCQIndexingTest#sequenceForPositionWithInteriorHoleStillFindsCorrectAnchor -q
```

Expected: PASS.

**Step 3: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "test: add interior-hole anchor regression guard for SCQIndexing"
```

---

## Task 6: Concurrent Tailer Stress Test

**Goal:** Verify that multiple tailers reading concurrently from the same queue do not corrupt shared state or throw exceptions. This validates the atomic counter changes under real contention.

**Step 1: Add test to `SCQIndexingTest.java`**

```java
@Test
void concurrentTailersDoNotCorruptOrThrow() throws Exception {
    java.nio.file.Path tmpDir = java.nio.file.Files.createTempDirectory("cq-iter2-t5");
    try (net.openhft.chronicle.queue.ChronicleQueue queue =
            net.openhft.chronicle.queue.ChronicleQueue.singleBuilder(tmpDir.toFile())
                .rollCycle(net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST2_DAILY)
                .build()) {

        // Write entries to ensure linear scans occur
        long firstIndex;
        try (net.openhft.chronicle.queue.ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < 500; i++) {
                appender.writeText("concurrent-" + i);
            }
            firstIndex = queue.firstIndex();
        }

        // Read from multiple threads simultaneously
        int threadCount = 4;
        int readsPerThread = 50;
        java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(threadCount);
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
        long idx = firstIndex;

        for (int t = 0; t < threadCount; t++) {
            final long seekTarget = idx;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    try (net.openhft.chronicle.queue.ExcerptTailer tailer = queue.createTailer()) {
                        for (int r = 0; r < readsPerThread; r++) {
                            assertTrue(tailer.moveToIndex(seekTarget));
                            try (net.openhft.chronicle.wire.DocumentContext dc = tailer.readingDocument()) {
                                assertTrue(dc.isPresent());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (java.util.concurrent.Future<?> f : futures) {
            f.get(15, java.util.concurrent.TimeUnit.SECONDS);
        }
        executor.shutdown();
    } finally {
        net.openhft.chronicle.core.io.IOTools.deleteDirWithFiles(tmpDir.toFile());
    }
}
```

**Step 2: Run to confirm it passes**

```bash
mvn test -Dtest=SCQIndexingTest#concurrentTailersDoNotCorruptOrThrow -q
```

Expected: PASS (no exceptions under concurrent access).

**Step 3: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "test: add concurrent tailer stress test for atomic counter validation"
```

---

## Task 7: TableStoreWriteLockTest Flakiness Hardening

**Goal:** Beyond the timeout increase from Iteration 1, add a diagnostic assertion that the subprocess actually started and acquired the lock before the test proceeds. The root cause of the flakiness is cold JVM startup latency — the 5s timeout was marginal. The 15s timeout fix is necessary but this task adds a guard that makes future flakiness debuggable rather than opaque.

**Step 1: Add a targeted test to verify subprocess liveness**

Add to `SCQIndexingTest.java` (or a new `TableStoreWriteLockFlakinessSmokeTest.java` if preferred):

```java
@Test
void tableStoreWriteLockSubprocessStartsWithin15Seconds() throws Exception {
    // Smoke test: verify that the subprocess JVM used by TableStoreWriteLockTest
    // starts and produces output within the 15s timeout window.
    // This catches CI environments where JVM startup exceeds expectations.
    ProcessBuilder pb = new ProcessBuilder("java", "-version");
    pb.redirectErrorStream(true);
    long start = System.nanoTime();
    Process p = pb.start();
    boolean exited = p.waitFor(15, java.util.concurrent.TimeUnit.SECONDS);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertTrue(exited, "java -version must complete within 15s (took " + elapsedMs + "ms)");
    assertTrue(elapsedMs < 10_000,
        "JVM startup took " + elapsedMs + "ms — TableStoreWriteLockTest timeouts may need increasing");
}
```

**Step 2: Run to confirm it passes**

```bash
mvn test -Dtest=SCQIndexingTest#tableStoreWriteLockSubprocessStartsWithin15Seconds -q
```

Expected: PASS. If `elapsedMs >= 10_000`, the 15s timeout may still be marginal on this CI environment and should be raised further.

**Step 3: Run TableStoreWriteLockTest to confirm 0 timeout errors**

```bash
mvn test -Dtest=TableStoreWriteLockTest -q 2>&1 | tail -5
```

Expected: all tests pass, 0 errors.

**Step 4: Commit**

```bash
git add src/test/java/net/openhft/chronicle/queue/impl/single/SCQIndexingTest.java
git commit -m "test: add subprocess startup smoke test for TableStoreWriteLockTest flakiness guard"
```

---

## Task 8: Full Gate and Review Output

**Goal:** Confirm no regressions from the new tests.

**Step 1: Run new tests in isolation**

```bash
mvn test -Dtest=SCQIndexingTest,SCQIndexingArchTest -q
```

Expected: all tests pass.

**Step 2: Run full suite**

```bash
mvn test 2>&1 | tail -10
```

Expected: BUILD SUCCESS, 0 failures, 0 errors.

**Step 3: Record evidence in GATES.jsonl**

**Step 4: Commit**

```bash
git add GATES.jsonl
git commit -m "chore: iteration 2 gate evidence"
```

---

## Acceptance Criteria

```text
AC-ID: AC-001
Statement: Iteration 1 regression contract tests exist and pass on this branch.
Verification: mvn test -Dtest=SCQIndexingTest#linearScanCountersShouldBeThreadSafe+sequenceForPositionMustNotCreateSecondaryIndexBlocks+sequenceForPositionMatchesLinearAndBinarySearch+tailerCanReadAllEntriesWhenIndexHasHoles+sequenceForPositionWithInteriorHoleStillFindsCorrectAnchor
Expected: PASS

AC-ID: AC-001b
Statement: SingleChronicleQueueStoreTest compiles and passes against atomic counter fields.
Verification: mvn test -Dtest=SingleChronicleQueueStoreTest -q
Expected: PASS

AC-ID: AC-001c
Statement: TableStoreWriteLockTest passes with 0 timeout errors (15s timeouts present).
Verification: mvn test -Dtest=TableStoreWriteLockTest -q
Expected: PASS, 0 errors

AC-ID: AC-002
Statement: ArchUnit rule fails if linearScanCount, linearScanByPositionCount, or lastScannedIndex are not atomic types.
Verification: mvn test -Dtest=SCQIndexingArchTest#diagnosticCountersMustBeAtomic
Expected: PASS

AC-ID: AC-003
Statement: ArchUnit rule fails if sequenceForPosition calls getSecondaryAddress instead of getSecondaryAddressReadOnly.
Verification: mvn test -Dtest=SCQIndexingArchTest#sequenceForPositionMustNotCallGetSecondaryAddress
Expected: PASS

AC-ID: AC-003b
Statement: ArchUnit rule verifies extracted helper methods exist (codex implementation only; skip if sonnet's approach is merged).
Verification: mvn test -Dtest=SCQIndexingArchTest#searchHelperMethodsMustExist
Expected: PASS (or SKIPPED if sonnet's implementation was merged)

AC-ID: AC-004
Statement: Reading a queue with multiple secondary index blocks does not grow store file size.
Verification: mvn test -Dtest=SCQIndexingTest#sequenceForPositionMustNotCreateSecondaryIndexBlocksWithLargeQueue
Expected: PASS

AC-ID: AC-005
Statement: Interior-hole anchor handling preserves correct reads before, around, and after the hole.
Verification: mvn test -Dtest=SCQIndexingTest#sequenceForPositionWithInteriorHoleStillFindsCorrectAnchor
Expected: PASS

AC-ID: AC-006
Statement: Concurrent tailer access does not throw exceptions or corrupt state.
Verification: mvn test -Dtest=SCQIndexingTest#concurrentTailersDoNotCorruptOrThrow
Expected: PASS

AC-ID: AC-007
Statement: JVM subprocess startup completes well within timeout window (flakiness guard).
Verification: mvn test -Dtest=SCQIndexingTest#tableStoreWriteLockSubprocessStartsWithin15Seconds
Expected: PASS, elapsedMs < 10000

AC-ID: AC-008
Statement: Full test suite passes without regression, including 0 TableStoreWriteLockTest timeout errors.
Verification: mvn test 2>&1 | tail -5
Expected: BUILD SUCCESS, >= 1117 tests, 0 failures, 0 errors
```

---

## Risk Notes

- **ArchUnit dependency already present:** `com.tngtech.archunit:archunit:1.2.1` is already in the project's test dependencies. No dependency changes needed.

- **`getSecondaryAddressReadOnly` is private:** The ArchUnit call-graph rule in Task 2 validates at the bytecode level that `sequenceForPosition` does not call `getSecondaryAddress`. This catches accidental method renames or refactors that bypass the read-only path.

- **Binary search plus hole fallback rationale:** `sequenceForPosition` now combines binary anchor lookup with hole-aware reverse-scan fallback. Task 5 provides a concrete regression guard for interior-hole correctness so this behavior is not lost in future refactors.

- **Implementation variant awareness:** Task 3 (helper method ArchUnit guard) is conditional on codex's implementation being merged. If sonnet's simpler forward-scan approach is adopted instead, the helper methods won't exist and that task should be skipped. The behavioral tests (Tasks 4-6) are implementation-agnostic and must pass regardless of which variant is merged.

- **Branch drift risk:** Some branches may not yet contain `SCQIndexingTest.java` or the companion changes to `SingleChronicleQueueStoreTest.java` and `TableStoreWriteLockTest.java`. Task 0 explicitly verifies and backfills all three files so hardening work cannot proceed without preserving prior protections.

- **No production code changes in this iteration.** All changes are test-only, minimizing risk.

- **TableStoreWriteLockTest flakiness:** The 4 subprocess-spawning tests use 15s timeouts (up from 5s) to accommodate cold JVM startup (~5.5s). Task 7 adds a smoke test that warns if JVM startup approaches the timeout window, making future flakiness on slower CI environments debuggable before it becomes opaque timeout failures.

- **Duplicate implementation risk (dex):** The Iteration 1 review found impl/dex was byte-identical to impl/codex across all files. This was vetoed. Future iterations should monitor for duplicate submissions and ensure each lane produces independent work.
