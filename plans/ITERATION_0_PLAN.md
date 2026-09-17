# Chronicle-Queue Iteration 0: Implementation Review

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Audit the current Chronicle-Queue codebase for major implementation issues, covering build health, thread safety, indexing correctness, incomplete features, and test coverage gaps.

**Architecture:** This is a review-only iteration. No production code is changed. Findings are recorded as a `plans/FINDINGS.md` report, which will drive priorities for Iteration 1. Each task is a targeted inspection that produces written evidence of what was found.

**Tech Stack:** Java 8+, Maven, JUnit 5, JaCoCo, Chronicle-Wire, Chronicle-Bytes

---

## Context

Chronicle-Queue is a broker-less, off-heap, memory-mapped file queue for ultra-low-latency messaging. Core classes:

| File | LOC | Role |
|------|-----|------|
| `StoreTailer.java` | 1,871 | Reading + seeking |
| `SingleChronicleQueue.java` | 1,755 | Core queue lifecycle |
| `SingleChronicleQueueBuilder.java` | 1,655 | Builder / config |
| `StoreAppender.java` | 1,508 | Writing |
| `SCQIndexing.java` | 1,132 | Index management |

All source is under `src/main/java/net/openhft/chronicle/queue/`.
All tests are under `src/test/java/net/openhft/chronicle/queue/`.

---

## Output File

All findings go to: `plans/FINDINGS.md`

Create this file before Task 1 with the following skeleton:

```markdown
# Chronicle-Queue Iteration 0 Findings

## T1: Build and Test Health
## T2: TODO / FIXME Audit
## T3: Thread Safety Review
## T4: Indexing and Linear Scan Review
## T5: Incomplete Feature Audit
## T6: Test Coverage Review
## T7: API and Documentation Accuracy
```

---

## Task 1: Build and Test Health

**Goal:** Confirm the project compiles and tests pass from a clean state.

**Files:**
- Read: `pom.xml`
- Output: `plans/FINDINGS.md` § T1

**Step 1: Run a clean build**

```bash
cd /path/to/Chronicle-Queue
mvn -q clean verify -DskipTests
```

Expected: BUILD SUCCESS. Note any compiler warnings.

**Step 2: Run the test suite**

```bash
mvn test 2>&1 | tail -40
```

Expected: all tests pass. Record any failures, skips, or timeout patterns.

**Step 3: Check JaCoCo thresholds**

```bash
mvn verify 2>&1 | grep -E "(Coverage|WARN|FAILED|heck)"
```

Note the configured thresholds (line: 78.8%, branch: 71.3%) and whether they pass.

**Step 4: Record findings**

In `plans/FINDINGS.md` § T1 write:
- Did `mvn clean verify` succeed? Any warnings?
- How many tests ran, skipped, failed?
- Did JaCoCo thresholds pass?
- Any deprecated API warnings from the compiler?

---

## Task 2: TODO / FIXME Audit

**Goal:** Catalogue every `TODO`, `FIXME`, and `HACK` comment in main source (not tests).

**Files:**
- Search: `src/main/java/**/*.java`
- Output: `plans/FINDINGS.md` § T2

**Step 1: Find all TODO/FIXME comments**

```bash
grep -rn "TODO\|FIXME\|HACK\|XXX" src/main/java/ --include="*.java"
```

**Step 2: Classify each by severity**

For each match, assign:
- **Critical** — marks a known correctness bug or data-loss risk
- **High** — marks a performance regression path or missing required feature
- **Low** — marks a cleanup or nice-to-have

Known candidates from initial scan:
- `SCQIndexing.java` — "TODO use a binary rather than linear search" → **High**
- `SCQIndexing.java` — "TODO pos shouldn't be 0, but holes in the index appear" → **Critical** (data integrity)
- `StoreAppender.java` — "TODO FIX" (unspecified) → **Critical** (needs investigation)
- `StoreAppender.java` — "TODO: Position on existing one?" in EOF cycle handling → **High**
- `SingleTableBuilder.java` — "TODO Change this to a single chunk file in x.28" → **Low**
- `ChronicleHistoryReader.java` — "TODO: allow builder to be overridden" → **Low**
- `HistoryReader.java` — `UnsupportedOperationException` body → **High**

**Step 3: Record findings**

In `plans/FINDINGS.md` § T2, produce a table:

| File | Line | Text | Severity | Notes |
|------|------|------|----------|-------|

---

## Task 3: Thread Safety Review

**Goal:** Verify that `@SingleThreaded` contracts are consistently enforced, and that shared mutable state is properly protected.

**Files to read:**
- `src/main/java/net/openhft/chronicle/queue/ExcerptAppender.java`
- `src/main/java/net/openhft/chronicle/queue/ExcerptTailer.java`
- `src/main/java/net/openhft/chronicle/queue/impl/single/StoreAppender.java`
- `src/main/java/net/openhft/chronicle/queue/impl/single/StoreTailer.java`
- `src/main/java/net/openhft/chronicle/queue/impl/single/SingleChronicleQueue.java`
- Output: `plans/FINDINGS.md` § T3

**Step 1: Locate all `synchronized` and `volatile` usage**

```bash
grep -n "synchronized\|volatile\|AtomicLong\|AtomicInteger\|ConcurrentHashMap" \
  src/main/java/net/openhft/chronicle/queue/impl/single/*.java
```

**Step 2: Check write-lock paths in `StoreAppender`**

Read `StoreAppender.java` and answer:
- Is `WriteLock` acquired before every mutation that could race?
- Are there any early-return or exception paths that skip lock release?
- Is there a try-finally block guarding each lock?

**Step 3: Check tailer state sharing**

Read `StoreTailer.java` and answer:
- Does any field shared between forward/backward direction reads lack synchronization?
- Is `TailerDirection` transition (FORWARD ↔ BACKWARD) handled atomically?
- Can two threads accidentally share a `StoreTailer` instance without detection?

**Step 4: Check `SingleChronicleQueue` lifecycle**

Read `SingleChronicleQueue.java` and answer:
- Is `close()` idempotent and thread-safe?
- Is the `StorePool` (queue file cache) thread-safe?
- Are there any `static` mutable fields?

**Step 5: Record findings**

In `plans/FINDINGS.md` § T3 write:
- List of any unsynchronized mutations on shared state
- Any missing try-finally around lock acquire/release
- Any `static` mutable state that could leak across tests
- Overall thread-safety verdict: Clean / Minor issues / Major issues

---

## Task 4: Indexing and Linear Scan Review

**Goal:** Assess the correctness and latency risk of the indexing subsystem, particularly the linear scan fallback.

**Files to read:**
- `src/main/java/net/openhft/chronicle/queue/impl/single/SCQIndexing.java`
- `src/main/java/net/openhft/chronicle/queue/impl/single/StoreTailer.java` (binary search / linear scan sections)
- `src/main/java/net/openhft/chronicle/queue/impl/BinarySearch.java`
- Output: `plans/FINDINGS.md` § T4

**Step 1: Locate the linear scan threshold**

In `StoreTailer.java`, search for the value `70` (the linear scan fallback threshold):

```bash
grep -n "70\|LINEAR\|linear" src/main/java/net/openhft/chronicle/queue/impl/single/StoreTailer.java | head -20
```

Read the surrounding logic: when does it fall back, and how far can the scan travel?

**Step 2: Trace the binary search implementation**

Read `BinarySearch.java`. Answer:
- Does it correctly handle sparse indices (gaps)?
- Does it handle the edge case where the target index is before the first entry?
- Is there a worst-case O(n) path hidden inside the binary search?

**Step 3: Audit the index-hole TODO**

In `SCQIndexing.java`, find the comment "TODO pos shouldn't be 0, but holes in the index appear". Read the surrounding 50 lines. Answer:
- What conditions produce a zero `pos`?
- What does the code do when `pos == 0`? Does it skip, corrupt, or throw?
- Is this reproducible with any existing test?

**Step 4: Check index consistency invariants**

Search for `queue.check.index` usage:

```bash
grep -rn "check.index\|checkIndex\|CHECK_INDEX" src/main/java/ --include="*.java"
```

Read the invariant check code. Is the check actually called in hot paths, or only in debug mode?

**Step 5: Record findings**

In `plans/FINDINGS.md` § T4 write:
- Linear scan trigger condition and worst-case scan distance
- Any correctness gaps in `BinarySearch.java`
- Impact of the index-hole bug (data skip, stall, or corruption)
- Whether index consistency checks are meaningful in production

---

## Task 5: Incomplete Feature Audit

**Goal:** Identify methods that throw `UnsupportedOperationException` or are otherwise unimplemented stubs.

**Files:**
- Search: `src/main/java/**/*.java`
- Output: `plans/FINDINGS.md` § T5

**Step 1: Find all stubs**

```bash
grep -rn "UnsupportedOperationException\|throw new UnsupportedOperation" \
  src/main/java/ --include="*.java"
```

**Step 2: Classify each stub**

For each occurrence, determine:
- Is the method part of the public API (in `net.openhft.chronicle.queue` package, not `impl` or `internal`)?
- Is it called by any test?
- Is it documented as enterprise-only or future-work?

Known candidates:
- `HistoryReader.java` — at least one method body is a stub
- Several methods in replication-related classes
- `ReadOnlyWriteLock` — write operations should throw

**Step 3: Check deprecated API removal timeline**

```bash
grep -rn "@Deprecated\|deltaCheckpointInterval\|StoreFileListener" \
  src/main/java/ --include="*.java" | head -20
```

Note which version they are scheduled for removal (documented as `x.29`).

**Step 4: Record findings**

In `plans/FINDINGS.md` § T5 produce a table:

| Class | Method | Public API? | Tested? | Notes |
|-------|--------|-------------|---------|-------|

---

## Task 6: Test Coverage Review

**Goal:** Identify under-tested areas, particularly in the largest and most complex classes.

**Files:**
- `src/test/java/**/*.java`
- Output: `plans/FINDINGS.md` § T6

**Step 1: Map tests to the five largest classes**

For each class below, list the test classes that exercise it:

| Production Class | LOC | Test classes |
|-----------------|-----|-------------|
| `StoreTailer` | 1,871 | ? |
| `SingleChronicleQueue` | 1,755 | ? |
| `SingleChronicleQueueBuilder` | 1,655 | ? |
| `StoreAppender` | 1,508 | ? |
| `SCQIndexing` | 1,132 | ? |

```bash
grep -rl "StoreTailer\|StoreAppender\|SCQIndexing\|SingleChronicleQueue" \
  src/test/java/ --include="*.java"
```

**Step 2: Check coverage of error paths**

Search for tests that cover exception branches:

```bash
grep -rl "MissingStoreFileException\|IllegalIndexException\|IndexNotAvailableException\|NamedTailerNotAvailableException" \
  src/test/java/ --include="*.java"
```

Are all custom exception types exercised by at least one test?

**Step 3: Check backward-read coverage**

```bash
grep -rl "BACKWARD\|readBackwards\|TailerDirection" \
  src/test/java/ --include="*.java"
```

Verify `QueueReadBackwardsTest.java` exists and exercises direction switching.

**Step 4: Check interrupt-handling test coverage**

```bash
grep -rl "interrupt\|Interrupt" src/test/java/ --include="*.java"
```

`NoDataIsSkippedWithInterruptTest.java` should exist. Does it cover the documented "avoid interrupts" warning?

**Step 5: Record findings**

In `plans/FINDINGS.md` § T6 write:
- Which of the five largest classes has the weakest test mapping?
- Which custom exceptions are not tested?
- Any major untested scenario (backward read edge cases, interrupt handling, cycle overflow)?

---

## Task 7: API and Documentation Accuracy

**Goal:** Check that the public API Javadoc and README-level documentation match the actual implementation.

**Files to read:**
- `src/main/java/net/openhft/chronicle/queue/ChronicleQueue.java`
- `src/main/java/net/openhft/chronicle/queue/ExcerptAppender.java`
- `src/main/java/net/openhft/chronicle/queue/ExcerptTailer.java`
- `docs/How_it_works.adoc` (or equivalent)
- Output: `plans/FINDINGS.md` § T7

**Step 1: Check threading documentation on public API**

Read `ExcerptAppender.java` and `ExcerptTailer.java`. Verify:
- Is `@SingleThreaded` annotation present on both interfaces?
- Is there a Javadoc comment explaining the threading constraint?
- Is misuse detectable at runtime (guard clause, assertion, or check)?

**Step 2: Check `clear()` method contract**

In `ChronicleQueue.java`, find the `clear()` method. Read its Javadoc. Then find its implementation in `SingleChronicleQueue.java`. Answer:
- Does the implementation match the documented behaviour?
- Is there a test for `clear()` followed by append and read?

**Step 3: Check filesystem constraint documentation**

Search the docs directory:

```bash
grep -rn "NFS\|network.file\|AFS\|SAN\|memory.map" docs/ --include="*.adoc" --include="*.md"
```

Verify the NFS/network-filesystem constraint is prominently documented, not buried.

**Step 4: Check roll cycle documentation**

Read `docs/` for roll cycle documentation. Verify that all roll cycles in `rollcycles/` package (MINUTELY, HOURLY, DAILY, FOUR_HOURLY, TWO_HOURLY, WEEKLY, MONTHLY, SPARSE, LARGE, TEST) are mentioned and their index capacity documented.

**Step 5: Record findings**

In `plans/FINDINGS.md` § T7 write:
- Are threading constraints documented at the API level and verified at runtime?
- Is `clear()` correctly implemented and tested?
- Is the NFS constraint easy to find?
- Any roll cycle undocumented or missing from docs?

---

## Acceptance Criteria

```text
AC-ID: AC-001
Statement: plans/FINDINGS.md exists and has a non-empty section for each of T1–T7.
Verification: ls -la plans/FINDINGS.md && grep -c "^## T" plans/FINDINGS.md
Expected output: 7

AC-ID: AC-002
Statement: Every TODO/FIXME in src/main/java is listed in FINDINGS.md § T2 with a severity.
Verification: grep -c "TODO\|FIXME" src/main/java/**/*.java vs count in FINDINGS.md table.

AC-ID: AC-003
Statement: The SCQIndexing index-hole bug is documented with a concrete reproduction path or confirmed non-reproducible.
Verification: FINDINGS.md § T4 contains a paragraph starting with "Index-hole:" that is non-empty.

AC-ID: AC-004
Statement: Every UnsupportedOperationException throw site is listed in FINDINGS.md § T5.
Verification: Count from grep matches count in table.

AC-ID: AC-005
Statement: FINDINGS.md concludes with a ranked priority list of at most 10 issues for Iteration 1.
Verification: FINDINGS.md ends with a "## Priority List for Iteration 1" section with numbered items.
```

---

## Completion

When all seven tasks are done and `plans/FINDINGS.md` is complete:

1. Verify all AC checks above pass
2. Commit findings:
   ```bash
   git add plans/
   git commit -m "chore: iteration 0 review findings"
   ```
3. The priority list at the bottom of `FINDINGS.md` becomes the input to `ITERATION_1_PLAN.md`
