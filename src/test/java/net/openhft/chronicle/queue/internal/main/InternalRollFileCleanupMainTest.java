/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the retention CLI surfaces the expected {@code warn}-level conditions, asserted through
 * {@link QueueTestCommon#expectException}. The {@code --verbose} debug trace is captured separately
 * in {@code InternalRollFileCleanupMainOutputTest} (it swaps exception handlers, which must not
 * clash with this base class's tracker).
 */
public class InternalRollFileCleanupMainTest extends QueueTestCommon {

    private static SingleChronicleQueueBuilder builder(File dir, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.single(dir).rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time);
    }

    private static void writeDaily(File dir, SetTimeProvider time, int days) {
        try (ChronicleQueue q = builder(dir, time).build();
             ExcerptAppender appender = q.createAppender()) {
            for (int d = 0; d < days; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
        }
    }

    @Test
    public void warnsWhenATailerLags() throws Exception {
        File dir = Files.createTempDirectory("main-lag").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(60_000));
        writeDaily(dir, time, 5);
        try (SingleChronicleQueue q = builder(dir, time).build();
             ExcerptTailer slow = q.createTailer("slow")) {
            assertTrue(slow.moveToIndex(q.rollCycle().toIndex(q.firstCycle(), 0)));
        }

        expectException("pin rolls below cycle");
        boolean warned = InternalRollFileCleanupMain.sweep(dir, 2, 0, false, Collections.emptyList(), false);
        assertTrue("a lagging tailer must be reported as warned", warned);
    }

    @Test
    public void warnsWhenDiskBelowThreshold() throws Exception {
        File dir = Files.createTempDirectory("main-disk").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(70_000));
        writeDaily(dir, time, 3);

        expectException("below threshold");
        boolean warned = InternalRollFileCleanupMain.sweep(
                dir, 2, Long.MAX_VALUE, false, Collections.emptyList(), false);
        assertTrue("low disk must be reported as warned", warned);
    }

    @Test
    public void parkWithoutDeleteRetiresTailerAndRemovesNothing() throws Exception {
        File dir = Files.createTempDirectory("main-park-oneshot").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(74_000));
        writeDaily(dir, time, 5);
        createLaggingTailer(dir, time, "dead");

        // --park is a one-shot metadata action: it applies on its own (no --delete) and never
        // removes files. Parking happens before analysis, so the retired tailer no longer lags.
        boolean warned = InternalRollFileCleanupMain.sweep(
                dir, 2, 0, false, Collections.singletonList("dead"), false);

        assertFalse("a successful one-shot park does not warn", warned);
        assertEquals("park removes no roll files", 5, rollFiles(dir));
        try (SingleChronicleQueue reopened = builder(dir, time).build()) {
            assertEquals("--park must reset the committed tailer index to 0",
                    0L, reopened.namedTailerIndexes().get("dead").longValue());
        }
    }

    @Test
    public void parkDoesNotMutateTailerIfAnalysisPreflightFails() throws Exception {
        File dir = Files.createTempDirectory("main-park-preflight").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(86_000));
        writeDaily(dir, time, 5);
        final long pinned;
        try (SingleChronicleQueue q = builder(dir, time).build();
             ExcerptTailer dead = q.createTailer("dead")) {
            pinned = q.rollCycle().toIndex(q.firstCycle(), 0);
            assertTrue(dead.moveToIndex(pinned));
        }

        // The package-private sweep hook can still be called with a bad keep value. Even then,
        // a failing analysis/preflight must not partially apply the destructive --park action.
        expectException("failed to sweep queue");
        boolean warned = InternalRollFileCleanupMain.sweep(
                dir, 0, 0, false, Collections.singletonList("dead"), false);

        assertTrue("a failed analysis should be reported as warned", warned);
        try (SingleChronicleQueue reopened = builder(dir, time).build()) {
            assertEquals("failed preflight must leave the committed tailer index untouched",
                    pinned, reopened.namedTailerIndexes().get("dead").longValue());
        }
    }

    @Test
    public void parkCombinedWithDeleteIsUsageError() throws Exception {
        File dir = Files.createTempDirectory("main-park-delete").toFile();
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--park", "dead", "--delete"}));
        assertEquals("--park with --delete is a usage error", 1, e.exitCode());
    }

    @Test
    public void parkCombinedWithIntervalIsUsageError() throws Exception {
        File dir = Files.createTempDirectory("main-park-interval").toFile();
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--park", "dead", "--interval", "60"}));
        assertEquals("--park with --interval is a usage error", 1, e.exitCode());
    }

    @Test
    public void emptyParkValueIsUsageError() throws Exception {
        // A wrapper script expanding an unset variable produces --park "" - the run must fail
        // loudly, not silently degrade to a plain sweep the operator mistakes for a parked tailer.
        File dir = Files.createTempDirectory("main-park-empty").toFile();
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--park", ""}));
        assertEquals("--park with an empty value is a usage error", 1, e.exitCode());

        InternalRollFileCleanupMain.ExitCodeException e2 = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--park", ","}));
        assertEquals("--park with only commas is a usage error", 1, e2.exitCode());
    }

    @Test
    public void unknownParkNameWarns() throws Exception {
        File dir = Files.createTempDirectory("main-park-unknown").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(82_000));
        writeDaily(dir, time, 3);

        // A typo'd --park name must not be silently ignored: the operator believes the tailer was
        // retired while its stale index continues to pin rolls.
        expectException("no such named tailer");
        boolean warned = InternalRollFileCleanupMain.sweep(
                dir, 2, 0, false, Collections.singletonList("nosuch"), false);
        assertTrue("an unknown park name must be reported as warned", warned);
    }

    @Test
    public void corruptQueueIsSkippedAndOthersStillSwept() throws Exception {
        File root = Files.createTempDirectory("main-corrupt").toFile();
        File good = new File(root, "good");
        File bad = new File(root, "bad");
        assertTrue(bad.mkdir());
        // Make "bad" discoverable as a queue but unopenable via the factory below.
        assertTrue(new File(bad, "19700101.cq4").createNewFile());
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(83_000));
        writeDaily(good, time, 5);

        expectException("failed to sweep queue");
        boolean warned = InternalRollFileCleanupMain.sweep(root, 2, 0, true,
                Collections.emptyList(), false, queueDir -> {
                    if (queueDir.getName().equals("bad"))
                        throw new IllegalStateException("corrupt metadata");
                    return builder(queueDir, time).build();
                });

        assertTrue("a failed queue must be reported as warned", warned);
        assertEquals("the good queue must still be swept (keep-last-2 of 5 rolls)",
                2, rollFiles(good));
    }

    @Test
    public void noArgumentsIsUsageErrorNotExitZero() {
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[0]));
        assertEquals("a missing <rootDir> must not exit 0", 1, e.exitCode());
    }

    @Test
    public void nonexistentRootIsAnError() {
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        "/no/such/root/anywhere"}));
        assertEquals("a nonexistent root is an error, not a warning", 2, e.exitCode());
    }

    @Test
    public void missingOptionValueIsUsageError() throws Exception {
        File dir = Files.createTempDirectory("main-missing-value").toFile();
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep"}));
        assertEquals("a trailing option with no value is a usage error", 1, e.exitCode());
    }

    @Test
    public void malformedOptionValueIsUsageError() throws Exception {
        File dir = Files.createTempDirectory("main-bad-value").toFile();
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep", "two"}));
        assertEquals("a malformed option value is a usage error", 1, e.exitCode());
    }

    @Test
    public void keepBelowOneIsRejectedBeforeAnySweep() throws Exception {
        File dir = Files.createTempDirectory("main-keep-zero").toFile();
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep", "0"}));
        assertEquals("--keep 0 must be rejected up front as a usage error", 1, e.exitCode());
    }

    @Test
    public void nestedQueueDirectoryWarns() throws Exception {
        // A stray roll file copied into the root makes the root itself look like a queue, hiding
        // the real child queues. Directories inside a queue are not supported, so warn.
        File root = Files.createTempDirectory("main-nested").toFile();
        assertTrue(new File(root, "19700101.cq4").createNewFile());
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(84_000));
        writeDaily(new File(root, "real-queue"), time, 3);

        expectException("nested queue directory");
        boolean warned = InternalRollFileCleanupMain.sweep(
                root, 2, 0, false, Collections.emptyList(), false);
        assertTrue("a queue directory containing queue directories must warn", warned);
    }

    @Test
    public void sweepNeverMapsRollFileStores() throws Exception {
        File dir = Files.createTempDirectory("main-no-mapping").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(85_000));
        writeDaily(dir, time, 5);

        // Deleting a memory-mapped file fails on Windows and defers space reclaim on Linux, so the
        // sweep must analyse without ever mapping a roll-file store.
        java.util.concurrent.atomic.AtomicInteger acquired = new java.util.concurrent.atomic.AtomicInteger();
        net.openhft.chronicle.queue.impl.StoreFileListener listener = new net.openhft.chronicle.queue.impl.StoreFileListener() {
            @Override
            public void onAcquired(int cycle, File file) {
                acquired.incrementAndGet();
            }

            @Override
            public void onReleased(int cycle, File file) {
            }
        };
        boolean warned = InternalRollFileCleanupMain.sweep(dir, 2, 0, false, Collections.emptyList(), false,
                queueDir -> builder(queueDir, time).storeFileListener(listener).build());

        assertFalse(warned);
        assertEquals("analysis must not map any roll-file store", 0, acquired.get());
    }

    @Test
    public void failOnWarnThrowsExitCodeFromRun() throws Exception {
        File dir = Files.createTempDirectory("main-fail").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(75_000));
        writeDaily(dir, time, 5);
        createLaggingTailer(dir, time, "slow");

        expectException("pin rolls below cycle");
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep", "2", "--fail-on-warn"}));
        assertEquals(3, e.exitCode());
    }

    @Test
    public void failOnWarnThrowsBeforeIntervalSleep() throws Exception {
        File dir = Files.createTempDirectory("main-fail-interval").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(76_000));
        writeDaily(dir, time, 5);
        createLaggingTailer(dir, time, "slow");

        expectException("pin rolls below cycle");
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep", "2", "--interval", "3600", "--fail-on-warn"}));
        assertEquals(3, e.exitCode());
    }

    @Test
    public void deleteRemovesRemovableRollsAndReportsNoWarning() throws Exception {
        File dir = Files.createTempDirectory("main-delete").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(72_000));
        writeDaily(dir, time, 5);
        assertEquals("five rolls written", 5, rollFiles(dir));

        // End-to-end delete coverage (finding #9): keep last 2, no tailers, min-free 0 -> the three
        // oldest rolls are actually removed from disk and a clean sweep does not warn.
        boolean warned = InternalRollFileCleanupMain.sweep(dir, 2, 0, true, Collections.emptyList(), false);

        assertFalse("a clean delete sweep does not warn", warned);
        assertEquals("keep-last-2 leaves exactly two rolls on disk", 2, rollFiles(dir));
    }

    @Test
    public void wrongRootWithNoQueuesWarns() throws Exception {
        // A valid directory that holds no queues at all - a mistyped root (/var/queue instead of
        // /var/queues), or the wrong nesting level. It is neither a queue nor a parent of queues.
        File notQueues = Files.createTempDirectory("main-wrong-root").toFile();
        assertTrue(new File(notQueues, "logs").mkdir());
        assertTrue(new File(notQueues, "conf").mkdir());

        // A root that resolves to zero queues is almost certainly a typo, so the sweep
        // warns rather than reporting a healthy run.
        expectException("no queue directories found");
        boolean warned = InternalRollFileCleanupMain.sweep(
                notQueues, 2, 0, false, Collections.emptyList(), false);

        assertTrue("a root containing no queues should warn, not silently succeed", warned);
    }

    @Test
    public void failedDeleteIsReportedAsWarned() throws Exception {
        File dir = Files.createTempDirectory("main-delete-fail").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(78_000));
        writeDaily(dir, time, 5); // 5 rolls, keep 2 -> 3 removable

        // Open the queue while the directory is still writable (opening acquires the table-store lock).
        SingleChronicleQueue q = builder(dir, time).build();
        try {
            // Make the roll files un-deletable by removing write permission on the directory: on a
            // POSIX filesystem unlink() needs write on the parent directory. Writes to the already-open,
            // memory-mapped queue files are unaffected, so the sweep still analyses (and closes) normally.
            File probe = new File(dir, "perm-probe");
            assertTrue(probe.createNewFile());
            Assume.assumeTrue("filesystem supports removing directory write permission",
                    dir.setWritable(false, false));
            final boolean permissionsEnforced = !probe.delete();
            if (!permissionsEnforced) {   // running as root, or a filesystem that ignores the bit
                dir.setWritable(true, false);
                probe.delete();
            }
            Assume.assumeTrue("filesystem enforces directory write permission (not running as root)",
                    permissionsEnforced);

            // delete=true, no named tailers (no lag), min-free 0 (no disk warning). Every candidate
            // delete() fails, which logs a warn and stops that queue...
            expectException("failed to delete");
            boolean warned = InternalRollFileCleanupMain.sweep(
                    dir, 2, 0, true, Collections.emptyList(), false, queueDir -> q);

            // A failed delete is folded into the warned flag so that "--fail-on-warn"
            // makes the job exit 3 instead of silently succeeding while no disk was reclaimed.
            assertTrue("a failed delete must be reported as warned", warned);
            assertEquals("no roll was actually deleted", 5, rollFiles(dir));
        } finally {
            dir.setWritable(true, false);
            q.close();
        }
    }

    @Test
    public void replicatedTailerParkIsRefusedByTheCli() throws Exception {
        File dir = Files.createTempDirectory("main-replicated-park").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(79_000));
        writeDaily(dir, time, 5);
        final String name = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";

        final long pinned;
        SingleChronicleQueue q = builder(dir, time).build();
        try {
            try (ExcerptTailer sink = q.createTailer(name)) {
                pinned = q.rollCycle().toIndex(q.firstCycle(), 0);
                assertTrue(sink.moveToIndex(pinned));
            }
            // The CLI refuses to park a replicated tailer: it warns instead of resetting it. This
            // tailer also pins rolls, so the sweep warns for both the refused park and lag.
            expectException("refusing to park replicated named tailer");
            expectException("pin rolls below cycle");
            InternalRollFileCleanupMain.sweep(
                    dir, 2, 0, false, Collections.singletonList(name), false, queueDir -> q);
        } finally {
            q.close();
        }

        try (SingleChronicleQueue reopened = builder(dir, time).build()) {
            assertEquals("a refused park must leave the committed index untouched",
                    pinned, reopened.namedTailerIndexes().get(name).longValue());
        }
    }

    @Test
    public void failOnWarnThrowsWhenDryRunReplicatedParkIsRefusedWithoutLag() throws Exception {
        File dir = Files.createTempDirectory("main-replicated-park-clean").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(81_000));
        writeDaily(dir, time, 5);
        final String name = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
        final long currentCycleIndex;

        try (SingleChronicleQueue q = builder(dir, time).build();
             ExcerptTailer sink = q.createTailer(name)) {
            currentCycleIndex = q.rollCycle().toIndex(q.lastCycle(), 0);
            assertTrue(sink.moveToIndex(currentCycleIndex));
        }

        expectException("refusing to park replicated named tailer");
        InternalRollFileCleanupMain.ExitCodeException e = assertThrows(
                InternalRollFileCleanupMain.ExitCodeException.class,
                () -> InternalRollFileCleanupMain.run(new String[]{
                        dir.getAbsolutePath(), "--keep", "2", "--park", name, "--fail-on-warn"}));
        assertEquals(3, e.exitCode());

        try (SingleChronicleQueue reopened = builder(dir, time).build()) {
            assertEquals("dry-run refused park must leave the committed index untouched",
                    currentCycleIndex, reopened.namedTailerIndexes().get(name).longValue());
        }
    }

    private static int rollFiles(File dir) {
        File[] files = dir.listFiles((d, n) -> n.endsWith(".cq4"));
        return files == null ? 0 : files.length;
    }

    private static void createLaggingTailer(File dir, SetTimeProvider time, String name) {
        try (SingleChronicleQueue q = builder(dir, time).build();
             ExcerptTailer slow = q.createTailer(name)) {
            assertTrue(slow.moveToIndex(q.rollCycle().toIndex(q.firstCycle(), 0)));
        }
    }
}
