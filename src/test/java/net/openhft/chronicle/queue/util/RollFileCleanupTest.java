/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.internal.util.InternalRollFileCleanup;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Proves commitment-based retention: a stopped named tailer's unread rolls are protected by its
 * persisted index, the last N cycles are always kept, and reading a tailer's position never
 * advances it.
 */
public class RollFileCleanupTest extends QueueTestCommon {

    private static SingleChronicleQueueBuilder builder(File dir, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.single(dir).rollCycle(TestRollCycles.TEST_DAILY).timeProvider(time);
    }

    /** Writes one excerpt per day so each write lands in its own daily roll. */
    private static void writeDailyExcerpts(File dir, SetTimeProvider time, int days) {
        try (ChronicleQueue queue = builder(dir, time).build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int d = 0; d < days; d++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("n").int32(d);
                }
                time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            }
        }
    }

    @Test
    public void aLaggingStoppedConsumerExtendsRetentionAndWarns() throws Exception {
        File dir = Files.createTempDirectory("retain").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(10_000));
        writeDailyExcerpts(dir, time, 6); // cycles f .. f+5

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            final int target = q.firstCycle() + 2;
            final long targetIndex = q.rollCycle().toIndex(target, 0);
            // Position the named consumer deterministically (moveToIndex also models an operator
            // override of a consumer's committed position), then "stop" it by closing.
            try (ExcerptTailer consumer = q.createTailer("gateway")) {
                assertTrue(consumer.moveToIndex(targetIndex));
            }

            InternalRollFileCleanup.Analysis a = InternalRollFileCleanup.analyse(q, 2);

            // keepFloor (last-2 window) would allow deleting more, but the lagging consumer at cycle
            // f+2 pulls the floor down to itself: only the strictly-older rolls go.
            assertEquals(target, a.deleteBelow());
            assertEquals("only cycles f and f+1 are removable", 2, a.removable().size());
            assertTrue("a consumer older than the last-2 window is lagging", a.lagWarning());
            assertEquals(Collections.singletonList("gateway"), a.laggingTailers());

            // The analysis never advanced the consumer's committed position.
            try (ExcerptTailer consumer = q.createTailer("gateway")) {
                assertEquals(targetIndex, consumer.index());
            }
        }
    }

    @Test
    public void lagWarningWhenTailerOlderThanKeepWindow() throws Exception {
        File dir = Files.createTempDirectory("retain-lag").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(20_000));
        writeDailyExcerpts(dir, time, 5);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            // A consumer positioned at the first cycle (a real, non-zero index) pins rolls older
            // than the last-2 window.
            try (ExcerptTailer stalled = q.createTailer("stalled")) {
                assertTrue(stalled.moveToIndex(q.rollCycle().toIndex(q.firstCycle(), 0)));
            }
            InternalRollFileCleanup.Analysis a = InternalRollFileCleanup.analyse(q, 2);
            assertTrue("tailer pins rolls older than the last-2 window", a.lagWarning());
            assertEquals(Collections.singletonList("stalled"), a.laggingTailers());
            assertTrue("nothing removable behind a first-cycle tailer", a.removable().isEmpty());
        }
    }

    @Test
    public void parkingADeadTailerResetsItToZeroAndStopsItPinning() throws Exception {
        File dir = Files.createTempDirectory("retain-park").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(25_000));
        writeDailyExcerpts(dir, time, 5); // cycles f .. f+4

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            // A dead consumer positioned at the first cycle pins everything and lags.
            try (ExcerptTailer dead = q.createTailer("dead")) {
                assertTrue(dead.moveToIndex(q.rollCycle().toIndex(q.firstCycle(), 0)));
            }
            assertTrue("pinned before parking", InternalRollFileCleanup.analyse(q, 2).lagWarning());
            assertTrue(InternalRollFileCleanup.analyse(q, 2).removable().isEmpty());

            // Park it: index reset to 0, the new-tailer default, so it no longer pins.
            assertTrue(q.parkNamedTailer("dead"));
            assertEquals(Long.valueOf(0L), q.namedTailerIndexes().get("dead"));

            InternalRollFileCleanup.Analysis a = InternalRollFileCleanup.analyse(q, 2);
            assertFalse("parked tailer no longer lags", a.lagWarning());
            // keep-last-2 now governs: 5 cycles -> 3 removable. A restarted "dead" would resume
            // from the oldest surviving roll.
            assertEquals(3, a.removable().size());
        }
    }

    @Test
    public void aRegisteredButUnreadFromStartConsumerDoesNotProtectItsBacklog() throws Exception {
        File dir = Files.createTempDirectory("retain-unread").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(28_000));
        writeDailyExcerpts(dir, time, 5); // cycles f .. f+4

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            // Register a named consumer but never advance it: its committed index stays at 0, the
            // same value the analysis reads as "parked / never read".
            q.createTailer("newFromStart").close();
            assertEquals("a freshly registered named tailer sits at index 0",
                    Long.valueOf(0L), q.namedTailerIndexes().get("newFromStart"));

            InternalRollFileCleanup.Analysis a = InternalRollFileCleanup.analyse(q, 2);

            // Index 0 is indistinguishable from parked, so a consumer that intends to replay from the
            // start but has not yet read is NOT treated as lagging and does NOT pin its backlog.
            assertFalse("an unread from-start consumer is not flagged as lagging", a.lagWarning());
            assertEquals("its intended backlog (3 of 5 cycles) is removable", 3, a.removable().size());
        }
    }

    @Test
    public void createTailerRejectsLockAndVersionIdsWithIllegalArgument() throws Exception {
        File dir = Files.createTempDirectory("retain-badid").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(29_000));
        writeDailyExcerpts(dir, time, 1);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            // A bad tailer id is an argument error, so createTailer(String) rejects reserved
            // metastore suffixes with IllegalArgumentException.
            assertThrows(IllegalArgumentException.class, () -> q.createTailer("index.gateway.version"));
            assertThrows(IllegalArgumentException.class, () -> q.createTailer("index.gateway.lock"));
        }
    }

    @Test
    public void reservedTailerIdSuffixesCollideWithAnotherTailersMetadata() throws Exception {
        File dir = Files.createTempDirectory("retain-collide").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(31_000));
        writeDailyExcerpts(dir, time, 1);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            // Why the suffixes are reserved: tailer "a" keeps its state under the metadata keys
            // "index.a", "index.a.lock" and "index.a.version". A tailer *named* "a.lock" or
            // "a.version" would store its position under "index.a.lock" / "index.a.version" - the
            // very keys tailer "a" uses - so the two tailers would silently share state.
            final String replicated = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "a";
            q.createTailer(replicated).close();

            // The replicated tailer's ".lock"/".version" companion keys exist in the metastore but
            // must never surface as tailers of their own.
            assertTrue("the tailer itself is listed",
                    q.namedTailerIndexes().containsKey(replicated));
            assertTrue("its lock/version metadata keys are not tailers",
                    q.namedTailerIndexes().keySet().stream()
                            .noneMatch(k -> k.endsWith(".lock") || k.endsWith(".version")));

            // Both reserved suffixes are rejected with a message explaining the collision.
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> q.createTailer("a.version"));
            assertTrue("message names the offending id: " + e.getMessage(),
                    e.getMessage().contains("a.version"));
            assertTrue("message explains the reservation: " + e.getMessage(),
                    e.getMessage().contains("reserved"));
            assertThrows(IllegalArgumentException.class, () -> q.createTailer("a.lock"));
        }
    }

    @Test
    public void parkingAReplicatedTailerIsRefused() throws Exception {
        File dir = Files.createTempDirectory("retain-replicated-refuse").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(26_000));
        writeDailyExcerpts(dir, time, 5);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            final String name = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
            final long pinned = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer sink = q.createTailer(name)) {
                assertTrue(sink.moveToIndex(pinned));
            }
            try (LongValue version = q.indexVersionForId(name)) {
                final long versionBefore = version.getValue();
                // A backward index reset with no replication-version coordination is unsafe, so
                // parkNamedTailer refuses a replicated named tailer - returning false and leaving both
                // its committed index and its replication version untouched.
                assertFalse("parking a replicated named tailer must be refused", q.parkNamedTailer(name));
                assertEquals("a refused park must not move the committed index",
                        Long.valueOf(pinned), q.namedTailerIndexes().get(name));
                assertEquals("a refused park must not touch the replication version",
                        versionBefore, version.getValue());
            }
        }
    }

    @Test
    public void keepsOnlyLastNWhenNoTailers() throws Exception {
        File dir = Files.createTempDirectory("retain-none").toFile();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(30_000));
        writeDailyExcerpts(dir, time, 5);

        List<File> removable = FileUtil.removableRollFileCandidatesByTailerPosition(dir, 2)
                .collect(Collectors.toList());
        // 5 rolls, keep last 2 -> 3 removable, earliest first.
        assertEquals(3, removable.size());
        assertTrue(removable.get(0).getName().compareTo(removable.get(2).getName()) < 0);
    }

}
