/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class SingleChronicleQueueNamedTailerMetadataTest extends QueueTestCommon {

    private static SingleChronicleQueueBuilder builder(File dir, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.single(dir)
                .rollCycle(TestRollCycles.TEST_DAILY)
                .timeProvider(time);
    }

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
    public void namedTailerIndexesReturnsCommittedTailerPositionsOnly() {
        File dir = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(10_000));
        writeDailyExcerpts(dir, time, 2);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            long gatewayIndex = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer gateway = q.createTailer("gateway")) {
                assertTrue(gateway.moveToIndex(gatewayIndex));
            }

            String replicated = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
            try (ExcerptTailer sink = q.createTailer(replicated)) {
                assertTrue(sink.moveToIndex(q.rollCycle().toIndex(q.lastCycle(), 0)));
            }

            NavigableMap<String, Long> indexes = q.namedTailerIndexes();
            assertEquals(Long.valueOf(gatewayIndex), indexes.get("gateway"));
            assertEquals(Long.valueOf(q.rollCycle().toIndex(q.lastCycle(), 0)), indexes.get(replicated));
            assertTrue(indexes.keySet().stream().noneMatch(k -> k.endsWith(".lock") || k.endsWith(".version")));
        }
    }

    @Test
    public void parkNamedTailerResetsExistingNonReplicatedTailer() {
        File dir = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(20_000));
        writeDailyExcerpts(dir, time, 3);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            long pinned = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer dead = q.createTailer("dead")) {
                assertTrue(dead.moveToIndex(pinned));
            }

            assertTrue(q.parkNamedTailer("dead"));
            assertEquals(Long.valueOf(0L), q.namedTailerIndexes().get("dead"));
        }
    }

    @Test
    public void parkNamedTailerDoesNotCreateMissingTailer() {
        File dir = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(30_000));
        writeDailyExcerpts(dir, time, 1);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            assertFalse(q.parkNamedTailer("missing"));
            assertFalse(q.namedTailerIndexes().containsKey("missing"));
        }
    }

    @Test
    public void parkNamedTailerRejectsReservedSuffixesWithoutMutatingMetadata() {
        File dir = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(40_000));
        writeDailyExcerpts(dir, time, 1);

        try (SingleChronicleQueue q = builder(dir, time).build();
             LongValue version = q.indexVersionForId("gateway");
             TableStoreWriteLock lock = q.versionIndexLockForId("gateway")) {
            version.setValue(42L);
            lock.lock();
            try {
                long lockBefore = q.tableStoreGet("index.gateway.lock");

                assertThrows(IllegalArgumentException.class, () -> q.parkNamedTailer("gateway.version"));
                assertEquals(42L, version.getValue());

                assertThrows(IllegalArgumentException.class, () -> q.parkNamedTailer("gateway.lock"));
                assertEquals(lockBefore, q.tableStoreGet("index.gateway.lock"));
            } finally {
                lock.unlock();
            }
        }
    }

    @Test
    public void replicatedNamedTailersCannotBeParked() {
        File dir = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(50_000));
        writeDailyExcerpts(dir, time, 2);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            String name = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "sink";
            long pinned = q.rollCycle().toIndex(q.firstCycle(), 0);
            try (ExcerptTailer sink = q.createTailer(name)) {
                assertTrue(sink.moveToIndex(pinned));
            }
            try (LongValue version = q.indexVersionForId(name)) {
                long versionBefore = version.getValue();

                assertFalse(q.parkNamedTailer(name));
                assertEquals(Long.valueOf(pinned), q.namedTailerIndexes().get(name));
                assertEquals(versionBefore, version.getValue());
            }
        }
    }

    @Test
    public void createTailerRejectsLockAndVersionSuffixes() {
        File dir = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(60_000));
        writeDailyExcerpts(dir, time, 1);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            assertThrows(IllegalArgumentException.class, () -> q.createTailer("gateway.lock"));
            assertThrows(IllegalArgumentException.class, () -> q.createTailer("gateway.version"));
        }
    }

    @Test
    public void fileForCycleReturnsExistingRollFileOnly() {
        File dir = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(TimeUnit.DAYS.toNanos(70_000));
        writeDailyExcerpts(dir, time, 1);

        try (SingleChronicleQueue q = builder(dir, time).build()) {
            File first = q.fileForCycle(q.firstCycle());
            assertNotNull(first);
            assertTrue(first.exists());
            assertNull(q.fileForCycle(q.firstCycle() - 1));
        }
    }
}
