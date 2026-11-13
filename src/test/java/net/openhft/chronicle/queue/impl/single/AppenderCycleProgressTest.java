/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteAfterEOFException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class AppenderCycleProgressTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private static final long DAY = TimeUnit.DAYS.toMillis(1);

    private SingleChronicleQueue open(File dir, AtomicLong clock) {
        return SingleChronicleQueueBuilder.binary(dir).testBlockSize()
                .rollCycle(LegacyRollCycles.DAILY).timeProvider(clock::get).build();
    }

    private void put(StoreAppender appender, long value, boolean document) {
        if (document) {
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeLong(value);
            }
        } else {
            Bytes<?> bytes = Bytes.allocateElasticOnHeap();
            try { bytes.writeLong(value); appender.writeBytes(bytes); }
            finally { bytes.releaseLast(); }
        }
    }

    // Internal test fixture: no public API for injecting trailing bytes is implied.
    private long seal(SingleChronicleQueue queue, StoreAppender appender) {
        queue.writeLock().lock();
        try {
            long start = appender.wire().bytes().writePosition();
            assertTrue(appender.wire().writeEndOfWire(1, TimeUnit.SECONDS, start));
            long eof = appender.wire().bytes().writePosition() - 8;
            assertEquals(Wires.END_OF_DATA, appender.wire().bytes().readVolatileInt(eof));
            return eof;
        } finally { queue.writeLock().unlock(); }
    }

    private void replay(SingleChronicleQueue queue, long... values) {
        try (ExcerptTailer tailer = queue.createTailer()) {
            for (long value : values) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent()); assertEquals(value, dc.wire().bytes().readLong());
                }
            }
            try (DocumentContext dc = tailer.readingDocument()) { assertFalse(dc.isPresent()); }
        }
    }

    @Test public void ownSealAdvancesRepeatedlyWithFrozenClock() throws Exception {
        for (boolean document : new boolean[]{false, true}) {
            AtomicLong clock = new AtomicLong(DAY);
            try (SingleChronicleQueue queue = open(temporary.newFolder(), clock);
                 StoreAppender appender = (StoreAppender) queue.createAppender()) {
                for (int cycle = 1; cycle <= 4; cycle++) {
                    put(appender, cycle, document);
                    assertEquals(cycle, appender.cycle());
                    assertEquals(0, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
                    if (cycle != 4) seal(queue, appender);
                }
                assertEquals(1, queue.cycle());
                replay(queue, 1, 2, 3, 4);
            }
        }
    }

    @Test public void laggingAppenderFollowsSealsAcrossQueueInstances() throws Exception {
        File dir = temporary.newFolder();
        AtomicLong clock = new AtomicLong(DAY);
        try (SingleChronicleQueue first = open(dir, clock);
             SingleChronicleQueue second = open(dir, clock);
             StoreAppender a = (StoreAppender) first.createAppender();
             StoreAppender b = (StoreAppender) second.createAppender()) {
            put(a, 1, true); put(b, 2, false);
            seal(second, b);
            put(b, 3, true); seal(second, b);
            put(a, 4, false);
            assertEquals(3, a.cycle());
            replay(first, 1, 2, 3, 4);
        }
    }

    @Test public void backwardClockAndReopenNeverCreateHistoricalRolls() throws Exception {
        File dir = temporary.newFolder();
        AtomicLong clock = new AtomicLong(3 * DAY);
        try (SingleChronicleQueue queue = open(dir, clock);
             StoreAppender a = (StoreAppender) queue.createAppender()) {
            put(a, 1, true); clock.set(DAY); put(a, 2, false);
            assertEquals(3, a.cycle());
        }
        try (SingleChronicleQueue queue = open(dir, clock);
             StoreAppender a = (StoreAppender) queue.createAppender()) {
            put(a, 3, true); assertEquals(3, a.cycle()); assertEquals(3, queue.firstCycle());
            replay(queue, 1, 2, 3);
        }
    }

    @Test public void indexedWriteRemainsStrictAndOrdinaryWriteRecovers() throws Exception {
        AtomicLong clock = new AtomicLong(DAY);
        try (SingleChronicleQueue queue = open(temporary.newFolder(), clock);
             StoreAppender a = (StoreAppender) queue.createAppender()) {
            put(a, 1, true); long index = a.lastIndexAppended() + 1;
            seal(queue, a);
            Bytes<?> bytes = Bytes.allocateElasticOnHeap();
            try {
                bytes.writeLong(99);
                assertThrows(WriteAfterEOFException.class, () -> a.writeBytes(index, bytes));
            } finally { bytes.releaseLast(); }
            put(a, 2, false);
            assertEquals(2, a.cycle()); replay(queue, 1, 2);
        }
    }

    @Test public void futureCycleWithoutSealDoesNotMoveAnActiveAppender() throws Exception {
        AtomicLong clock = new AtomicLong(DAY);
        try (SingleChronicleQueue queue = open(temporary.newFolder(), clock);
             StoreAppender a = (StoreAppender) queue.createAppender()) {
            put(a, 1, true);
            // Same store acquisition used to precreate a future cycle, without sealing this one.
            SingleChronicleQueueStore future = queue.storeForCycle(3, 0, true, null);
            future.close();
            put(a, 2, false); assertEquals(1, a.cycle()); replay(queue, 1, 2);
        }
    }
    @Test public void laggingAndReopenedAppendersSkipMissingCycles() throws Exception {
        File dir = temporary.newFolder();
        AtomicLong clock = new AtomicLong(DAY);
        try (SingleChronicleQueue queue = open(dir, clock);
             StoreAppender a = (StoreAppender) queue.createAppender();
             StoreAppender lagging = (StoreAppender) queue.createAppender()) {
            put(lagging, 1, false); clock.set(5 * DAY); put(a, 2, true); clock.set(DAY);
            put(lagging, 3, false); assertEquals(5, lagging.cycle());
        }
        try (SingleChronicleQueue queue = open(dir, clock);
             StoreAppender reopened = (StoreAppender) queue.createAppender()) {
            put(reopened, 4, true); assertEquals(5, reopened.cycle());
            assertEquals(2, dir.listFiles(f -> f.getName().endsWith(".cq4")).length);
            replay(queue, 1, 2, 3, 4);
        }
    }

}
