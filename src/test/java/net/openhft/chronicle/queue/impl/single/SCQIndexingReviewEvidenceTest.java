/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.QueueSystemProperties;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Deterministic review evidence: assert the physical sequence before bounding header work.
 * Tail lookup and writingDocument performance regressions are distinguished from the
 * pre-existing finite-position, moveToIndex and forward-toEnd alias defects.
 */
public class SCQIndexingReviewEvidenceTest extends QueueTestCommon {

    private static final int RECORDS = 20_000;

    @Test
    public void lastSequenceNumberStaysOnTheTrackerForHugeDailyXSparse() throws Exception {
        assertTrackerLookup(SparseRollCycles.HUGE_DAILY_XSPARSE);
    }

    @Test
    public void lastSequenceNumberStaysOnTheTrackerForHugeDailyAboveSixteenMiB() throws Exception {
        assertTrackerLookup(LargeRollCycles.HUGE_DAILY);
    }

    @Test
    public void tailLookupAfterDirectByteWritesUsesCommittedFullPosition() throws Exception {
        assertTrackerLookup(SparseRollCycles.HUGE_DAILY_XSPARSE, true);
    }

    private void assertTrackerLookup(RollCycle rollCycle) throws Exception {
        assertTrackerLookup(rollCycle, false);
    }

    private void assertTrackerLookup(RollCycle rollCycle, boolean directBytes) throws Exception {
        boolean checkIndex = QueueSystemProperties.CHECK_INDEX;
        // Production writes must establish the pair without relying on optional assertion scans.
        QueueSystemProperties.CHECK_INDEX = false;
        Bytes<?> payload = Bytes.wrapForRead(new byte[8]);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(rollCycle).blockSize(32 << 20).timeProvider(() -> 0L).build();
             ExcerptAppender appender = queue.createAppender();
             StoreTailer tailer = (StoreTailer) queue.createTailer()) {
            long aliasPeriod = 1L << (64 - cycleShift(queue));
            if (aliasPeriod > (16 << 20))
                throw new AssertionError("this test needs a cycle with an alias period of at most 16 MiB");
            // Push the write position past the alias period with one large record, then add small ones.
            appender.writeBytes(bytes -> bytes.writeSkip(aliasPeriod));
            for (int i = 1; i < RECORDS; i++) {
                if (directBytes) {
                    // The BytesStore overload bypasses writingDocument/resetPosition.
                    appender.writeBytes(payload);
                } else {
                    appender.writeText("record-" + i);
                }
            }
            SingleChronicleQueueStore store = ((StoreAppender) appender).store;
            long writePosition = store.writePosition();
            assertTrue("write position " + writePosition + " must be at or above the alias period " + aliasPeriod,
                    writePosition >= aliasPeriod);

            assertTrue(tailer.moveToIndex(queue.firstIndex()));
            CountingContext ctx = new CountingContext(tailer);

            long start = System.nanoTime();
            long seq = store.lastSequenceNumber(ctx);
            long tookUs = (System.nanoTime() - start) / 1000;

            assertEquals(RECORDS - 1, seq);
            String message = rollCycle + ": lastSequenceNumber read " + ctx.headersRead + " headers in " + tookUs
                    + " us at write position " + writePosition + " (alias period " + aliasPeriod + ")";
            System.out.println(message);
            assertTrue(message, ctx.headersRead <= 4);
        } finally {
            payload.releaseLast();
            QueueSystemProperties.CHECK_INDEX = checkIndex;
        }
    }

    @Test
    public void moveToIndexWithStaleAliasedTrackerLandsOnTheCorrectRecord() throws Exception {
        long aliasPeriod = 65536;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            // The same pattern as SequenceForPositionSafetyTest.writeAliasablePattern.
            appender.writeText("first");
            appender.writeBytes(bytes -> bytes.writeSkip(aliasPeriod - 4));
            SingleChronicleQueueStore store = ((StoreAppender) appender).store;
            long previousPosition = store.writePosition();
            appender.writeText("last");
            long lastPosition = store.writePosition();
            assertEquals(aliasPeriod, lastPosition - previousPosition);

            // Control: with a current tracker, moveToIndex(1) reads the filler record.
            assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(queue.cycle(), 1)));
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertTrue(dc.wire().bytes().readRemaining() >= aliasPeriod - 4);
            }

            // A writer that stopped between the write position and the tracker leaves the
            // tracker on the previous record, which aliases the last one.
            store.indexing.sequence.setSequence(1, previousPosition);
            try {
                assertEquals(1, store.indexing.sequence.getSequence(lastPosition));
                assertTrue(tailer.moveToIndex(queue.rollCycle().toIndex(queue.cycle(), 1)));
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    long remaining = dc.wire().bytes().readRemaining();
                    assertTrue("moveToIndex(1) must land on the 65,532-byte filler record, not on the aliased"
                            + " last record; the record read has " + remaining + " bytes", remaining >= aliasPeriod - 4);
                }
            } finally {
                store.indexing.sequence.setSequence(2, lastPosition);
            }
        }
    }

    @Test
    public void singleAppenderDocumentWriteHasBoundedHeaderWork() throws Exception {
        assertDocumentWrites(1);
    }

    @Test
    public void alternatingAppenderDocumentWritesHaveBoundedHeaderWork() throws Exception {
        assertDocumentWrites(2);
    }

    @Test
    public void repeatedTailLookupRetainsTheLatestProvedFullPosition() throws Exception {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreAppender appender = (StoreAppender) queue.createAppender();
             SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(queue.file())
                     .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreTailer tailer = (StoreTailer) reader.createTailer()) {
            appender.writeText("first");
            appender.writeText("second");
            long secondPosition = appender.store.writePosition();
            for (int i = 2; i < RECORDS; i++)
                appender.writeText("record-" + i);
            tailer.toStart();
            CountingContext counter = new CountingContext(tailer);
            assertEquals(RECORDS - 1, tailer.store.lastSequenceNumber(counter));
            // A later finite lookup must not discard the newer, independently proved pair.
            assertEquals(1, tailer.store.sequenceForPosition(counter, secondPosition, true));
            appender.writeText("new-tail");
            counter.headersRead = 0;
            assertEquals(RECORDS, tailer.store.lastSequenceNumber(counter));
            assertTrue("A warmed reader rescanned " + counter.headersRead + " headers", counter.headersRead <= 4);
        }
    }

    @Test
    public void indexedLookupReusesTheProvedFullPosition() throws Exception {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreAppender appender = (StoreAppender) queue.createAppender();
             SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(queue.file())
                     .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreTailer tailer = (StoreTailer) reader.createTailer()) {
            appender.writeText("first");
            long firstPosition = appender.store.writePosition();
            for (int i = 1; i < RECORDS; i++)
                appender.writeText("record-" + i);
            long lastPosition = appender.store.writePosition();
            tailer.toStart();
            CountingContext counter = new CountingContext(tailer);
            assertEquals(RECORDS - 1, tailer.store.lastSequenceNumber(counter));
            counter.headersRead = 0;
            assertEquals(ScanResult.FOUND,
                    tailer.store.indexing.linearScanTo(RECORDS - 1, 0, counter, firstPosition));
            assertEquals(lastPosition, tailer.wire().bytes().readPosition());
            assertTrue("A warmed index lookup rescanned " + counter.headersRead + " headers", counter.headersRead <= 2);
        }
    }

    @Test
    public void repeatedPublicCountQueriesReuseTheMappedFileAnchor() throws Exception {
        AtomicInteger headers = new AtomicInteger();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreAppender appender = (StoreAppender) queue.createAppender();
             StoreTailer tailer = new StoreTailer(queue, queue.pool) {
                 @Override
                 public Wire wireForIndex() {
                     return counted(super.wireForIndex(), headers::incrementAndGet);
                 }
             }) {
            for (int i = 0; i < RECORDS; i++)
                appender.writeText("record-" + i);
            for (int i = 0; i < 3; i++) {
                headers.set(0);
                assertEquals(RECORDS + i, tailer.excerptsInCycle(0));
                assertNull("the public query releases its store", tailer.store);
                assertTrue("reacquired store read " + headers.get() + " headers", headers.get() <= 4);
                appender.writeText("next-" + i);
            }
        }
    }

    @Test
    public void scanAnchorsDoNotSurviveReplacementOfTheMappedFile() throws Exception {
        File directory = getTmpDir();
        MappedFile oldMapping;
        File cycleFile;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreAppender appender = (StoreAppender) queue.createAppender()) {
            for (int i = 0; i < 1000; i++)
                appender.writeText("old-" + i);
            oldMapping = ((MappedBytes) appender.wire().bytes()).mappedFile();
            cycleFile = appender.store.file();
        }
        BackgroundResourceReleaser.releasePendingResources();
        Files.delete(cycleFile.toPath());
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory)
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreAppender appender = (StoreAppender) queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("replacement");
            assertNotSame(oldMapping, ((MappedBytes) appender.wire().bytes()).mappedFile());
            assertEquals(cycleFile, appender.store.file());
            assertEquals(0, appender.store.lastSequenceNumber(appender));
            assertEquals(1, tailer.excerptsInCycle(0));
            assertEquals("replacement", tailer.toStart().readText());
            assertNull(tailer.readText());
        }
    }

    private void assertDocumentWrites(int writers) throws Exception {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreAppender first = (StoreAppender) queue.createAppender();
             StoreAppender second = (StoreAppender) queue.createAppender()) {
            for (int i = 0; i < RECORDS; i++)
                first.writeText("record-" + i);
            StoreAppender[] appenders = writers == 1 ? new StoreAppender[]{first} : new StoreAppender[]{first, second};
            // Establish each writer's store before measuring steady-state document opening.
            for (StoreAppender appender : appenders)
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write().text("warmup");
                }
            Field field = StoreAppender.class.getDeclaredField("wireForIndex");
            field.setAccessible(true);
            for (int i = 0; i < 6; i++) {
                StoreAppender appender = appenders[i % writers];
                Wire original = appender.wireForIndex();
                CountingContext counter = new CountingContext(appender);
                field.set(appender, counter.wireForIndex());
                try {
                    try (DocumentContext dc = appender.writingDocument()) {
                        dc.wire().write().text("measured-" + i);
                    }
                    assertEquals(RECORDS + writers + i, queue.rollCycle().toSequenceNumber(appender.lastIndexAppended()));
                    assertTrue(writers + " appender(s): document write read " + counter.headersRead + " headers",
                            counter.headersRead <= 8);
                } finally {
                    field.set(appender, original);
                }
            }
        }
    }

    @Test
    public void finitePositionWithStaleAliasedTrackerReturnsThePhysicalSequence() throws Exception {
        assertAdditionalStaleTrackerPath(false);
    }

    @Test
    public void forwardToEndWithStaleAliasedTrackerUsesThePhysicalTail() throws Exception {
        assertAdditionalStaleTrackerPath(true);
    }

    private void assertAdditionalStaleTrackerPath(boolean forwardToEnd) throws Exception {
        final long aliasPeriod = 65536;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(1 << 20).timeProvider(() -> 0L).build();
             StoreAppender appender = (StoreAppender) queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("first");
            appender.writeBytes(bytes -> bytes.writeSkip(aliasPeriod - 4));
            long previousPosition = appender.store.writePosition();
            appender.writeText("last");
            long lastPosition = appender.store.writePosition();
            assertEquals(aliasPeriod, lastPosition - previousPosition);
            // Independent physical-record oracle before installing real codec residue.
            assertEquals("first", tailer.readText());
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(aliasPeriod - 4, dc.wire().bytes().readRemaining());
            }
            assertEquals("last", tailer.readText());
            appender.store.indexing.sequence.setSequence(1, previousPosition);
            try {
                assertEquals(1, appender.store.indexing.sequence.getSequence(lastPosition));
                if (forwardToEnd) {
                    tailer.toEnd();
                    assertEquals(queue.rollCycle().toIndex(0, 3), tailer.index());
                    assertEquals(null, tailer.readText());
                } else {
                    for (boolean inclusive : new boolean[]{false, true})
                        assertEquals(inclusive ? 2 : 1,
                                appender.store.sequenceForPosition(appender, lastPosition, inclusive));
                }
            } finally {
                appender.store.indexing.sequence.setSequence(2, lastPosition);
            }
        }
    }

    private static int cycleShift(SingleChronicleQueue queue) {
        int indexCount = queue.indexCount();
        int indexSpacing = queue.indexSpacing();
        return Math.max(32, net.openhft.chronicle.core.Maths.intLog2(indexCount) * 2
                + net.openhft.chronicle.core.Maths.intLog2(indexSpacing));
    }

    private static final class CountingContext implements ExcerptContext {
        private final Wire indexWire;
        private final Wire dataWire;
        int headersRead;

        CountingContext(ExcerptContext context) {
            indexWire = counted(context.wireForIndex(), () -> headersRead++);
            dataWire = counted(context.wire(), () -> headersRead++);
        }

        public Wire wire() {
            return dataWire;
        }

        public Wire wireForIndex() {
            return indexWire;
        }

        public long timeoutMS() {
            return 1000;
        }
    }

    private static Wire counted(Wire delegate, Runnable onHeader) {
        if (delegate == null)
            return null;
        return (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("readDataHeader"))
                        onHeader.run();
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
