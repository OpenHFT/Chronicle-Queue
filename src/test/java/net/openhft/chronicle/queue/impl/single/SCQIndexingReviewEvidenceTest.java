/*
 * Copyright 2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Review evidence for PR 1706, SCQIndexing.java lines 1036 to 1046.
 * The alias bound in lastSequenceNumber rejects the tracker for every position at or above
 * 2^(64 - cycleShift). For the large and sparse roll cycles the bound is 16 MiB or less, so the
 * tail lookup falls back to a linear scan from the last index entry. On the base commit the
 * tracker answers with two header reads. Each test counts the headers that the lookup reads.
 * <p>
 * The third test checks SCQIndexing.java lines 508 to 513, linearScan0, which the PR does not
 * change. It trusts the tracker pair with no alias bound, so a stale aliased tracker moves the
 * tailer to the wrong record.
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

    private void assertTrackerLookup(RollCycle rollCycle) throws Exception {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .rollCycle(rollCycle).blockSize(32 << 20).timeProvider(() -> 0L).build();
             ExcerptAppender appender = queue.createAppender();
             StoreTailer tailer = (StoreTailer) queue.createTailer()) {
            long aliasPeriod = 1L << (64 - cycleShift(queue));
            if (aliasPeriod > (16 << 20))
                throw new AssertionError("this test needs a cycle with an alias period of at most 16 MiB");
            // Push the write position past the alias period with one large record, then add small ones.
            appender.writeBytes(bytes -> bytes.writeSkip(aliasPeriod));
            for (int i = 1; i < RECORDS; i++)
                appender.writeText("record-" + i);
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

    private static int cycleShift(SingleChronicleQueue queue) {
        int indexCount = queue.indexCount();
        int indexSpacing = queue.indexSpacing();
        return Math.max(32, net.openhft.chronicle.core.Maths.intLog2(indexCount) * 2
                + net.openhft.chronicle.core.Maths.intLog2(indexSpacing));
    }

    private static final class CountingContext implements ExcerptContext {
        private final StoreTailer tailer;
        private final Wire indexWire;
        int headersRead;

        CountingContext(StoreTailer tailer) {
            this.tailer = tailer;
            Wire delegate = tailer.wireForIndex();
            indexWire = (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("readDataHeader"))
                            headersRead++;
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        public Wire wire() {
            return tailer.wire();
        }

        public Wire wireForIndex() {
            return indexWire;
        }

        public long timeoutMS() {
            return 1000;
        }
    }
}
