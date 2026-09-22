/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class SequenceLookupRegressionTest extends QueueTestCommon {
    @Test
    public void staleReaderAnchorMustNotPrecedeNewerSparseIndex() throws Exception {
        assertBurstLookup(false);
    }

    @Test
    public void forwardEndPrefersNewerIndexAfterWriterBurst() throws Exception {
        assertBurstLookup(true);
    }

    private void assertBurstLookup(boolean forwardEnd) throws Exception {
        try (SingleChronicleQueue writer = queue(LargeRollCycles.HUGE_DAILY);
             StoreAppender appender = (StoreAppender) writer.createAppender();
             SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(writer.file())
                     .rollCycle(LargeRollCycles.HUGE_DAILY).blockSize(32 << 20).timeProvider(() -> 0L).build();
             CountingTailer tailer = new CountingTailer(reader)) {
            appender.writeBytes(bytes -> bytes.writeSkip(16 << 20));
            tailer.toStart();
            assertEquals(0, tailer.store.lastSequenceNumber(tailer));
            for (int i = 1; i <= 5_000; i++)
                appender.writeText("message-" + i);
            if (forwardEnd) {
                tailer.headers = 0;
                tailer.toEnd();
                assertEquals(reader.rollCycle().toIndex(0, 5_001), tailer.index());
                assertTrue("toEnd ignored a newer sparse entry: " + tailer.headers,
                        tailer.headers <= reader.indexSpacing() + 4);
                return;
            }
            Counter counter = new Counter(tailer);
            assertEquals(5_000, tailer.store.lastSequenceNumber(counter));
            System.out.println("reader after writer burst: " + counter.headers + " headers");
            assertTrue("old cached position read " + counter.headers + " headers despite newer sparse entries",
                    counter.headers <= reader.indexSpacing() + 1);
            counter.headers = 0;
            assertEquals(5_000, tailer.store.lastSequenceNumber(counter));
            assertTrue("recovered pair must remain reusable", counter.headers <= 2);
        }
    }

    @Test
    public void forwardToEndHasBoundedWorkAfterCacheWarmup() throws Exception {
        try (SingleChronicleQueue queue = queue(SparseRollCycles.HUGE_DAILY_XSPARSE);
             StoreAppender appender = (StoreAppender) queue.createAppender();
             CountingTailer tailer = new CountingTailer(queue)) {
            for (int i = 0; i < 5_000; i++)
                appender.writeText("message-" + i);
            for (int i = 0; i < 3; i++) {
                tailer.headers = 0;
                tailer.toEnd();
                assertEquals(queue.rollCycle().toIndex(0, 5_000 + i), tailer.index());
                System.out.println("warm forward toEnd: " + tailer.headers + " headers");
                assertTrue("forward toEnd read " + tailer.headers + " headers", tailer.headers <= 3);
                assertNull(tailer.readText());
                appender.writeText("new-" + i);
                assertEquals("new-" + i, tailer.readText());
            }
        }
    }

    @Test
    public void forwardEndRetainsTheLastCommittedSuffixPosition() throws Exception {
        try (SingleChronicleQueue writer = queue(SparseRollCycles.HUGE_DAILY_XSPARSE);
             StoreAppender appender = (StoreAppender) writer.createAppender();
             SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(writer.file())
                     .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(32 << 20).timeProvider(() -> 0L).build();
             CountingTailer tailer = new CountingTailer(reader)) {
            appender.writeText("first");
            tailer.toStart();
            assertEquals(0, tailer.store.lastSequenceNumber(tailer));
            for (int i = 1; i <= 500; i++) {
                appender.writeText("message-" + i);
                if (i % 10 == 0)
                    try (DocumentContext dc = appender.writingDocument(true)) {
                        dc.wire().bytes().writeLong(i);
                    }
            }
            assertEquals(501, tailer.store.moveToEndForRead(tailer.privateWire()));
            tailer.headers = 0;
            assertEquals(501, tailer.store.moveToEndForRead(tailer.privateWire()));
            // The last data record, trailing metadata and terminating header still need reading.
            assertTrue("repeated forward end rescanned the suffix: " + tailer.headers, tailer.headers <= 3);
        }
    }

    @Test
    public void rollbackAndMetadataDoNotPoisonTheAnchor() throws Exception {
        for (RollCycle cycle : new RollCycle[]{RollCycles.FAST_DAILY, SparseRollCycles.HUGE_DAILY_XSPARSE}) {
            try (SingleChronicleQueue queue = queue(cycle);
                 StoreAppender appender = (StoreAppender) queue.createAppender();
                 ExcerptTailer tailer = queue.createTailer()) {
                appender.writeText("first");
                assertEquals(0, appender.store.lastSequenceNumber(appender));
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write().text("rolled back");
                    dc.rollbackOnClose();
                }
                try (DocumentContext dc = appender.writingDocument(true)) {
                    dc.wire().write().text("metadata");
                }
                assertEquals(0, appender.store.lastSequenceNumber(appender));
                appender.writeText("second");
                assertEquals(1, appender.store.lastSequenceNumber(appender));
                assertEquals("first", tailer.readText());
                assertEquals("second", tailer.readText());
                assertNull(tailer.readText());
                assertEquals(queue.rollCycle().toIndex(0, 1), queue.lastIndex());
            }
        }
    }

    @Test
    public void concurrentWriterAndIndependentReaderKeepPhysicalSequences() throws Exception {
        final int records = 5_000;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch firstWrite = new CountDownLatch(1);
        CountDownLatch firstQuery = new CountDownLatch(1);
        AtomicReference<long[]> published = new AtomicReference<>();
        try (SingleChronicleQueue writer = queue(SparseRollCycles.HUGE_DAILY_XSPARSE);
             SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(writer.file())
                     .rollCycle(SparseRollCycles.HUGE_DAILY_XSPARSE).blockSize(32 << 20).timeProvider(() -> 0L).build()) {
            Future<?> writes = executor.submit(() -> {
                try (StoreAppender appender = (StoreAppender) writer.createAppender()) {
                    for (int i = 0; i < records; i++) {
                        if ((i & 63) == 0) {
                            try (DocumentContext dc = appender.writingDocument(true)) {
                                dc.wire().bytes().writeLong(-1);
                            }
                            try (DocumentContext dc = appender.writingDocument()) {
                                dc.wire().bytes().writeLong(-2);
                                dc.rollbackOnClose();
                            }
                        }
                        try (DocumentContext dc = appender.writingDocument()) {
                            dc.wire().bytes().writeLong(i);
                        }
                        published.set(new long[]{i, appender.store.writePosition()});
                        if (i == 0) {
                            firstWrite.countDown();
                            assertTrue(firstQuery.await(20, TimeUnit.SECONDS));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            Future<?> reads = executor.submit(() -> {
                try {
                    assertTrue(firstWrite.await(20, TimeUnit.SECONDS));
                    try (StoreTailer probe = (StoreTailer) reader.createTailer();
                         ExcerptTailer stream = reader.createTailer()) {
                        assertEquals(1, probe.excerptsInCycle(0));
                        firstQuery.countDown();
                        for (int expected = 0; expected < records;) {
                            try (DocumentContext dc = stream.readingDocument()) {
                                if (!dc.isPresent()) {
                                    Thread.yield();
                                    continue;
                                }
                                assertEquals(expected, reader.rollCycle().toSequenceNumber(dc.index()));
                                assertEquals(expected, dc.wire().bytes().readLong());
                                expected++;
                            }
                            if ((expected & 63) == 0) {
                                long[] sample = published.get();
                                long count = probe.excerptsInCycle(0);
                                assertTrue(count >= sample[0] + 1 && count <= records);
                                probe.toEnd();
                                long end = reader.rollCycle().toSequenceNumber(probe.index());
                                assertTrue(end >= sample[0] + 1 && end <= records);
                                assertEquals(sample[0], probe.store.sequenceForPosition(probe, sample[1], true));
                            }
                        }
                        assertNull(stream.readText());
                        assertEquals(records, probe.excerptsInCycle(0));
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    firstQuery.countDown();
                }
            });
            try {
                writes.get(30, TimeUnit.SECONDS);
                reads.get(30, TimeUnit.SECONDS);
            } finally {
                firstQuery.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private SingleChronicleQueue queue(RollCycle cycle) {
        return SingleChronicleQueueBuilder.binary(getTmpDir()).rollCycle(cycle)
                .blockSize(32 << 20).timeProvider(() -> 0L).build();
    }

    private static Wire counted(Wire wire, Runnable onHeader) {
        if (wire == null)
            return null;
        return (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("readDataHeader"))
                        onHeader.run();
                    try {
                        return method.invoke(wire, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static final class Counter implements ExcerptContext {
        private final Wire wire;
        private final Wire indexWire;
        long headers;

        Counter(ExcerptContext context) {
            wire = counted(context.wire(), () -> headers++);
            indexWire = counted(context.wireForIndex(), () -> headers++);
        }

        public Wire wire() { return wire; }
        public Wire wireForIndex() { return indexWire; }
        public long timeoutMS() { return 1_000; }
    }

    private static final class CountingTailer extends StoreTailer {
        long headers;

        CountingTailer(SingleChronicleQueue queue) {
            super(queue, queue.pool);
        }

        @Override
        public Wire privateWire() {
            Wire original = super.privateWire();
            if (original == null)
                return null;
            Bytes<?> bytes = original.bytes();
            Bytes<?> countedBytes = (Bytes<?>) Proxy.newProxyInstance(Bytes.class.getClassLoader(),
                    new Class<?>[]{Bytes.class}, (proxy, method, args) -> {
                        if (method.getName().equals("readVolatileInt") || method.getName().equals("peekVolatileInt"))
                            headers++;
                        try {
                            return method.invoke(bytes, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            return (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("bytes"))
                            return countedBytes;
                        if (method.getName().equals("readDataHeader"))
                            headers++;
                        try {
                            return method.invoke(original, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        @Override
        public Wire wireForIndex() {
            return counted(super.wireForIndex(), () -> headers++);
        }
    }
}
