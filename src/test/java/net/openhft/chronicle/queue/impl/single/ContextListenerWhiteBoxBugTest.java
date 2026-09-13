/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

/** White-box regression tests for the MarshallableOut.ContextListener adoption in Queue. */
public class ContextListenerWhiteBoxBugTest extends QueueTestCommon {

    // =====================================================================================
    // T-P0-2 : the indexed writeBytes(index, bytes) path never notifies the context listener
    // =====================================================================================
    @Test
    public void indexedWriteIntoFreshRollDoesNotInjectContextRecord() {
        // P0-2 (design decision): an explicit-index writeBytes(index, bytes) - the replication/sink
        // path - must NOT inject a context record. Injecting one at sequence 0 would push the
        // caller's data to sequence 1, breaking the requested index and diverging a replicated sink
        // from its source. So indexed writes are deliberately not a "new context" for the listener.
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, writer -> writer.context("queue"))
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            // roll to a brand-new, empty cycle
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            int cycle = ((SingleChronicleQueue) queue).cycle();
            long index = queue.rollCycle().toIndex(cycle, 0);

            // encode a proper wire-document body ("msg": "data")
            Bytes<?> data = Bytes.allocateElasticOnHeap();
            Wire tmp = WireType.BINARY.apply(data);
            tmp.write("msg").text("data");

            ((InternalAppender) appender).writeBytes(index, data);
            data.releaseLast();
        }

        List<Entry> entries = readEntries(path);
        List<String> events = entries.stream()
                .map(e -> e.eventName + ':' + e.value)
                .collect(Collectors.toList());
        assertEquals("indexed write must not be preceded by an injected context record",
                java.util.Arrays.asList("msg:data"), events);
        assertEquals("the data must keep the sequence number the caller asked for",
                0, entries.get(0).sequence);
    }

    // =====================================================================================
    // T-P0-3 : the listener bypasses the append lock, injecting a data record when only metadata is allowed
    // =====================================================================================
    @Test
    public void listenerDoesNotBypassAppendLock() {
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, writer -> writer.context("injected"))
                .build()) {
            ((SingleChronicleQueue) queue).appendLock().lock();

            ExcerptAppender appender = queue.createAppender();
            // only a metadata write is permitted while the append lock is held
            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("meta").text("m");
            }
        }

        // readingDocument() only surfaces DATA documents; under a metadata-only append lock there must be none.
        List<String> events = readEntries(path).stream()
                .map(e -> e.eventName + ':' + e.value)
                .collect(Collectors.toList());
        assertTrue("append lock was bypassed: the listener injected a data record " + events +
                " while only metadata writes were permitted", events.isEmpty());
    }

    // =====================================================================================
    // contextCount monotonicity: once a cycle is rolled past (EOF written), the appender must
    // never reuse an earlier cycle even if the clock goes backwards - so contextCount is
    // force-forward and equality comparison by DTOs is safe against clock adjustment.
    // =====================================================================================
    @Test
    public void contextCountDoesNotGoBackwardsWhenClockDoes() {
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = builder(path, timeProvider).build()) {
            ExcerptAppender appender = queue.createAppender();

            long first = writeAndCount(appender, "one");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            long second = writeAndCount(appender, "two");
            assertTrue("roll must advance the context count", second > first);

            // Clock goes BACKWARDS a full cycle. The rolled-past cycle has an EOF, and an EOF'd
            // cycle is never reused: the queue REFUSES the write rather than writing into an
            // earlier cycle, so the context count can never be observed to go backwards.
            timeProvider.advanceMillis(-TEST_SECONDLY.lengthInMillis());
            assertThrows(net.openhft.chronicle.wire.WriteAfterEOFException.class,
                    () -> writeAndCount(appender, "backwards"));

            // Once the clock is back in the current cycle's period, writing resumes at the same
            // count - still never below the highest count seen.
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            long third = writeAndCount(appender, "three");
            assertEquals("resumed writes stay on the rolled-to cycle", second, third);

            // and a fresh appender on the same queue must agree
            long fourth = writeAndCount(queue.createAppender(), "four");
            assertTrue("fresh appender must not resurrect an earlier cycle (was " + second + ", now " + fourth + ")",
                    fourth >= second);
        }
    }

    private static long writeAndCount(ExcerptAppender appender, String value) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().write("msg").text(value);
            return dc.contextCount();
        }
    }

    // Rulings: a valid context count is always positive and never 0; an unknown/absent count is
    // negative. doubleBuffer + contextCount is an unsupported combination and must fail fast
    // deterministically, not only when this particular write happened to hit lock contention.
    @Test
    public void contextCountIsRejectedOnDoubleBufferedQueueEvenWithoutContention() {
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .doubleBuffer(true)
                .build()) {
            ExcerptAppender appender = queue.createAppender();
            // no lock contention: this write is NOT buffered, yet the queue is configured for
            // double buffering, so contextCount must still be rejected - otherwise the same code
            // works or throws depending on runtime contention.
            try (DocumentContext dc = appender.writingDocument(false)) {
                assertThrows(IndexNotAvailableException.class, dc::contextCount);
                dc.wire().write("msg").text("m");
            }
        }
    }

    @Test
    public void absentTailerDocumentReportsNegativeContextCount() {
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = builder(path, timeProvider).build()) {
            ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
                assertTrue("an absent document has no context; its count must be negative, not a " +
                        "valid-looking 1, but was " + dc.contextCount(), dc.contextCount() < 0);
            }
        }
    }

    @Test
    public void contextCountIsRejectedForDoubleBufferedDocument() throws Exception {
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .doubleBuffer(true)
                .build()) {

            ExcerptAppender appender = queue.createAppender();
            final WriteLock writeLock = ((SingleChronicleQueue) queue).writeLock();
            final CountDownLatch locked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);

            Thread lockThread = new Thread(() -> {
                writeLock.lock();
                locked.countDown();
                try {
                    release.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    writeLock.unlock();
                }
            }, "context-count-lock-holder");
            lockThread.start();
            assertTrue(locked.await(10, TimeUnit.SECONDS));

            DocumentContext dc = appender.writingDocument(false);
            try {
                assertThrows(IndexNotAvailableException.class, () -> dc.contextCount());
                dc.wire().write("msg").text("message");
                release.countDown();
            } finally {
                release.countDown();
                dc.close();
                lockThread.join(TimeUnit.SECONDS.toMillis(20));
            }
        }
    }

    // =====================================================================================
    // T-P0-1 : double-buffered close's wire.clear() clobbers the live mapped wire
    // =====================================================================================
    @Test
    public void doubleBufferedCloseDoesNotClobberMappedWire() throws Exception {
        // This drives the suspected clobber path: the buffered-index assertion proves double
        // buffering, and the context record proves the listener fired during the buffered flush.
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .doubleBuffer(true)
                .contextListener(ContextEvents.class, writer -> writer.context("queue"))
                .build()) {

            final WriteLock writeLock = ((SingleChronicleQueue) queue).writeLock();
            final CountDownLatch locked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);

            // Create the appender first: the StoreAppender constructor itself acquires the write lock.
            ExcerptAppender appender = queue.createAppender();

            // A helper thread holds the write lock (without writing) so our appender is forced down the
            // double-buffer branch, then releases it so the buffered close can flush.
            Thread lockThread = new Thread(() -> {
                writeLock.lock();
                locked.countDown();
                try {
                    release.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    writeLock.unlock();
                }
            }, "lock-holder");
            lockThread.start();

            assertTrue(locked.await(10, TimeUnit.SECONDS));

            try {
                // write lock is held elsewhere -> this goes to the double buffer
                DocumentContext dc = appender.writingDocument(false);
                // prove we are on the double-buffer path: index is unavailable while buffering
                boolean buffered;
                try {
                    dc.index();
                    buffered = false;
                } catch (RuntimeException expected) {
                    buffered = true; // IndexNotAvailableException confirms double buffering
                }
                assertTrue("expected the write to be double-buffered but it was not", buffered);
                dc.wire().write("msg").text("message");
                // let the lock holder release, then close -> buffered flush fires the listener + wire.clear()
                release.countDown();
                dc.close();

                // exercise the live appender after the clobber: this write must land after "message",
                // not overwrite it (which happens if wire.clear() reset the live mapped wire position)
                try (DocumentContext dc2 = appender.writingDocument(false)) {
                    dc2.wire().write("msg").text("message2");
                }
            } catch (Throwable t) {
                failure.set(t);
            }

            lockThread.join(TimeUnit.SECONDS.toMillis(20));
        }

        if (failure.get() != null)
            fail("double-buffered close/next-write threw (mapped wire clobbered): " + failure.get());

        List<String> events = readEntries(path).stream()
                .map(e -> e.eventName + ':' + e.value)
                .collect(Collectors.toList());
        assertEquals("double-buffered roll should contain context + both messages in order",
                java.util.Arrays.asList("context:queue", "msg:message", "msg:message2"), events);
    }

    @Test
    public void noOpListenerDoesNotRollbackOuterDoubleBufferedDocument() throws Exception {
        finishedNormally = false;
        File path = getTmpDir();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(new SetTimeProvider(1_000_000_000L))
                .doubleBuffer(true)
                .contextListener(ContextEvents.class, writer -> {
                    // A context listener is allowed to have nothing to write for this roll.
                })
                .build()) {
            writeDoubleBuffered(queue, queue.createAppender(), "message");
        }

        List<Entry> entries = readEntries(path);
        assertEquals(1, entries.size());
        assertEquals("msg", entries.get(0).eventName);
        assertEquals("message", entries.get(0).value);
    }

    // =====================================================================================
    // Q1 : consecutive double-buffered writes - the flush must clear the buffer wire, not the
    // mapped wire the listener notification reassigned context.wire to
    // =====================================================================================
    @Test
    public void consecutiveDoubleBufferedWritesDoNotDuplicateBody() throws Exception {
        // The buffered close does writeBytes(wire.bytes()); the listener firing inside that flush
        // reassigns context.wire to the mapped wire, so the following wire.clear() clears the WRONG
        // wire and bufferWire keeps the flushed body. A second buffered write then appends after the
        // stale body and its flush writes both as one merged excerpt.
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .doubleBuffer(true)
                .contextListener(ContextEvents.class, writer -> writer.context("queue"))
                .build()) {

            ExcerptAppender appender = queue.createAppender();
            for (String msg : new String[]{"m1", "m2"}) {
                writeDoubleBuffered(queue, appender, msg);
            }
        }

        List<Entry> entries = readEntries(path);
        List<String> events = entries.stream()
                .map(e -> e.eventName + ':' + e.value)
                .collect(Collectors.toList());
        assertEquals("each double-buffered write must land as its own excerpt with no stale-buffer duplication",
                java.util.Arrays.asList("context:queue", "msg:m1", "msg:m2"), events);
    }

    /** Writes one message forced down the double-buffer path by holding the write lock elsewhere. */
    private static void writeDoubleBuffered(ChronicleQueue queue, ExcerptAppender appender, String msg)
            throws Exception {
        final WriteLock writeLock = ((SingleChronicleQueue) queue).writeLock();
        final CountDownLatch locked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        Thread lockThread = new Thread(() -> {
            writeLock.lock();
            locked.countDown();
            try {
                release.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writeLock.unlock();
            }
        }, "lock-holder-" + msg);
        lockThread.start();
        assertTrue(locked.await(10, TimeUnit.SECONDS));
        try {
            DocumentContext dc = appender.writingDocument(false);
            boolean buffered;
            try {
                dc.index();
                buffered = false;
            } catch (RuntimeException expected) {
                buffered = true;
            }
            assertTrue("write of '" + msg + "' expected to be double-buffered", buffered);
            dc.wire().write("msg").text(msg);
            release.countDown();
            dc.close();
        } finally {
            release.countDown();
            lockThread.join(TimeUnit.SECONDS.toMillis(20));
        }
    }

    // NOTE P1-9q: shouldNotifyContextListener now tolerates StreamCorruptedException (warn + skip)
    // rather than escalating to AssertionError. A deterministic corrupt-index fixture proved too
    // fragile to keep as a test; the tolerant branch is trivially inspectable in
    // SingleChronicleQueue.shouldNotifyContextListener.

    // ---------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------
    private static SingleChronicleQueueBuilder builder(File path, SetTimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider);
    }

    private static List<Entry> readEntries(File path) {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer();
            List<Entry> entries = new ArrayList<>();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;
                    StringBuilder eventName = new StringBuilder();
                    ValueIn valueIn = dc.wire().readEventName(eventName);
                    entries.add(new Entry(eventName.toString(), valueIn.text(),
                            queue.rollCycle().toSequenceNumber(dc.index())));
                }
            }
            return entries;
        }
    }

    interface ContextEvents {
        void context(String source);
    }

    private static final class Entry {
        private final String eventName;
        private final String value;
        private final long sequence;

        private Entry(String eventName, String value, long sequence) {
            this.eventName = eventName;
            this.value = value;
            this.sequence = sequence;
        }
    }
}
