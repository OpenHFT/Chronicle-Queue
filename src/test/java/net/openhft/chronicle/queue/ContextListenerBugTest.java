/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

/** Regression tests for the MarshallableOut.ContextListener adoption in Queue. */
public class ContextListenerBugTest extends QueueTestCommon {

    // =====================================================================================
    // T-P0-23 : an Error thrown by onNewContext leaks the write lock
    // =====================================================================================
    @Test
    public void listenerErrorDoesNotLeakWriteLock() throws Exception {
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, writer -> {
                    throw new AssertionError("boom");
                })
                .build();
        ExcerptAppender appender = queue.createAppender();

        // The Error must propagate on the first write.
        assertThrows(AssertionError.class, () -> writeMessage(appender, "one"));

        // On another thread, a SECOND queue on the same path must still be able to write.
        // If the write lock leaked, this write blocks until the lock times out (well beyond 2s).
        ExecutorService es = Executors.newSingleThreadExecutor();
        Future<?> future = es.submit(() -> {
            try (ChronicleQueue q2 = builder(path, timeProvider).build()) {
                writeMessage(q2.createAppender(), "two");
            }
        });
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            fail("second queue's write blocked -> write lock leaked by the listener Error");
        } catch (ExecutionException e) {
            // an unrelated failure inside the writer thread - surface it
            throw new AssertionError("second write failed unexpectedly", e.getCause());
        } finally {
            es.shutdownNow();
            Closeable.closeQuietly(appender, queue);
        }
    }

    // =====================================================================================
    // Q2 : an Error from the listener must not wedge the appender (count leak)
    // =====================================================================================
    @Test
    public void appenderUsableAfterListenerError() {
        // The count field is only decremented for RuntimeException; an Error rethrown through
        // prepareAndReturnWriteContext leaks count, so the next write takes the count>1 fast path
        // and is handed a stale, never-opened context (dc.wire() == null).
        finishedNormally = false;
        File path = getTmpDir();
        AtomicInteger attempts = new AtomicInteger();

        try (ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, writer -> {
                    if (attempts.getAndIncrement() == 0)
                        throw new AssertionError("boom");
                    writer.context("retry");
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            assertThrows(AssertionError.class, () -> writeMessage(appender, "one"));
            // the SAME appender must still be usable after the Error
            writeMessage(appender, "two");
        }

        List<String> events = readEntries(path).stream()
                .map(e -> e.eventName + ':' + e.value)
                .collect(Collectors.toList());
        assertEquals(java.util.Arrays.asList("context:retry", "msg:two"), events);
    }

    // =====================================================================================
    // Q3 : writeBytes from within the listener must fail fast, not self-deadlock
    // =====================================================================================
    @Test
    public void reentrantWriteBytesInsideListenerFailsFast() {
        // The reentrancy guard covers writingDocument only; writeBytes re-acquires the
        // non-reentrant cross-process write lock the thread already holds, stalling until the
        // lock timeout and then force-unlocking - breaking mutual exclusion mid-append.
        finishedNormally = false;
        File path = getTmpDir();
        ExcerptAppender[] holder = new ExcerptAppender[1];

        try (ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, writer -> {
                    net.openhft.chronicle.bytes.Bytes<?> b = net.openhft.chronicle.bytes.Bytes.from("x");
                    try {
                        holder[0].writeBytes(b);
                    } finally {
                        b.releaseLast();
                    }
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();
            holder[0] = appender;

            long start = System.currentTimeMillis();
            Throwable thrown = null;
            try {
                writeMessage(appender, "one");
            } catch (Throwable t) {
                thrown = t;
            }
            long elapsed = System.currentTimeMillis() - start;

            assertNotNull("re-entrant writeBytes inside the listener must fail", thrown);
            assertTrue("must fail fast with IllegalStateException, not stall on the write lock ("
                            + elapsed + "ms, " + thrown + ")",
                    thrown instanceof IllegalStateException && elapsed < 5_000);
        }
    }

    // =====================================================================================
    // T-P1-6q : a listener that writes then throws leaves a permanent half-context, retry suppressed
    // =====================================================================================
    @Test
    public void partialContextWriteSurfacesErrorAndIsNotDuplicated() {
        // P1-6q: a listener that writes a record then throws cannot be transactionally rolled back
        // (a committed excerpt cannot be un-written), so the contract is at-most-once: the error is
        // surfaced to the caller, the append can proceed on retry, and the already-written record is
        // NOT duplicated by a silent re-run. (Completing a half-written context is out of scope - a
        // context listener must not throw after writing; keep it complete-or-nothing and idempotent.)
        finishedNormally = false;
        File path = getTmpDir();
        AtomicInteger attempts = new AtomicInteger();

        try (ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, (ContextEvents writer) -> {
                    writer.context("a");
                    if (attempts.getAndIncrement() == 0)
                        throw new IllegalStateException("boom");
                    writer.context("b");
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            // the listener failure is surfaced, not swallowed
            assertThrows(IllegalStateException.class, () -> writeMessage(appender, "one"));
            // the append proceeds on retry ...
            writeMessage(appender, "one");
        }

        List<String> events = readEntries(path).stream()
                .map(e -> e.eventName + ':' + e.value)
                .collect(Collectors.toList());
        // ... the data was written, and the context record the failed listener committed is present
        // exactly once (no duplicate from a silent re-run).
        assertTrue("the retried append must be written: " + events, events.contains("msg:one"));
        assertEquals("the committed context record must not be duplicated on retry: " + events,
                1, events.stream().filter("context:a"::equals).count());
    }

    // =====================================================================================
    // T-P1-24 : a listener shared by two appenders is closed early / double-closed
    // =====================================================================================
    @Test
    public void sharedAppenderListenerIsClosedByLastAppender() {
        finishedNormally = false;
        CountingContextListener listener = new CountingContextListener("shared");

        try (ChronicleQueue queue = builder(getTmpDir(), new SetTimeProvider(1_000_000_000L)).build()) {
            ExcerptAppender a1 = queue.createAppender();
            ExcerptAppender a2 = queue.createAppender();
            a1.contextListener(ContextEvents.class, listener);
            a2.contextListener(ContextEvents.class, listener);

            try {
                a1.close();
                assertEquals("closing one appender must not close a listener still used by another", 0, listener.closeCount.get());
                a2.close();
                assertEquals(1, listener.closeCount.get());
            } finally {
                Closeable.closeQuietly(a1, a2);
            }
        }
    }

    // =====================================================================================
    // T-P2-26 : re-entrant writingDocument() from inside onNewContext
    // =====================================================================================
    @Test
    public void reentrantWritingDocumentInsideListener() {
        finishedNormally = false;
        File path = getTmpDir();
        ExcerptAppender[] holder = new ExcerptAppender[1];

        Throwable thrown = null;
        try (ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L)).build()) {
            ExcerptAppender appender = queue.createAppender();
            holder[0] = appender;
            appender.contextListener(ContextEvents.class, (ContextEvents writer) -> {
                // Re-entrant misuse: use the appender directly instead of the supplied writer.
                try (DocumentContext dc = holder[0].writingDocument()) {
                    dc.wire().write("reentrant").text("x");
                }
            });
            try {
                writeMessage(appender, "one");
            } catch (Throwable t) {
                thrown = t;
            }
        }

        // A robust API should fail this misuse deterministically. Instead the re-entrant call is served a
        // stale/half-open context whose wire() is null, producing a confusing NullPointerException, and the
        // triggering message is silently dropped (the roll ends up empty).
        assertNotNull("re-entrant writingDocument() inside onNewContext should fail deterministically", thrown);
        assertFalse("re-entrancy produced a raw NullPointerException from a null/stale context instead of a deterministic error: " + thrown,
                thrown instanceof NullPointerException);
    }

    // =====================================================================================
    // T-P2-12 : non-binary queue mis-encodes the context record (hardcoded Binary handler)
    // =====================================================================================
    @Test
    public void contextRecordEncodedWithTheQueuesWireType() {
        // P2-12: the context method writer must be built for the queue's wire type, not a hardcoded
        // Binary handler. Non-binary text wires (YAML/TEXT) cannot back a queue store at all
        // (SingleChronicleQueueStore needs int128 bindings), so exercise a non-default but
        // queue-valid binary-family type, BINARY_LIGHT, and confirm the record round-trips.
        finishedNormally = false;
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY_LIGHT)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .contextListener(ContextEvents.class, writer -> writer.context("queue"))
                .build();
             ExcerptAppender appender = queue.createAppender()) {
            writeMessage(appender, "one");
        }

        List<String> events = readEntriesWith(path, WireType.BINARY_LIGHT).stream()
                .map(e -> e.eventName + ':' + e.value)
                .collect(Collectors.toList());
        assertEquals(java.util.Arrays.asList("context:queue", "msg:one"), events);
    }

    // =====================================================================================
    // T-P2-17 : ExcerptAppender.contextListener default throws UOE (contract documentation - PASSES)
    // =====================================================================================
    @Test
    public void bareExcerptAppenderContextListenerThrowsUnsupported() {
        // The base ExcerptAppender interface documents the fluent API, but unsupported
        // implementations must still fail clearly through the default method.
        ExcerptAppender bare = new ExcerptAppender() {
            @Override public void writeBytes(net.openhft.chronicle.bytes.BytesStore<?, ?> bytes) { throw new UnsupportedOperationException(); }
            @Override public long lastIndexAppended() { throw new UnsupportedOperationException(); }
            @Override public int cycle() { throw new UnsupportedOperationException(); }
            @Override public Wire wire() { throw new UnsupportedOperationException(); }
            @Override public int sourceId() { throw new UnsupportedOperationException(); }
            @Override public ChronicleQueue queue() { throw new UnsupportedOperationException(); }
            @Override public DocumentContext writingDocument(boolean metaData) { throw new UnsupportedOperationException(); }
            @Override public DocumentContext acquireWritingDocument(boolean metaData) { throw new UnsupportedOperationException(); }
            @Override public void close() { }
            @Override public boolean isClosed() { return false; }
            @Override public void singleThreadedCheckReset() { }
            @Override public void singleThreadedCheckDisabled(boolean disableThreadSafetyCheck) { }
        };

        assertThrows(UnsupportedOperationException.class,
                () -> bare.contextListener(ContextEvents.class, writer -> writer.context("x")));
    }

    // =====================================================================================
    // T-P1-7 : lastCycleChecked visibility race - two appenders racing across rolls (STRESS)
    // =====================================================================================
    @Test
    public void stressTwoAppendersOneContextRecordPerRoll() throws Exception {
        finishedNormally = false;
        final int iterations = Integer.getInteger("contextListener.stress.iterations", 300);
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        // Each appender must stay pinned to a single thread (single-threaded check), so use one
        // dedicated executor per appender and create each appender on its own thread.
        ExecutorService esA = Executors.newSingleThreadExecutor();
        ExecutorService esB = Executors.newSingleThreadExecutor();
        try (ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, writer -> writer.context("ctx"))
                .build()) {

            final ExcerptAppender appenderA = esA.submit(queue::createAppender).get();
            final ExcerptAppender appenderB = esB.submit(queue::createAppender).get();

            for (int i = 0; i < iterations; i++) {
                timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
                final java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(2);
                Future<?> fa = esA.submit(() -> raceWrite(barrier, appenderA, "A"));
                Future<?> fb = esB.submit(() -> raceWrite(barrier, appenderB, "B"));
                fa.get(30, TimeUnit.SECONDS);
                fb.get(30, TimeUnit.SECONDS);
            }
        } finally {
            esA.shutdownNow();
            esB.shutdownNow();
        }

        List<Entry> entries = readEntries(path);
        long contextRecords = entries.stream().filter(e -> e.eventName.equals("context")).count();

        // Every iteration advances the clock by one roll and writes two messages into that fresh cycle,
        // so there are exactly `iterations` rolls and there must be exactly one context record per roll.
        assertEquals("expected exactly one context record per roll (" + iterations +
                        " rolls) but saw " + contextRecords + " -> duplicate context record written for some roll",
                iterations, contextRecords);
    }

    private static void raceWrite(java.util.concurrent.CyclicBarrier barrier, ExcerptAppender appender, String tag) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        writeMessage(appender, tag);
    }

    // ---------------------------------------------------------------------------------------
    // helpers (copied from ContextListenerTest)
    // ---------------------------------------------------------------------------------------
    private static SingleChronicleQueueBuilder builder(File path, SetTimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider);
    }

    private static void writeMessage(ExcerptAppender appender, String value) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().write("msg").text(value);
        }
    }

    private static List<Entry> readEntries(File path) {
        return readEntriesWith(path, WireType.BINARY);
    }

    private static List<Entry> readEntriesWith(File path, WireType wireType) {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, wireType)
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
                    entries.add(new Entry(eventName.toString(), valueIn.text(), queue.rollCycle().toSequenceNumber(dc.index())));
                }
            }
            return entries;
        }
    }

    interface ContextEvents {
        void context(String source);
    }

    private static final class CountingContextListener implements MarshallableOut.ContextListener<ContextEvents>, AutoCloseable {
        private final String source;
        private final AtomicInteger invocationCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        private CountingContextListener(String source) {
            this.source = source;
        }

        @Override
        public void onNewContext(ContextEvents writer) {
            invocationCount.incrementAndGet();
            writer.context(source);
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
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
