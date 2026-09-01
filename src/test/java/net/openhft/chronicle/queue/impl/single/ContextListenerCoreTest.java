/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.BufferMode;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.DocumentWritten;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteAfterEOFException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ContextListenerCoreTest extends QueueTestCommon {
    private final SetTimeProvider timeProvider = new SetTimeProvider();

    @Before
    public void useDeterministicSystemTimeProvider() {
        timeProvider.currentTimeNanos(1_000_000_000L);
        ignoreException("Queue context listener failed:");
    }

    @Test
    public void writesContextBeforeDataAndAllowsRetainingWriter() {
        // Mutation-removing beforeDocument() leaves no context and fails this first ordering check.
        File path = getTmpDir();
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<Events> retainedWriter = new AtomicReference<>();

        try (ChronicleQueue queue = builder(path)
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    retainedWriter.set(writer);
                    writer.context(new ServiceContext("queue"));
                })
                .build()) {
            Events events = queue.methodWriter(Events.class);
            events.message(new Message("one"));
            retainedWriter.get().context(new ServiceContext("retained"));
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            events.message(new Message("two"));
        }

        assertEquals(2, callbacks.get());
        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: {\n" +
                "  name: queue\n" +
                "}\n" +
                "# index: 100000001\n" +
                "message: {\n" +
                "  text: one\n" +
                "}\n" +
                "# index: 100000002\n" +
                "context: {\n" +
                "  name: retained\n" +
                "}\n" +
                "# index: 200000000\n" +
                "context: {\n" +
                "  name: queue\n" +
                "}\n" +
                "# index: 200000001\n" +
                "message: {\n" +
                "  text: two\n" +
                "}\n" +
                "# no more messages at 8000000000000000\n", dump(path));
    }

    @Test
    public void writesContextBeforeHeldDataDocument() {
        // Pins one shared, nested listener document while the application context remains open.
        File path = getTmpDir();

        try (SingleChronicleQueue queue = builder(path).build()) {
            ExcerptAppender appender = queue.acquireAppender();
            appender.contextListener(Events.class,
                    writer -> writer.context(new ServiceContext("checkpoint")));
            ProgressiveEvents events = appender.methodWriter(ProgressiveEvents.class);

            writeMessages(events, "one", "two");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writeMessages(events, "three");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: {\n" +
                "  name: checkpoint\n" +
                "}\n" +
                "# index: 100000001\n" +
                "message: {\n" +
                "  text: one\n" +
                "}\n" +
                "message: {\n" +
                "  text: two\n" +
                "}\n" +
                "# index: 200000000\n" +
                "context: {\n" +
                "  name: checkpoint\n" +
                "}\n" +
                "# index: 200000001\n" +
                "message: {\n" +
                "  text: three\n" +
                "}\n" +
                "# no more messages at 8000000000000000\n", dump(path));
    }

    @Test
    public void indexedWriteDoesNotNotifyOrConsumeListenerRegistration() {
        // Distinguishes recovery writes from the first subsequent ordinary listener boundary.
        AtomicInteger callbacks = new AtomicInteger();

        try (ChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> callbacks.incrementAndGet())
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            long index = queue.rollCycle().toIndex(((SingleChronicleQueue) queue).cycle(), 0);
            Bytes<?> payload = binaryPayload("indexed");
            try {
                appender.writeBytes(index, payload);
            } finally {
                payload.releaseLast();
            }

            assertEquals(0, callbacks.get());
            appender.contextListener(Events.class, writer -> callbacks.incrementAndGet());
            appender.writeMessage("message", "ordinary");
            assertEquals(1, callbacks.get());
        }
    }

    @Test
    public void notifiesEachAppenderWithItsOwnContextOncePerRoll() {
        // Mutation of cycle rearming changes callback counts and fails this per-appender matrix.
        File path = getTmpDir();
        AtomicInteger firstCallbacks = new AtomicInteger();
        AtomicInteger secondCallbacks = new AtomicInteger();

        try (ChronicleQueue queue = builder(path).build()) {
            StoreAppender first = (StoreAppender) queue.createAppender()
                    .contextListener(Events.class, writer -> {
                        firstCallbacks.incrementAndGet();
                        writer.context(new ServiceContext("first"));
                    });
            StoreAppender second = (StoreAppender) queue.createAppender()
                    .contextListener(Events.class, writer -> {
                        secondCallbacks.incrementAndGet();
                        writer.context(new ServiceContext("second"));
                    });

            first.writeMessage("message", "one");
            first.writeMessage("message", "two");
            assertEquals(1, firstCallbacks.get());
            assertEquals(0, secondCallbacks.get());
            second.writeMessage("message", "three");
            second.writeMessage("message", "four");
            assertEquals(1, firstCallbacks.get());
            assertEquals(1, secondCallbacks.get());

            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            second.writeMessage("message", "five");
            second.writeMessage("message", "six");
            assertEquals(1, firstCallbacks.get());
            assertEquals(2, secondCallbacks.get());
            first.writeMessage("message", "seven");
            first.writeMessage("message", "eight");
            assertEquals(2, firstCallbacks.get());
            assertEquals(2, secondCallbacks.get());
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: {\n" +
                "  name: first\n" +
                "}\n" +
                "# index: 100000001\n" +
                "message: one\n" +
                "# index: 100000002\n" +
                "message: two\n" +
                "# index: 100000003\n" +
                "context: {\n" +
                "  name: second\n" +
                "}\n" +
                "# index: 100000004\n" +
                "message: three\n" +
                "# index: 100000005\n" +
                "message: four\n" +
                "# index: 200000000\n" +
                "context: {\n" +
                "  name: second\n" +
                "}\n" +
                "# index: 200000001\n" +
                "message: five\n" +
                "# index: 200000002\n" +
                "message: six\n" +
                "# index: 200000003\n" +
                "context: {\n" +
                "  name: first\n" +
                "}\n" +
                "# index: 200000004\n" +
                "message: seven\n" +
                "# index: 200000005\n" +
                "message: eight\n" +
                "# no more messages at 8000000000000000\n", dump(path));
    }

    @Test
    public void listenerFailureAfterWritingContextIsNotRetriedInTheSameRoll() {
        // Mutation-changing FAILED to READY permits the next write and fails the one-attempt count.
        File path = getTmpDir();
        AtomicInteger callbacks = new AtomicInteger();

        try (ChronicleQueue queue = builder(path)
                .contextListener(Events.class, writer -> {
                    if (callbacks.incrementAndGet() == 1) {
                        writer.context(new ServiceContext("partial"));
                        throw new IllegalStateException("boom");
                    }
                    writer.context(new ServiceContext("recovered"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();

            assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "first"));
            assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "blocked"));
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("message", "second");
            assertEquals(2, callbacks.get());
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: {\n" +
                "  name: partial\n" +
                "}\n" +
                "# index: 200000000\n" +
                "context: {\n" +
                "  name: recovered\n" +
                "}\n" +
                "# index: 200000001\n" +
                "message: second\n" +
                "# no more messages at 8000000000000000\n", dump(path));
    }

    @Test(timeout = 5_000)
    public void listenerErrorRollsBackHeldDocumentAndLeavesAppenderUsable() {
        // Pins Throwable cleanup, document rollback, count restoration and later-roll recovery.
        File path = getTmpDir();
        AtomicInteger callbacks = new AtomicInteger();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ProgressiveEvents.class, writer -> {
                    if (callbacks.incrementAndGet() == 1) {
                        DocumentContext document = writer.writingDocument();
                        writer.context(new ServiceContext("partial"));
                        assertTrue(document.isNotComplete());
                        throw new AssertionError("boom");
                    }
                    writer.context(new ServiceContext("recovered"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();

            assertThrows(AssertionError.class,
                    () -> appender.writeMessage("message", "first"));
            assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "blocked"));
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("message", "second");
            assertEquals(2, callbacks.get());
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 200000000\n" +
                "context: {\n" +
                "  name: recovered\n" +
                "}\n" +
                "# index: 200000001\n" +
                "message: second\n" +
                "# no more messages at 8000000000000000\n", dump(path));
    }

    @Test(timeout = 5_000)
    public void appenderWriteFromListenerFailsFast() {
        // Prevents deadlock/reentrancy through the captured appender's ordinary write path.
        File path = getTmpDir();
        AtomicReference<StoreAppender> appenderRef = new AtomicReference<>();
        AtomicInteger callbacks = new AtomicInteger();

        try (ChronicleQueue queue = builder(path)
                .contextListener(Events.class,
                        writer -> {
                            if (callbacks.incrementAndGet() == 1)
                                appenderRef.get().writeMessage("message", "reentrant");
                            else
                                writer.context(new ServiceContext("recovered"));
                        })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appenderRef.set(appender);

            IllegalStateException exception = assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "first"));
            assertTrue(exception.getMessage().contains("supplied method writer"));
            assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "blocked"));
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("message", "second");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 200000000\n" +
                "context: {\n" +
                "  name: recovered\n" +
                "}\n" +
                "# index: 200000001\n" +
                "message: second\n" +
                "# no more messages at 8000000000000000\n", dump(path));
    }

    @Test(timeout = 5_000)
    public void capturedAppenderRollbackFromListenerFailsFast() {
        // Pins rejection before rollback can release the outer write's shared context and lock.
        assertCapturedAppenderMutationFails(StoreAppender::rollbackIfNotComplete);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderNormalisationFromListenerFailsFast() {
        // Pins rejection before normalisation attempts to reacquire the non-reentrant write lock.
        assertCapturedAppenderMutationFails(StoreAppender::normaliseEOFs);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderCloseFromListenerFailsFast() {
        // Pins rejection before callback code can tear down the outer write's appender.
        assertCapturedAppenderMutationFails(StoreAppender::close);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderWireAccessFromListenerFailsFast() {
        // Prevents mutable outer-Wire escape from the supplied listener writer.
        assertCapturedAppenderMutationFails(StoreAppender::wire);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderIndexWireAccessFromListenerFailsFast() {
        // Prevents mutable index-Wire escape while internal index publication uses its private path.
        assertCapturedAppenderMutationFails(StoreAppender::wireForIndex);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderPretouchFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(StoreAppender::pretouch);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderMicroTouchFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(StoreAppender::microTouch);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderBackgroundMicroTouchFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(StoreAppender::bgMicroTouch);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderSyncFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(StoreAppender::sync);
    }

    @Test(timeout = 5_000)
    public void capturedAppenderListenerRegistrationFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(appender ->
                appender.contextListener(Events.class, writer -> { }));
    }

    @Test(timeout = 5_000)
    public void capturedAcquireWritingDocumentFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(appender -> appender.acquireWritingDocument(false));
    }

    @Test(timeout = 5_000)
    public void capturedRawWriteFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(appender -> writeRaw(appender, "recursive"));
    }

    @Test(timeout = 5_000)
    public void capturedExactWriteFromListenerFailsFast() {
        assertCapturedAppenderMutationFails(appender -> writeExact(appender, "recursive"));
    }

    @Test(timeout = 5_000)
    public void appenderCreationFromListenerFailsFast() {
        // Pins the common new-appender guard before a second appender can block on the write lock.
        final AtomicReference<SingleChronicleQueue> queueRef = new AtomicReference<>();

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> queueRef.get().createAppender())
                .build()) {
            queueRef.set(queue);
            final StoreAppender appender = (StoreAppender) queue.createAppender();

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "first"));
            assertTrue(failure.getMessage().contains("supplied method writer"));
            assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "blocked"));
        }
    }

    @Test(timeout = 5_000)
    public void acquireAppenderFromListenerFailsFast() {
        // Pre-populate the thread-local cache so this distinguishes rejection from construction.
        final AtomicReference<SingleChronicleQueue> queueRef = new AtomicReference<>();

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> queueRef.get().acquireAppender())
                .build()) {
            queueRef.set(queue);
            final StoreAppender cached = (StoreAppender) queue.acquireAppender();

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> cached.writeMessage("message", "first"));
            assertTrue(failure.getMessage().contains("supplied method writer"));
            assertThrows(IllegalStateException.class,
                    () -> cached.writeMessage("message", "blocked"));
        }
    }

    @Test(timeout = 5_000)
    public void queueCloseFromListenerFailsBeforeTeardownAndReleasesTheLock() {
        // Pins close rejection before teardown and verifies normal close after callback unwinding.
        final AtomicReference<SingleChronicleQueue> queueRef = new AtomicReference<>();
        final AtomicInteger callbacks = new AtomicInteger();

        final SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    if (callbacks.incrementAndGet() == 1)
                        queueRef.get().close();
                    else
                        writer.context(new ServiceContext("recovered"));
                })
                .build();
        queueRef.set(queue);
        try {
            final StoreAppender first = (StoreAppender) queue.createAppender();
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> first.writeMessage("message", "first"));
            assertTrue(failure.getMessage().contains("Cannot close a Queue"));
            assertFalse(queue.isClosing());

            try (ExcerptAppender second = queue.createAppender()) {
                second.writeMessage("message", "after-failure");
            }
            assertFalse(queue.isClosing());
        } finally {
            queue.close();
        }
    }

    @Test
    public void metadataDoesNotConsumeAppenderListenerRegistration() {
        // Mutation-removing the metadata exemption makes later listener registration fail here.
        final AtomicInteger callbacks = new AtomicInteger();
        try (SingleChronicleQueue queue = builder(getTmpDir()).build()) {
            final StoreAppender appender = (StoreAppender) queue.createAppender();
            try (DocumentContext metadata = appender.writingDocument(true)) {
                metadata.wire().write("header").text("metadata");
            }

            appender.contextListener(Events.class, writer -> {
                callbacks.incrementAndGet();
                writer.context(new ServiceContext("queue"));
            });
            appender.writeMessage("message", "ordinary");
            assertEquals(1, callbacks.get());
        }
    }

    private void assertCapturedAppenderMutationFails(Consumer<StoreAppender> mutation) {
        final AtomicReference<StoreAppender> appenderRef = new AtomicReference<>();
        final AtomicInteger callbacks = new AtomicInteger();

        try (ChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    if (callbacks.incrementAndGet() == 1)
                        mutation.accept(appenderRef.get());
                    else
                        writer.context(new ServiceContext("recovered"));
                })
                .build()) {
            final StoreAppender first = (StoreAppender) queue.createAppender();
            appenderRef.set(first);

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> first.writeMessage("message", "first"));
            assertTrue(failure.getMessage().contains("supplied method writer"));
            assertFalse(first.isClosed());

            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            try (ExcerptAppender second = queue.createAppender()) {
                second.writeMessage("message", "second");
            }
        }
    }

    @Test
    public void listenerCanHoldOneDocumentWhileWritingContext() {
        // Pins chained/nested supplied-writer semantics within one listener callback.
        AtomicBoolean outerDocumentRemainedOpen = new AtomicBoolean();
        AtomicInteger contextCount = new AtomicInteger();

        try (ChronicleQueue queue = builder(getTmpDir())
                .contextListener(ProgressiveEvents.class, writer -> {
                    try (DocumentContext document = writer.writingDocument()) {
                        contextCount.set(document.contextCount());
                        writer.context(new ServiceContext("queue"));
                        outerDocumentRemainedOpen.set(document.isNotComplete());
                    }
                })
                .build()) {
            int expectedCycle = ((SingleChronicleQueue) queue).cycle();
            queue.createAppender().writeMessage("message", "one");

            assertEquals(expectedCycle, contextCount.get());
            assertTrue(outerDocumentRemainedOpen.get());
        }
    }

    @Test
    public void sealedHighestRollIsResolvedBeforeDocumentListener() {
        // Mutation-removing destination preflight fails before the callback can target the rolled cycle.
        AtomicInteger callbacks = new AtomicInteger();
        AtomicInteger observedContext = new AtomicInteger(-1);
        AtomicReference<StoreAppender> appenderReference = new AtomicReference<>();

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    observedContext.set(appenderReference.get().contextCount());
                    writer.context(new ServiceContext("queue"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appenderReference.set(appender);
            appender.writeMessage("message", "before");
            int sealedCycle = appender.cycle();
            sealCurrentCycle(appender);

            appender.writeMessage("message", "after");

            assertEquals(2, callbacks.get());
            assertEquals(sealedCycle + 1, appender.cycle());
            assertEquals(sealedCycle + 1, appender.contextCount());
            assertEquals(sealedCycle + 1, observedContext.get());
            assertDocumentCycles(queue, sealedCycle, sealedCycle, sealedCycle + 1, sealedCycle + 1);
        }
    }

    @Test
    public void sealedHighestRollIsResolvedBeforeRawListener() {
        // Applies the same QUEUE-146 destination invariant to sequential raw writes.
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<StoreAppender> appenderReference = new AtomicReference<>();

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    assertEquals(appenderReference.get().cycle(), appenderReference.get().contextCount());
                    writer.context(new ServiceContext("queue"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appenderReference.set(appender);
            writeRaw(appender, "before");
            int sealedCycle = appender.cycle();
            sealCurrentCycle(appender);

            writeRaw(appender, "after");

            assertEquals(2, callbacks.get());
            assertEquals(sealedCycle + 1, appender.contextCount());
            assertDocumentCycles(queue, sealedCycle, sealedCycle, sealedCycle + 1, sealedCycle + 1);
        }
    }

    @Test
    public void trailingMetadataIsSkippedBeforeListenerDestination() {
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<StoreAppender> appenderReference = new AtomicReference<>();
        AtomicInteger observedContext = new AtomicInteger(-1);

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    observedContext.set(appenderReference.get().contextCount());
                    writer.context(new ServiceContext("queue"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appenderReference.set(appender);
            appender.writeMessage("message", "before");
            int sealedCycle = appender.cycle();
            try (DocumentContext metadata = appender.writingDocument(true)) {
                metadata.wire().write("header").text("trailing-metadata");
            }
            sealCurrentCycle(appender);

            appender.writeMessage("message", "after");

            assertEquals(2, callbacks.get());
            assertEquals(sealedCycle + 1, observedContext.get());
            assertEquals(sealedCycle + 1, appender.contextCount());
            assertDocumentCycles(queue, sealedCycle, sealedCycle, sealedCycle + 1, sealedCycle + 1);
        }
    }

    @Test
    public void rolledBackClockDoesNotRepeatAnOlderContext() {
        // Pins contextCount to the monotonic ordinary destination rather than rolled-back wall time.
        AtomicInteger callbacks = new AtomicInteger();
        AtomicLong clock = new AtomicLong(1_000L);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder(
                        getTmpDir(), WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(clock::get)
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    writer.context(new ServiceContext("queue"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appender.writeMessage("message", "first");
            clock.addAndGet(2L * TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("message", "high-water");
            int highWater = appender.contextCount();

            clock.set(1_000L);
            appender.writeMessage("message", "clock-rollback");

            assertEquals(2, callbacks.get());
            assertEquals(highWater, appender.contextCount());
            assertEquals(highWater, appender.cycle());
        }
    }

    @Test
    public void exactHistoricalRecoveryDoesNotChangeOrdinaryContext() {
        // Pins exact-index recovery as listener-free and unable to move the ordinary context backwards.
        ignoreException("Exact-index recovery reopened end-of-data");
        AtomicInteger callbacks = new AtomicInteger();

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    writer.context(new ServiceContext("queue"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appender.writeMessage("message", "historical");
            int historicalCycle = appender.contextCount();
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("message", "current");
            int ordinaryContext = appender.contextCount();
            assertTrue(ordinaryContext > historicalCycle);
            assertEquals(2, callbacks.get());

            Bytes<?> recovered = binaryPayload("recovered");
            try {
                long recoveryIndex = queue.rollCycle().toIndex(historicalCycle, 2);
                appender.writeBytes(recoveryIndex, recovered);
            } finally {
                recovered.releaseLast();
            }

            assertEquals(ordinaryContext, appender.contextCount());
            assertEquals(2, callbacks.get());
            appender.writeMessage("message", "following");
            assertEquals(ordinaryContext, appender.contextCount());
            assertEquals(2, callbacks.get());
        }
    }

    @Test
    public void secondEofFailsBeforeListenerStateChanges() {
        // Pins failure before callback state changes when QUEUE-146's one advance also finds EOF.
        AtomicInteger callbacks = new AtomicInteger();

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    writer.context(new ServiceContext("queue"));
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            appender.writeMessage("message", "before");
            int sealedCycle = appender.cycle();
            sealCurrentCycle(appender);
            sealCycle(queue, sealedCycle + 1);
            queue.tableStoreAcquire("listing.highestCycle", sealedCycle)
                    .setVolatileValue(sealedCycle);

            assertThrows(WriteAfterEOFException.class,
                    () -> appender.writeMessage("message", "must-fail"));
            assertEquals(1, callbacks.get());
            assertEquals(sealedCycle, appender.contextCount());
        }
    }

    @Test
    public void secondEofLeavesListenerRegistrationOpen() {
        AtomicInteger callbacks = new AtomicInteger();

        try (SingleChronicleQueue queue = builder(getTmpDir()).build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            int sealedCycle = queue.cycle();
            sealCycle(queue, sealedCycle);
            sealCycle(queue, sealedCycle + 1);
            queue.tableStoreAcquire("listing.highestCycle", sealedCycle)
                    .setVolatileValue(sealedCycle);

            assertThrows(WriteAfterEOFException.class,
                    () -> appender.writeMessage("message", "must-fail"));
            appender.contextListener(Events.class, writer -> {
                callbacks.incrementAndGet();
                writer.context(new ServiceContext("queue"));
            });

            timeProvider.advanceMillis(2L * TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("message", "accepted");
            assertEquals(1, callbacks.get());
        }
    }

    @Test
    public void unclosedListenerDocumentPoisonsOnlyTheCurrentRoll() {
        // Mutation-removing the open-document check changes the failure and leaves invalid header state.
        AtomicInteger callbacks = new AtomicInteger();

        try (SingleChronicleQueue queue = builder(getTmpDir())
                .contextListener(ProgressiveEvents.class, writer -> {
                    if (callbacks.incrementAndGet() == 1) {
                        writer.writingDocument();
                        writer.context(new ServiceContext("abandoned"));
                    } else {
                        writer.context(new ServiceContext("recovered"));
                    }
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            IllegalStateException abandoned = assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "first"));
            assertTrue(abandoned.getMessage().contains("unclosed document"));
            assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "blocked"));

            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("message", "second");
            assertEquals(2, callbacks.get());
        }
    }

    @Test(timeout = 5_000)
    public void secondAppenderReentryFailsImmediatelyAndReleasesTheQueueLock() {
        // Pins same-instance reentry rejection and subsequent lock usability.
        AtomicReference<StoreAppender> secondReference = new AtomicReference<>();

        try (SingleChronicleQueue queue = builder(getTmpDir()).build()) {
            StoreAppender first = (StoreAppender) queue.createAppender();
            StoreAppender second = (StoreAppender) queue.createAppender();
            secondReference.set(second);
            first.contextListener(Events.class,
                    writer -> secondReference.get().writeMessage("message", "reentrant"));

            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> first.writeMessage("message", "first"));
            assertTrue(failure.getMessage().contains("supplied method writer"));
            second.writeMessage("message", "after-failure");
            assertThrows(IllegalStateException.class,
                    () -> first.writeMessage("message", "blocked"));
        }
    }

    @Test(timeout = 5_000)
    public void secondQueueInstanceOnSamePathFailsImmediatelyDuringCallback() {
        // Mutation-removing the path guard times out on the cross-instance write lock.
        File path = getTmpDir();
        AtomicReference<SingleChronicleQueue> secondReference = new AtomicReference<>();

        try (SingleChronicleQueue first = builder(path)
                .contextListener(Events.class,
                        writer -> secondReference.get().createAppender()
                                .writeMessage("message", "reentrant"))
                .build();
             SingleChronicleQueue second = builder(path).build()) {
            secondReference.set(second);
            StoreAppender firstAppender = (StoreAppender) first.createAppender();

            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> firstAppender.writeMessage("message", "first"));
            assertTrue(failure.getMessage().contains("supplied method writer"));

            second.createAppender().writeMessage("message", "after-failure");
        }
    }

    @Test(timeout = 5_000)
    public void symlinkAliasCannotBypassCallbackGuard() throws Exception {
        Path real = Files.createDirectories(getTmpDir().toPath().toAbsolutePath());
        Path alias = real.resolveSibling(real.getFileName() + "-alias");
        try {
            Files.createSymbolicLink(alias, real);
        } catch (IOException | UnsupportedOperationException | SecurityException unavailable) {
            org.junit.Assume.assumeNoException(unavailable);
        }
        assertTrue(Files.isSameFile(real, alias));

        AtomicReference<SingleChronicleQueue> aliasQueue = new AtomicReference<>();
        AtomicReference<Throwable> aliasFailure = new AtomicReference<>();
        try (SingleChronicleQueue primary = builder(real.toFile())
                .timeoutMS(100)
                .contextListener(Events.class, writer -> {
                    try {
                        aliasQueue.get().createAppender().writeMessage("message", "reentrant");
                    } catch (Throwable failure) {
                        aliasFailure.set(failure);
                    }
                    writer.context(new ServiceContext("valid-context"));
                })
                .build();
             SingleChronicleQueue second = builder(alias.toFile()).timeoutMS(100).build()) {
            aliasQueue.set(second);

            // Callback isolation is keyed by physical Queue identity, not the spelling used to
            // open it; an alias must fail before touching the cross-process write lock.
            primary.createAppender().writeMessage("message", "application");

            assertTrue(aliasFailure.get() instanceof IllegalStateException);
            assertTrue(aliasFailure.get().getMessage().contains("supplied method writer"));
            second.createAppender().writeMessage("message", "after-callback");
        }
    }

    @Test(timeout = 5_000)
    public void otherThreadUsingSamePathFailsImmediatelyDuringCallback() throws Exception {
        // Pins JVM-wide path isolation rather than a thread-local-only callback guard.
        File path = getTmpDir();
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try (SingleChronicleQueue first = builder(path)
                .contextListener(Events.class, writer -> {
                    callbackStarted.countDown();
                    try {
                        assertTrue(releaseCallback.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while pausing listener", interrupted);
                    }
                    writer.context(new ServiceContext("first"));
                })
                .build();
             SingleChronicleQueue second = builder(path).build()) {
            Future<?> firstWrite = executor.submit(() ->
                    first.createAppender().writeMessage("message", "first"));
            assertTrue(callbackStarted.await(3, TimeUnit.SECONDS));

            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> second.createAppender().writeMessage("message", "blocked"));
            assertTrue(failure.getMessage().contains("supplied method writer"));

            releaseCallback.countDown();
            firstWrite.get(3, TimeUnit.SECONDS);
            second.createAppender().writeMessage("message", "after-callback");
        } finally {
            releaseCallback.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void appendLockRejectionDoesNotConsumeListenerRegistration() {
        // Keeps rejected preflight outside the listener's started/attempt lifecycle.
        try (SingleChronicleQueue queue = builder(getTmpDir()).timeoutMS(100).build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            WriteLock appendLock = queue.appendLock();
            appendLock.lock();
            try {
                assertThrows(IllegalStateException.class,
                        () -> appender.writeMessage("message", "blocked"));
            } finally {
                appendLock.unlock();
            }

            appender.contextListener(Events.class,
                    writer -> writer.context(new ServiceContext("queue")));
            appender.writeMessage("message", "allowed");
            assertEquals(appender.cycle(), appender.contextCount());
        }
    }

    @Test
    public void listenerClosePreservesNestingForDocumentWrite() {
        // Pins listener close/commit without consuming the outer application document nesting.
        try (ChronicleQueue queue = builder(getTmpDir())
                .timeoutMS(100)
                .contextListener(Events.class,
                        writer -> writer.context(new ServiceContext("queue")))
                .build()) {
            assertNestedWriteReusesContext((StoreAppender) queue.createAppender());
        }
    }

    @Test
    public void listenerClosePreservesNestingAfterRawWrite() {
        // Pins the same count restoration for sequential raw output.
        try (ChronicleQueue queue = builder(getTmpDir())
                .timeoutMS(100)
                .contextListener(Events.class,
                        writer -> writer.context(new ServiceContext("queue")))
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();
            Bytes<?> payload = binaryPayload("raw");
            try {
                appender.writeBytes(payload);
            } finally {
                payload.releaseLast();
            }

            assertNestedWriteReusesContext(appender);
        }
    }

    @Test
    public void listenerDocumentResetCommitsContextBeforeApplicationData() {
        // Fails if reset clears the holder before committing the active listener document.
        File path = getTmpDir();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ProgressiveEvents.class, writer -> {
                    try (DocumentContext document = writer.writingDocument()) {
                        writer.context(new ServiceContext("reset"));
                        document.reset();
                    }
                })
                .build()) {
            queue.methodWriter(Events.class).message(new Message("application"));
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: {\n" +
                "  name: reset\n" +
                "}\n" +
                "# index: 100000001\n" +
                "message: {\n" +
                "  text: application\n" +
                "}\n" +
                "# no more messages at 8000000000000000\n", dump(path));
    }

    @Test
    public void listenerDocumentResetMakesNestedAutomaticClosesHarmless() {
        // Mutation-removing closesAfterReset produces a spurious no-document-open failure.
        File path = getTmpDir();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ProgressiveEvents.class, writer -> {
                    try (DocumentContext outer = writer.writingDocument();
                         DocumentContext nested = writer.writingDocument()) {
                        assertSame(outer, nested);
                        writer.context(new ServiceContext("nested-reset"));
                        nested.reset();
                    }
                })
                .build()) {
            queue.methodWriter(Events.class).message(new Message("application"));
        }

        assertTrue(dump(path).contains("name: nested-reset"));
        assertTrue(dump(path).contains("text: application"));
    }

    @Test
    public void sequentialResetsPreserveAutomaticClosesForQueueListener() {
        assertSequentialResetsPreserveAutomaticCloses(true);
    }

    @Test
    public void sequentialResetsPreserveAutomaticClosesForAppenderListener() {
        assertSequentialResetsPreserveAutomaticCloses(false);
    }

    @Test
    public void listenerRollbackThenResetPoisonsContextAndAutomaticCloseIsHarmless() {
        // Pins rollback intent across reset while absorbing already-satisfied automatic closes.
        AtomicInteger attempts = new AtomicInteger();

        try (ChronicleQueue queue = builder(getTmpDir())
                .contextListener(ProgressiveEvents.class, writer -> {
                    attempts.incrementAndGet();
                    try (DocumentContext document = writer.writingDocument()) {
                        writer.context(new ServiceContext("rolled-back"));
                        document.rollbackOnClose();
                        document.reset();
                    }
                })
                .build()) {
            Events events = queue.methodWriter(Events.class);
            assertThrows(IllegalStateException.class,
                    () -> events.message(new Message("first")));
            assertThrows(IllegalStateException.class,
                    () -> events.message(new Message("second")));
        }

        assertEquals(1, attempts.get());
    }

    @Test
    public void listenerRemainsCallerOwned() {
        // Guards the ownership contract: Queue retains but never closes the supplied listener.
        CloseableListener listener = new CloseableListener();

        try (ChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, listener)
                .build()) {
            queue.methodWriter(Events.class).message(new Message("one"));
        }

        assertFalse(listener.closed);
    }

    @Test
    public void rejectsDoubleBuffering() {
        // Pins rejection because buffered destination selection occurs after notification time.
        assertThrows(UnsupportedOperationException.class, () -> builder(getTmpDir())
                .doubleBuffer(true)
                .contextListener(Events.class, writer -> { })
                .build());

        try (ChronicleQueue queue = builder(getTmpDir()).doubleBuffer(true).build()) {
            assertThrows(UnsupportedOperationException.class,
                    () -> queue.createAppender().contextListener(Events.class, writer -> { }));
        }
    }

    @Test
    public void doubleBufferedAppenderAndDocumentContextCountsAreUnavailable() {
        try (ChronicleQueue queue = builder(getTmpDir()).doubleBuffer(true).build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();

            assertThrows(IndexNotAvailableException.class, appender::contextCount);
            try (DocumentContext document = appender.writingDocument()) {
                assertThrows(IndexNotAvailableException.class, document::contextCount);
                document.rollbackOnClose();
            }
        }
    }

    @Test
    public void rejectsEveryUnsupportedEffectiveQueueMode() {
        // Directly pins the mode-classification helper; builder integration is covered separately.
        assertThrows(UnsupportedOperationException.class,
                () -> SingleChronicleQueue.validateContextListenerCompatibility(true, false, false));
        assertThrows(UnsupportedOperationException.class,
                () -> SingleChronicleQueue.validateContextListenerCompatibility(false, true, false));
        assertThrows(UnsupportedOperationException.class,
                () -> SingleChronicleQueue.validateContextListenerCompatibility(false, false, true));
    }

    @Test
    public void rejectsEncodedAndEncryptedBuilderModes() {
        assertThrows(UnsupportedOperationException.class, () -> builder(getTmpDir())
                .aesEncryption(new byte[16])
                .contextListener(Events.class, writer -> { })
                .build());

        assertThrows(UnsupportedOperationException.class, () -> builder(getTmpDir())
                .codingSuppliers(() -> (input, output) -> { }, () -> (input, output) -> { })
                .contextListener(Events.class, writer -> { })
                .build());
    }

    @Test
    public void validatesModesLoadedDuringPreBuildAndClosesMetadata() {
        // Pins post-preBuild validation and exceptional metadata cleanup.
        SingleChronicleQueueBuilder delayedMode = new SingleChronicleQueueBuilder() {
            @Override
            protected void preBuild() {
                super.preBuild();
                writeBufferMode(BufferMode.Asynchronous);
            }
        };
        delayedMode.path(getTmpDir())
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider)
                .contextListener(Events.class, writer -> { });

        assertThrows(UnsupportedOperationException.class, delayedMode::build);
        assertTrue(delayedMode.metaStore().isClosed());
    }

    @Test
    public void encodesContextWithQueueWireType() {
        // Distinguishes Queue's configured Wire type from a hard-coded listener format.
        File path = getTmpDir();

        try (ChronicleQueue queue = builder(path, WireType.BINARY_LIGHT)
                .contextListener(Events.class,
                        writer -> writer.context(new ServiceContext("queue")))
                .build()) {
            queue.createAppender().writeMessage("message", "one");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: {\n" +
                "  name: queue\n" +
                "}\n" +
                "# index: 100000001\n" +
                "message: one\n" +
                "# no more messages at 8000000000000000\n", dump(path, WireType.BINARY_LIGHT));
    }

    @Test
    public void tailerDocumentsExposeTheirActualCycle() {
        // Mutation-returning a constant/unavailable count fails for present documents across rolls.
        final File path = getTmpDir();
        try (ChronicleQueue queue = builder(path).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("one");
            appender.writeText("two");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appender.writeText("three");
        }

        assertContextCounts(path, 1, 1, 2);
    }

    private SingleChronicleQueueBuilder builder(File path) {
        return builder(path, WireType.BINARY);
    }

    private SingleChronicleQueueBuilder builder(File path, WireType wireType) {
        return SingleChronicleQueueBuilder.builder(path, wireType)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(timeProvider);
    }

    private static void writeMessages(ProgressiveEvents events, String... messages) {
        try (DocumentContext document = events.writingDocument()) {
            assertTrue(document.isOpen());
            for (String message : messages)
                events.message(new Message(message));
        }
    }

    private void assertSequentialResetsPreserveAutomaticCloses(boolean queueListener) {
        File path = getTmpDir();
        MarshallableOut.ContextListener<ProgressiveEvents> listener = writer -> {
            try (DocumentContext first = writer.writingDocument()) {
                writer.context(new ServiceContext("first"));
                first.reset();
                try (DocumentContext second = writer.writingDocument()) {
                    writer.context(new ServiceContext("second"));
                    second.reset();
                }
            }
        };

        SingleChronicleQueueBuilder builder = builder(path);
        if (queueListener)
            builder.contextListener(ProgressiveEvents.class, listener);
        try (SingleChronicleQueue queue = builder.build()) {
            ExcerptAppender appender = queue.createAppender();
            if (!queueListener)
                appender.contextListener(ProgressiveEvents.class, listener);

            // Each reset satisfies one active document but every enclosing try-with-resources
            // scope still closes later; deferred close accounting must accumulate across resets.
            if (queueListener)
                appender.methodWriter(Events.class).message(new Message("application"));
            else
                appender.writeMessage("message", "application");
        }

        String dump = dump(path);
        assertTrue(dump, dump.indexOf("name: first") < dump.indexOf("name: second"));
        assertTrue(dump, dump.indexOf("name: second") < dump.indexOf("application"));
    }

    private void assertContextCounts(File path, int... expected) {
        try (ChronicleQueue queue = builder(path).build();
             ExcerptTailer tailer = queue.createTailer()) {
            for (int contextCount : expected) {
                try (DocumentContext document = tailer.readingDocument()) {
                    assertTrue(document.isPresent());
                    assertEquals(contextCount, document.contextCount());
                    assertEquals(queue.rollCycle().toCycle(document.index()), document.contextCount());
                }
            }
            try (DocumentContext document = tailer.readingDocument()) {
                assertFalse(document.isPresent());
                assertEquals(-1, document.contextCount());
            }
        }
    }

    private static Bytes<?> binaryPayload(String value) {
        Bytes<?> payload = Bytes.allocateElasticOnHeap();
        Wire wire = WireType.BINARY.apply(payload);
        wire.write("message").text(value);
        return payload;
    }

    private static void writeRaw(StoreAppender appender, String value) {
        Bytes<?> payload = binaryPayload(value);
        try {
            appender.writeBytes(payload);
        } finally {
            payload.releaseLast();
        }
    }

    private static void writeExact(StoreAppender appender, String value) {
        Bytes<?> payload = binaryPayload(value);
        try {
            appender.writeBytes(0L, payload);
        } finally {
            payload.releaseLast();
        }
    }

    private static void assertDocumentCycles(SingleChronicleQueue queue, int... expectedCycles) {
        try (ExcerptTailer tailer = queue.createTailer()) {
            for (int expectedCycle : expectedCycles) {
                try (DocumentContext document = tailer.readingDocument()) {
                    assertTrue(document.isPresent());
                    assertEquals(expectedCycle, queue.rollCycle().toCycle(document.index()));
                }
            }
            try (DocumentContext document = tailer.readingDocument()) {
                assertFalse(document.isPresent());
            }
        }
    }

    private static void sealCurrentCycle(StoreAppender appender) {
        SingleChronicleQueueStore store = appender.store;
        if (store == null)
            throw new AssertionError("Appender has no current store");
        try (MappedBytes bytes = store.bytes()) {
            Wire wire = appender.queue().wireType().apply(bytes);
            wire.usePadding(store.dataVersion() > 0);
            assertTrue(store.writeEOF(wire, appender.queue().timeoutMS));
        }
    }

    private static void sealCycle(SingleChronicleQueue queue, int cycle) {
        try (SingleChronicleQueueStore store = queue.storeForCycle(cycle, queue.epoch(), true, null);
             MappedBytes bytes = store.bytes()) {
            Wire wire = queue.wireType().apply(bytes);
            wire.usePadding(store.dataVersion() > 0);
            assertTrue(store.writeEOF(wire, queue.timeoutMS));
        }
    }

    private static void assertNestedWriteReusesContext(StoreAppender appender) {
        try (DocumentContext outer = appender.writingDocument()) {
            outer.wire().write("outer").text("one");
            try (DocumentContext nested = appender.writingDocument()) {
                assertSame(outer, nested);
                nested.wire().write("nested").text("two");
            }
            assertTrue(outer.isNotComplete());
        }
    }

    private String dump(File path) {
        return dump(path, WireType.BINARY);
    }

    private String dump(File path, WireType wireType) {
        try (ChronicleQueue queue = builder(path, wireType).build()) {
            StringWriter writer = new StringWriter();
            queue.dump(writer, 0, Long.MAX_VALUE);
            return writer.toString();
        }
    }

    interface Events {
        void context(ServiceContext context);

        void message(Message message);
    }

    interface ProgressiveEvents extends Events, DocumentWritten {
    }

    static final class ServiceContext extends SelfDescribingMarshallable {
        private final String name;

        ServiceContext(String name) {
            this.name = name;
        }
    }

    static final class Message extends SelfDescribingMarshallable {
        private final String text;

        Message(String text) {
            this.text = text;
        }
    }

    private static final class CloseableListener
            implements MarshallableOut.ContextListener<Events>, AutoCloseable {
        private boolean closed;

        @Override
        public void onNewContext(Events writer) {
            writer.context(new ServiceContext("queue"));
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
