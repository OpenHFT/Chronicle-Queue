/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.DocumentWritten;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
        SystemTimeProvider.CLOCK = timeProvider;
    }

    @After
    public void resetSystemTimeProvider() {
        SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
    }

    @Test
    public void writesContextBeforeDataAndAllowsRetainingWriter() {
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
    public void indexedWriteDoesNotNotifyButStartsAppender() {
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
            assertThrows(IllegalStateException.class,
                    () -> appender.contextListener(Events.class, writer -> { }));
        }
    }

    @Test
    public void notifiesEachAppenderWithItsOwnContextOncePerRoll() {
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
    public void listenerFailureIsNotRetriedInTheSameRoll() {
        AtomicInteger callbacks = new AtomicInteger();

        try (ChronicleQueue queue = builder(getTmpDir())
                .contextListener(Events.class, writer -> {
                    callbacks.incrementAndGet();
                    throw new IllegalStateException("boom");
                })
                .build()) {
            StoreAppender appender = (StoreAppender) queue.createAppender();

            assertThrows(IllegalStateException.class,
                    () -> appender.writeMessage("message", "first"));
            appender.writeMessage("message", "second");
            assertEquals(1, callbacks.get());
        }
    }

    @Test
    public void listenerCanHoldOneDocumentWhileWritingContext() {
        AtomicBoolean outerDocumentRemainedOpen = new AtomicBoolean();
        AtomicLong contextCount = new AtomicLong();

        try (ChronicleQueue queue = builder(getTmpDir())
                .contextListener(ProgressiveEvents.class, writer -> {
                    try (DocumentContext document = writer.writingDocument()) {
                        contextCount.set(document.contextCount());
                        writer.context(new ServiceContext("queue"));
                        outerDocumentRemainedOpen.set(document.isNotComplete());
                    }
                })
                .build()) {
            long expectedCycle = ((SingleChronicleQueue) queue).cycle();
            queue.createAppender().writeMessage("message", "one");

            assertEquals(expectedCycle, contextCount.get());
            assertTrue(outerDocumentRemainedOpen.get());
        }
    }

    @Test
    public void listenerClosePreservesNestingForDocumentWrite() {
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
    public void listenerRemainsCallerOwned() {
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
        assertThrows(UnsupportedOperationException.class, () -> builder(getTmpDir())
                .doubleBuffer(true)
                .contextListener(Events.class, writer -> { })
                .build());

        try (ChronicleQueue queue = builder(getTmpDir()).doubleBuffer(true).build()) {
            assertThrows(UnsupportedOperationException.class,
                    () -> queue.createAppender().contextListener(Events.class, writer -> { }));
        }
    }

    private static SingleChronicleQueueBuilder builder(File path) {
        return SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY);
    }

    private static Bytes<?> binaryPayload(String value) {
        Bytes<?> payload = Bytes.allocateElasticOnHeap();
        Wire wire = WireType.BINARY.apply(payload);
        wire.write("message").text(value);
        return payload;
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

    private static String dump(File path) {
        try (ChronicleQueue queue = builder(path).build()) {
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
