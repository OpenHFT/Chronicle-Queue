/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.DocumentWritten;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

public class ContextListenerTest extends QueueTestCommon {
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
    public void builderListenerWritesBeforeFirstDocumentOnFirstUseAndAfterRoll() {
        File path = getTmpDir();
        CountingContextListener listener = new CountingContextListener("queue");

        try (ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, listener)
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            appender.writeMessage("msg", "one");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appender.writeMessage("msg", "two");
        }
        assertEquals("listener should be called on first use and after the first roll", 2,
                listener.invocationCount.get());

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: queue\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# index: 200000000\n" +
                "context: queue\n" +
                "# index: 200000001\n" +
                "msg: two\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void documentContextCountIsQueueRollCycleForWritesAndReads() {
        File path = getTmpDir();
        List<Long> writeCounts = new ArrayList<>();

        try (ChronicleQueue queue = builder(path).build()) {
            ExcerptAppender appender = queue.createAppender();
            writeCounts.add(writeMessageAndContextCount(queue, appender, "one"));
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writeCounts.add(writeMessageAndContextCount(queue, appender, "two"));
        }

        assertNotEquals("roll cycle should change after advancing the test clock",
                writeCounts.get(0), writeCounts.get(1));
        assertEquals(writeCounts, readContextCounts(path));
    }

    @Test
    public void bigDtoIsWrittenOncePerRollUsingTransientLastRollCycle() {
        File path = getTmpDir();
        BigDto bigDto = new BigDto("static");
        long firstRollCycle;
        long sameRollCycle;
        long secondRollCycle;

        try (ChronicleQueue queue = builder(path).build()) {
            BigDtoEvents out = queue.createAppender().methodWriter(BigDtoEvents.class);

            firstRollCycle = writeMessageAssumingContext(out, bigDto, "one");
            sameRollCycle = writeMessageAssumingContext(out, bigDto, "two");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            secondRollCycle = writeMessageAssumingContext(out, bigDto, "three");
        }

        assertEquals(firstRollCycle, sameRollCycle);
        assertNotEquals(firstRollCycle, secondRollCycle);
        assertEquals(secondRollCycle, bigDto.lastRollCycleWrittenTo);
        assertEquals(2, bigDto.writeCount);
        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "bigDto: {\n" +
                "  value: static\n" +
                "}\n" +
                "msg: one\n" +
                "# index: 100000001\n" +
                "msg: two\n" +
                "# index: 200000000\n" +
                "bigDto: {\n" +
                "  value: static\n" +
                "}\n" +
                "msg: three\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void progressiveContextIsWrittenInsideHeldDocumentOnlyWhenMissingForRoll() {
        File path = getTmpDir();
        BigDto bigDto = new BigDto("static");

        try (ChronicleQueue queue = builder(path).build()) {
            BigDtoEvents out = queue.createAppender().methodWriter(BigDtoEvents.class);

            writeMessageAssumingContext(out, bigDto, "one");
            writeMessageAssumingContext(out, bigDto, "two");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writeMessageAssumingContext(out, bigDto, "three");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "bigDto: {\n" +
                "  value: static\n" +
                "}\n" +
                "msg: one\n" +
                "# index: 100000001\n" +
                "msg: two\n" +
                "# index: 200000000\n" +
                "bigDto: {\n" +
                "  value: static\n" +
                "}\n" +
                "msg: three\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void listenerMayWriteNothingOnFirstUse() {
        File path = getTmpDir();
        AtomicInteger invocations = new AtomicInteger();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, writer -> invocations.incrementAndGet())
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            appender.writeMessage("msg", "one");
            appender.writeMessage("msg", "two");
        }

        assertEquals("no-op listener should be called once on first use", 1, invocations.get());
        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "msg: one\n" +
                "# index: 100000001\n" +
                "msg: two\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void messageHistoryIsOnlyWrittenForNaturallyTriggeredMessages() {
        ignoreException("Overriding sourceId from existing metadata, was 0, overriding to 1");
        File path = getTmpDir();

        final MessageHistory previousHistory = MessageHistory.get();
        final VanillaMessageHistory messageHistory = new VanillaMessageHistory();
        messageHistory.addSourceDetails(true);
        messageHistory.historyWallClock(true);
        MessageHistory.set(messageHistory);
        try (ChronicleQueue queue = builder(path)
                .sourceId(1)
                .contextListener(HistoryEvents.class, writer -> writer.context("queue"))
                .build()) {
            HistoryEvents writer = queue.methodWriter(HistoryEvents.class);

            writer.msg("one");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writer.msg("two");
        } finally {
            MessageHistory.set(previousHistory);
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: queue\n" +
                "# index: 100000001\n" +
                "history: {\n" +
                "  sources: [ ],\n" +
                "  timings: [\n" +
                "    1000000000\n" +
                "  ]\n" +
                "}\n" +
                "msg: one\n" +
                "# index: 200000000\n" +
                "context: queue\n" +
                "# index: 200000001\n" +
                "history: {\n" +
                "  sources: [ ],\n" +
                "  timings: [\n" +
                "    2000000000\n" +
                "  ]\n" +
                "}\n" +
                "msg: two\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void builderContextListenerSupplierCreatesOneListenerPerBuiltQueue() {
        File root = getTmpDir();
        File firstPath = new File(root, "first");
        File secondPath = new File(root, "second");
        List<CountingContextListener> listeners = new ArrayList<>();
        AtomicInteger listenerIds = new AtomicInteger();

        SingleChronicleQueueBuilder baseBuilder = builder(firstPath)
                .contextListenerSupplier(ContextEvents.class, () -> {
                    CountingContextListener listener = new CountingContextListener("listener-" + listenerIds.getAndIncrement());
                    listeners.add(listener);
                    return listener;
                });

        try (ChronicleQueue firstQueue = baseBuilder.clone().path(firstPath).build();
             ChronicleQueue secondQueue = baseBuilder.clone().path(secondPath).build()) {
            ExcerptAppender appender1 = firstQueue.createAppender();
            appender1.writeMessage("msg", "one");
            ExcerptAppender appender = secondQueue.createAppender();
            appender.writeMessage("msg", "two");
        }

        assertEquals(2, listeners.size());
        assertNotSame(listeners.get(0), listeners.get(1));
        assertEquals(1, listeners.get(0).invocationCount.get());
        assertEquals(1, listeners.get(1).invocationCount.get());
        assertEquals(1, listeners.get(0).closeCount.get());
        assertEquals(1, listeners.get(1).closeCount.get());
        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: listener-0\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(firstPath));
        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: listener-1\n" +
                "# index: 100000001\n" +
                "msg: two\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(secondPath));
    }

    @Test
    public void reopeningExistingRollDoesNotWriteDuplicateContextRecord() {
        File path = getTmpDir();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, writer -> writer.context("first"))
                .build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeMessage("msg", "one");
        }

        try (ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, writer -> writer.context("second"))
                .build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeMessage("msg", "two");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: first\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# index: 100000002\n" +
                "msg: two\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void appenderListenerOverridesBuilderListener() {
        File path = getTmpDir();
        CountingContextListener builderListener = new CountingContextListener("builder");
        CountingContextListener appenderListener = new CountingContextListener("appender");

        ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, builderListener)
                .build();
        ExcerptAppender appender = queue.createAppender();
        appender.contextListener(ContextEvents.class, appenderListener);

        appender.writeMessage("msg", "one");
        appender.close();
        assertEquals(0, builderListener.closeCount.get());
        assertEquals(1, appenderListener.closeCount.get());
        queue.close();
        assertEquals(1, builderListener.closeCount.get());

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: appender\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
        assertEquals(0, builderListener.invocationCount.get());
        assertEquals(1, appenderListener.invocationCount.get());
    }

    @Test
    public void sameListenerOnBuilderAndAppenderIsClosedOnlyByQueue() {
        File path = getTmpDir();
        CountingContextListener listener = new CountingContextListener("shared");

        ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, listener)
                .build();
        ExcerptAppender appender = queue.createAppender();
        appender.contextListener(ContextEvents.class, listener);

        appender.close();
        assertEquals(0, listener.closeCount.get());
        queue.close();
        assertEquals(1, listener.closeCount.get());
    }

    @Test
    public void builderListenerIsClosedByQueueWithNoAppenders() {
        CountingContextListener listener = new CountingContextListener("builder");
        ChronicleQueue queue = builder(getTmpDir())
                .contextListener(ContextEvents.class, listener)
                .build();

        queue.close();

        assertEquals(1, listener.closeCount.get());
    }

    @Test
    public void replacingAppenderListenerClosesPreviousLocalListener() {
        CountingContextListener first = new CountingContextListener("first");
        CountingContextListener second = new CountingContextListener("second");

        try (ChronicleQueue queue = builder(getTmpDir()).build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.contextListener(ContextEvents.class, first);
            appender.contextListener(ContextEvents.class, second);

            assertEquals(1, first.closeCount.get());
            assertEquals(0, second.closeCount.get());

            appender.close();
            assertEquals(1, second.closeCount.get());
        }
    }

    @Test
    public void invalidAppenderListenerArgumentsDoNotChangeOwnership() {
        CountingContextListener first = new CountingContextListener("first");
        CountingContextListener second = new CountingContextListener("second");

        try (ChronicleQueue queue = builder(getTmpDir()).build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.contextListener(ContextEvents.class, first);

            assertThrows(NullPointerException.class, () -> appender.contextListener(null, second));
            assertThrows(NullPointerException.class, () -> appender.contextListener(ContextEvents.class, null));

            assertEquals(0, first.closeCount.get());
            assertEquals(0, second.closeCount.get());

            appender.close();
            assertEquals(1, first.closeCount.get());
            assertEquals(0, second.closeCount.get());
        }
    }

    @Test
    public void appenderListenerCannotBeChangedAfterFirstWrite() {
        try (ChronicleQueue queue = builder(getTmpDir()).build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeMessage("msg", "one");

            assertThrows(IllegalStateException.class,
                    () -> appender.contextListener(ContextEvents.class, writer -> writer.context("late")));
        }
    }

    @Test
    public void listenerFailureBeforeWritingRetriesOnNextWrite() {
        File path = getTmpDir();
        AtomicInteger attempts = new AtomicInteger();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, writer -> {
                    if (attempts.getAndIncrement() == 0)
                        throw new IllegalStateException("boom");
                    writer.context("retry");
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            assertThrows(IllegalStateException.class, () -> appender.writeMessage("msg", "one"));
            appender.writeMessage("msg", "one");
        }

        assertEquals(2, attempts.get());
        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: retry\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void interleavingQueuesOnSharedStoreWriteOneContextRecordPerRoll() {
        File path = getTmpDir();

        try (ChronicleQueue queueA = builder(path)
                .contextListener(ContextEvents.class, writer -> writer.context("A"))
                .build();
             ChronicleQueue queueB = builder(path)
                     .contextListener(ContextEvents.class, writer -> writer.context("B"))
                     .build()) {
            ExcerptAppender appenderA = queueA.createAppender();
            ExcerptAppender appenderB = queueB.createAppender();

            appenderA.writeMessage("msg", "A0");
            appenderB.writeMessage("msg", "B0");

            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appenderB.writeMessage("msg", "B1");
            appenderA.writeMessage("msg", "A1");

            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            appenderA.writeMessage("msg", "A2");
            appenderB.writeMessage("msg", "B2");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: A\n" +
                "# index: 100000001\n" +
                "msg: A0\n" +
                "# index: 100000002\n" +
                "msg: B0\n" +
                "# index: 200000000\n" +
                "context: B\n" +
                "# index: 200000001\n" +
                "msg: B1\n" +
                "# index: 200000002\n" +
                "msg: A1\n" +
                "# index: 300000000\n" +
                "context: A\n" +
                "# index: 300000001\n" +
                "msg: A2\n" +
                "# index: 300000002\n" +
                "msg: B2\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void suppliedWriterCanBeUsedAfterCallbackReturns() {
        File path = getTmpDir();
        AtomicReference<ContextEvents> retainedWriter = new AtomicReference<>();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, writer -> {
                    retainedWriter.set(writer);
                    writer.context("queue");
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            appender.writeMessage("msg", "one");
            retainedWriter.get().context("late");
            appender.writeMessage("msg", "two");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: queue\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# index: 100000002\n" +
                "context: late\n" +
                "# index: 100000003\n" +
                "msg: two\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void listenerDocumentClosesOnlyWithOutermostContext() {
        File path = getTmpDir();
        AtomicBoolean outerContextRemainedOpen = new AtomicBoolean();

        try (ChronicleQueue queue = builder(path)
                .contextListener(DocumentContextEvents.class, writer -> {
                    try (DocumentContext outer = writer.writingDocument()) {
                        writer.context("queue");
                        outerContextRemainedOpen.set(outer.isNotComplete());
                    }
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeMessage("msg", "one");
        }

        assertTrue("an inner method-writer close must not commit its outer document context",
                outerContextRemainedOpen.get());
        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "context: queue\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void chainedListenerMethodsShareOneDocument() {
        File path = getTmpDir();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ChainedContextStart.class,
                        writer -> writer.start("queue").end("ready"))
                .build()) {
            ExcerptAppender appender = queue.createAppender();
            appender.writeMessage("msg", "one");
        }

        assertEquals("" +
                "# firstIndex: 100000000\n" +
                "# index: 100000000\n" +
                "start: queue\n" +
                "end: ready\n" +
                "# index: 100000001\n" +
                "msg: one\n" +
                "# no more messages at 8000000000000000\n", readEventsAsString(path));
    }

    @Test
    public void appenderUsesCachedMethodWriterForEachNewRollFile() {
        File path = getTmpDir();
        List<ContextEvents> suppliedWriters = new ArrayList<>();

        try (ChronicleQueue queue = builder(path)
                .contextListener(ContextEvents.class, writer -> {
                    suppliedWriters.add(writer);
                    writer.context("queue");
                })
                .build()) {
            ExcerptAppender first = queue.createAppender();
            ExcerptAppender second = queue.createAppender();

            first.writeMessage("msg", "one");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            second.writeMessage("msg", "two");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            first.writeMessage("msg", "three");
        }

        assertEquals(3, suppliedWriters.size());
        assertSame("an appender should use its cached method writer for each new roll file",
                suppliedWriters.get(0), suppliedWriters.get(2));
        assertNotSame("method writers must not be shared between appenders",
                suppliedWriters.get(0), suppliedWriters.get(1));
    }

    @Test
    public void builderListenerClosesAfterQueueAppenders() {
        AtomicReference<ExcerptAppender> appenderRef = new AtomicReference<>();
        AtomicBoolean appenderWasClosedBeforeListener = new AtomicBoolean();
        CountingContextListener listener = new CountingContextListener("queue") {
            @Override
            public void close() {
                ExcerptAppender appender = appenderRef.get();
                appenderWasClosedBeforeListener.set(appender != null && appender.isClosed());
                super.close();
            }
        };

        ChronicleQueue queue = builder(getTmpDir())
                .contextListener(ContextEvents.class, listener)
                .build();
        appenderRef.set(queue.createAppender());

        queue.close();

        assertEquals(1, listener.closeCount.get());
        assertTrue("queue-owned listener should close after queue appenders", appenderWasClosedBeforeListener.get());
    }

    private static SingleChronicleQueueBuilder builder(File path) {
        return SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY);
    }

    private static long writeMessageAndContextCount(ChronicleQueue queue, ExcerptAppender appender, String value) {
        try (DocumentContext dc = appender.writingDocument()) {
            long contextCount = dc.contextCount();
            assertEquals(queue.rollCycle().toCycle(dc.index()), contextCount);
            dc.wire().write("msg").text(value);
            return contextCount;
        }
    }

    private static long writeMessageAssumingContext(BigDtoEvents out, BigDto bigDto, String value) {
        try (DocumentContext dc = out.writingDocument()) {
            if (bigDto.needsToBeWritten(dc.contextCount()))
                out.bigDto(bigDto);
            out.msg(value);
            // The sibling test asserts the roll cycle observed while the document is held open.
            return dc.contextCount();
        }
    }

    private static List<Long> readContextCounts(File path) {
        try (ChronicleQueue queue = builder(path).build()) {
            ExcerptTailer tailer = queue.createTailer();
            List<Long> contextCounts = new ArrayList<>();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;
                    long contextCount = dc.contextCount();
                    assertEquals(queue.rollCycle().toCycle(dc.index()), contextCount);
                    contextCounts.add(contextCount);
                }
            }
            return contextCounts;
        }
    }

    private static String readEventsAsString(File path) {
        try (ChronicleQueue queue = builder(path).build()) {
            StringWriter writer = new StringWriter();
            queue.dump(writer, 0, Long.MAX_VALUE);
            return writer.toString();
        }
    }


    interface ContextEvents {
        void context(String source);
    }

    interface DocumentContextEvents extends ContextEvents, DocumentWritten {
    }

    interface ChainedContextStart {
        ChainedContextEnd start(String source);
    }

    interface ChainedContextEnd {
        void end(String state);
    }

    interface HistoryEvents extends ContextEvents {
        void msg(String value);
    }

    interface BigDtoEvents extends DocumentWritten {
        void bigDto(BigDto bigDto);

        void msg(String value);
    }

    static final class BigDto extends SelfDescribingMarshallable {
        private final String value;
        private transient long lastRollCycleWrittenTo = Long.MIN_VALUE;
        private transient int writeCount;

        private BigDto(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        private boolean needsToBeWritten(long rollCycle) {
            if (lastRollCycleWrittenTo == rollCycle)
                return false;
            lastRollCycleWrittenTo = rollCycle;
            writeCount++;
            return true;
        }
    }

    private static class CountingContextListener implements MarshallableOut.ContextListener<ContextEvents>, AutoCloseable {
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

}
