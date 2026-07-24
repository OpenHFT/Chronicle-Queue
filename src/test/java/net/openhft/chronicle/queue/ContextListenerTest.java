/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.DocumentWritten;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

public class ContextListenerTest extends QueueTestCommon {

    @Test
    public void builderListenerWritesBeforeFirstDocumentOnFirstUseAndAfterRoll() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);
        CountingContextListener listener = new CountingContextListener("queue");

        try (ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, listener)
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            writeMessage(appender, "one");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writeMessage(appender, "two");
        }
        assertEquals("listener should be called on first use and after the first roll", 2,
                listener.invocationCount.get());

        List<Entry> entries = readEntries(path);
        assertEvents(entries,
                "context:queue", "msg:one",
                "context:queue", "msg:two");
        assertEquals(Arrays.asList(0L, 1L, 0L, 1L), sequences(entries));
    }

    @Test
    public void documentContextCountIsQueueRollCycleForWritesAndReads() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);
        List<Long> writeCounts = new ArrayList<>();

        try (ChronicleQueue queue = builder(path, timeProvider).build()) {
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
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);
        BigDto bigDto = new BigDto("static");
        long firstRollCycle;
        long sameRollCycle;
        long secondRollCycle;

        try (ChronicleQueue queue = builder(path, timeProvider).build()) {
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
        assertEquals(Arrays.asList("bigDto:static", "msg:one", "msg:two", "bigDto:static", "msg:three"),
                readAllEvents(path));
    }

    @Test
    public void listenerMayWriteNothingOnFirstUse() {
        File path = getTmpDir();
        AtomicInteger invocations = new AtomicInteger();

        try (ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, writer -> invocations.incrementAndGet())
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            writeMessage(appender, "one");
            writeMessage(appender, "two");
        }

        assertEquals("no-op listener should be called once on first use", 1, invocations.get());
        List<Entry> entries = readEntries(path);
        assertEvents(entries, "msg:one", "msg:two");
        assertEquals(Arrays.asList(0L, 1L), sequences(entries));
    }

    @Test
    public void messageHistoryIsOnlyWrittenForNaturallyTriggeredMessages() {
        ignoreException("Overriding sourceId from existing metadata, was 0, overriding to 1");
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = builder(path, timeProvider)
                .sourceId(1)
                .contextListener(HistoryEvents.class, writer -> writer.context("queue"))
                .build()) {
            HistoryEvents writer = queue.methodWriter(HistoryEvents.class);

            writer.msg("one");
            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writer.msg("two");
        }

        List<DocumentEntry> entries = readDocumentEntries(path);
        assertDocumentEvents(entries,
                "context:queue", "msg:one",
                "context:queue", "msg:two");
        assertEquals(Arrays.asList(false, true, false, true), entries.stream()
                .map(entry -> entry.hasHistory)
                .collect(Collectors.toList()));
    }

    @Test
    public void builderContextListenerSupplierCreatesOneListenerPerBuiltQueue() {
        File root = getTmpDir();
        File firstPath = new File(root, "first");
        File secondPath = new File(root, "second");
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);
        List<CountingContextListener> listeners = new ArrayList<>();
        AtomicInteger listenerIds = new AtomicInteger();

        SingleChronicleQueueBuilder baseBuilder = builder(firstPath, timeProvider)
                .contextListenerSupplier(ContextEvents.class, () -> {
                    CountingContextListener listener = new CountingContextListener("listener-" + listenerIds.getAndIncrement());
                    listeners.add(listener);
                    return listener;
                });

        try (ChronicleQueue firstQueue = baseBuilder.clone().path(firstPath).build();
             ChronicleQueue secondQueue = baseBuilder.clone().path(secondPath).build()) {
            writeMessage(firstQueue.createAppender(), "one");
            writeMessage(secondQueue.createAppender(), "two");
        }

        assertEquals(2, listeners.size());
        assertNotSame(listeners.get(0), listeners.get(1));
        assertEquals(1, listeners.get(0).invocationCount.get());
        assertEquals(1, listeners.get(1).invocationCount.get());
        assertEquals(1, listeners.get(0).closeCount.get());
        assertEquals(1, listeners.get(1).closeCount.get());
        assertEvents(readEntries(firstPath), "context:listener-0", "msg:one");
        assertEvents(readEntries(secondPath), "context:listener-1", "msg:two");
    }

    @Test
    public void reopeningExistingRollDoesNotWriteDuplicateContextRecord() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, writer -> writer.context("first"))
                .build()) {
            writeMessage(queue.createAppender(), "one");
        }

        try (ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, writer -> writer.context("second"))
                .build()) {
            writeMessage(queue.createAppender(), "two");
        }

        assertEvents(readEntries(path),
                "context:first", "msg:one", "msg:two");
    }

    @Test
    public void appenderListenerOverridesBuilderListener() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);
        CountingContextListener builderListener = new CountingContextListener("builder");
        CountingContextListener appenderListener = new CountingContextListener("appender");

        ChronicleQueue queue = builder(path, timeProvider)
                .contextListener(ContextEvents.class, builderListener)
                .build();
        ExcerptAppender appender = queue.createAppender();
        appender.contextListener(ContextEvents.class, appenderListener);

        writeMessage(appender, "one");
        appender.close();
        assertEquals(0, builderListener.closeCount.get());
        assertEquals(1, appenderListener.closeCount.get());
        queue.close();
        assertEquals(1, builderListener.closeCount.get());

        assertEvents(readEntries(path), "context:appender", "msg:one");
        assertEquals(0, builderListener.invocationCount.get());
        assertEquals(1, appenderListener.invocationCount.get());
    }

    @Test
    public void sameListenerOnBuilderAndAppenderIsClosedOnlyByQueue() {
        File path = getTmpDir();
        CountingContextListener listener = new CountingContextListener("shared");

        ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L))
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
        ChronicleQueue queue = builder(getTmpDir(), new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, listener)
                .build();

        queue.close();

        assertEquals(1, listener.closeCount.get());
    }

    @Test
    public void replacingAppenderListenerClosesPreviousLocalListener() {
        CountingContextListener first = new CountingContextListener("first");
        CountingContextListener second = new CountingContextListener("second");

        try (ChronicleQueue queue = builder(getTmpDir(), new SetTimeProvider(1_000_000_000L)).build()) {
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

        try (ChronicleQueue queue = builder(getTmpDir(), new SetTimeProvider(1_000_000_000L)).build()) {
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
        try (ChronicleQueue queue = builder(getTmpDir(), new SetTimeProvider(1_000_000_000L)).build()) {
            ExcerptAppender appender = queue.createAppender();
            writeMessage(appender, "one");

            assertThrows(IllegalStateException.class,
                    () -> appender.contextListener(ContextEvents.class, writer -> writer.context("late")));
        }
    }

    @Test
    public void listenerFailureBeforeWritingRetriesOnNextWrite() {
        File path = getTmpDir();
        AtomicInteger attempts = new AtomicInteger();

        try (ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, writer -> {
                    if (attempts.getAndIncrement() == 0)
                        throw new IllegalStateException("boom");
                    writer.context("retry");
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            assertThrows(IllegalStateException.class, () -> writeMessage(appender, "one"));
            writeMessage(appender, "one");
        }

        assertEquals(2, attempts.get());
        assertEvents(readEntries(path), "context:retry", "msg:one");
    }

    @Test
    public void interleavingQueuesOnSharedStoreWriteOneContextRecordPerRoll() {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider(1_000_000_000L);

        try (ChronicleQueue queueA = builder(path, timeProvider)
                .contextListener(ContextEvents.class, writer -> writer.context("A"))
                .build();
             ChronicleQueue queueB = builder(path, timeProvider)
                     .contextListener(ContextEvents.class, writer -> writer.context("B"))
                     .build()) {
            ExcerptAppender appenderA = queueA.createAppender();
            ExcerptAppender appenderB = queueB.createAppender();

            writeMessage(appenderA, "A0");
            writeMessage(appenderB, "B0");

            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writeMessage(appenderB, "B1");
            writeMessage(appenderA, "A1");

            timeProvider.advanceMillis(TEST_SECONDLY.lengthInMillis());
            writeMessage(appenderA, "A2");
            writeMessage(appenderB, "B2");
        }

        List<Entry> entries = readEntries(path);
        assertEvents(entries,
                "context:A", "msg:A0", "msg:B0",
                "context:B", "msg:B1", "msg:A1",
                "context:A", "msg:A2", "msg:B2");
        assertEquals(Arrays.asList(0L, 1L, 2L, 0L, 1L, 2L, 0L, 1L, 2L), sequences(entries));
    }

    @Test
    public void suppliedWriterCannotBeUsedAfterCallbackReturns() {
        File path = getTmpDir();
        AtomicReference<ContextEvents> retainedWriter = new AtomicReference<>();

        try (ChronicleQueue queue = builder(path, new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, writer -> {
                    retainedWriter.set(writer);
                    writer.context("queue");
                })
                .build()) {
            ExcerptAppender appender = queue.createAppender();

            writeMessage(appender, "one");
            assertThrows(IllegalStateException.class, () -> retainedWriter.get().context("late"));
            writeMessage(appender, "two");
        }

        assertEvents(readEntries(path), "context:queue", "msg:one", "msg:two");
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

        ChronicleQueue queue = builder(getTmpDir(), new SetTimeProvider(1_000_000_000L))
                .contextListener(ContextEvents.class, listener)
                .build();
        appenderRef.set(queue.createAppender());

        queue.close();

        assertEquals(1, listener.closeCount.get());
        assertTrue("queue-owned listener should close after queue appenders", appenderWasClosedBeforeListener.get());
    }

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
            long rollCycle = dc.contextCount();
            bigDto.writeIfNeeded(out, rollCycle);
            out.msg(value);
            return rollCycle;
        }
    }

    private static List<Long> readContextCounts(File path) {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer();
            List<Long> contextCounts = new ArrayList<>();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;
                    long contextCount = dc.contextCount();
                    assertEquals(queue.rollCycle().toCycle(dc.index()), contextCount);
                    StringBuilder eventName = new StringBuilder();
                    dc.wire().readEventName(eventName).skipValue();
                    contextCounts.add(contextCount);
                }
            }
            return contextCounts;
        }
    }

    private static List<String> readAllEvents(File path) {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer();
            List<String> events = new ArrayList<>();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;
                    while (dc.wire().hasMore()) {
                        StringBuilder eventName = new StringBuilder();
                        ValueIn valueIn = dc.wire().readEventName(eventName);
                        events.add(eventName + ":" + valueIn.text());
                    }
                }
            }
            return events;
        }
    }

    private static List<DocumentEntry> readDocumentEntries(File path) {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder(path, WireType.BINARY)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer();
            List<DocumentEntry> entries = new ArrayList<>();
            while (true) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;
                    boolean hasHistory = false;
                    String eventName = null;
                    String value = null;
                    while (dc.wire().hasMore()) {
                        StringBuilder fieldName = new StringBuilder();
                        ValueIn valueIn = dc.wire().readEventName(fieldName);
                        String field = fieldName.toString();
                        if (MethodReader.HISTORY.equals(field)) {
                            hasHistory = true;
                            valueIn.skipValue();
                        } else {
                            assertNull("document should contain only one non-history event", eventName);
                            eventName = field;
                            value = valueIn.text();
                        }
                    }
                    assertNotNull("document should contain a non-history event", eventName);
                    entries.add(new DocumentEntry(eventName, value, hasHistory));
                }
            }
            return entries;
        }
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
                    entries.add(new Entry(eventName.toString(), valueIn.text(), queue.rollCycle().toSequenceNumber(dc.index())));
                }
            }
            return entries;
        }
    }

    private static void assertEvents(List<Entry> entries, String... expected) {
        assertEquals(Arrays.asList(expected), entries.stream()
                .map(entry -> entry.eventName + ':' + entry.value)
                .collect(Collectors.toList()));
    }

    private static void assertDocumentEvents(List<DocumentEntry> entries, String... expected) {
        assertEquals(Arrays.asList(expected), entries.stream()
                .map(entry -> entry.eventName + ':' + entry.value)
                .collect(Collectors.toList()));
    }

    private static List<Long> sequences(List<Entry> entries) {
        return entries.stream()
                .map(entry -> entry.sequence)
                .collect(Collectors.toList());
    }

    interface ContextEvents {
        void context(String source);
    }

    interface HistoryEvents extends ContextEvents {
        void msg(String value);
    }

    interface BigDtoEvents extends DocumentWritten {
        void bigDto(String value);

        void msg(String value);
    }

    private static final class BigDto {
        private final String value;
        private transient long lastRollCycleWrittenTo = Long.MIN_VALUE;
        private int writeCount;

        private BigDto(String value) {
            this.value = value;
        }

        private void writeIfNeeded(BigDtoEvents out, long rollCycle) {
            if (lastRollCycleWrittenTo == rollCycle)
                return;
            lastRollCycleWrittenTo = rollCycle;
            writeCount++;
            out.bigDto(value);
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

    private static final class DocumentEntry {
        private final String eventName;
        private final String value;
        private final boolean hasHistory;

        private DocumentEntry(String eventName, String value, boolean hasHistory) {
            this.eventName = eventName;
            this.value = value;
            this.hasHistory = hasHistory;
        }
    }
}
