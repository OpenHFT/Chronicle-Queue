/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.openhft.chronicle.core.time.SystemTimeProvider.CLOCK;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_SECONDLY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"deprecation", "removal"})
public class ChronicleQueueIndexTest extends QueueTestCommon {

    @Test
    public void checkTheEOFisWrittenToPreQueueFileWritingDocumentMetadata() {

        final Consumer<InternalAppender> writer = appender -> {
            try (DocumentContext wd = appender.writingDocument(true)) {
                wd.wire().write("key").writeDouble(1);
            }
        };
        checkTheEOFisWrittenToPreQueueFileInner(writer, (tp, rollCycle) -> tp.advanceMillis(2L * rollCycle.lengthInMillis()), writer);
    }

    @Test
    public void checkTheEOFisWrittenToPreQueueFileWritingDocument() {
        final Consumer<InternalAppender> writer = appender -> {
            try (DocumentContext wd = appender.writingDocument()) {
                wd.wire().write("key").writeDouble(1);
            }
        };
        checkTheEOFisWrittenToPreQueueFileInner(writer, (tp, rollCycle) -> tp.advanceMillis(2L * rollCycle.lengthInMillis()), writer);
    }

    private void checkTheEOFisWrittenToPreQueueFileInner(Consumer<InternalAppender> writer1,
                                                         BiConsumer<SetTimeProvider, RollCycle> tpConsumer,
                                                         Consumer<InternalAppender> writer2) {
        SetTimeProvider tp = new SetTimeProvider(1_000_000_000);

        File file1 = getTmpDir();
        RollCycles rollCycle = RollCycles.DEFAULT;
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(rollCycle)
                .timeProvider(tp)
                .testBlockSize()
                .build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            writer1.accept(appender);

            Assertions.assertFalse(hasEOFAtEndOfFile(file1), "queue file should not have EOF marker before cycle roll");
        }

        tpConsumer.accept(tp, rollCycle);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(rollCycle)
                .timeProvider(tp)
                .testBlockSize()
                .build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            writer2.accept(appender);

            // Simulate the end of the day i.e the queue closes the day rolls
            // (note the change of index from 18264 to 18265)

            assertTrue(hasEOFAtEndOfFile(file1), "previous queue file should have EOF marker after cycle roll");
        }
    }

    private boolean hasEOFAtEndOfFile(final File file) {

        try (ChronicleQueue queue123 = SingleChronicleQueueBuilder.builder()
                .path(file).build()) {
            String dump = queue123.dump();
            return dump.contains(" EOF") && dump.contains("--- !!not-ready-meta-data");
        }
    }

    @Test
    public void testIndexQueue() {

        File file1 = getTmpDir();
        file1.deleteOnExit();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(RollCycles.DEFAULT)
                .build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            Bytes<byte[]> helloWorld = Bytes.from("Hello World 1");
            appender.writeBytes(RollCycles.DEFAULT.toIndex(18264, 0L), helloWorld);
            helloWorld.releaseLast();
            helloWorld = Bytes.from("Hello World 2");
            appender.writeBytes(RollCycles.DEFAULT.toIndex(18264, 1L), helloWorld);
            helloWorld.releaseLast();

            // Simulate the end of the day i.e the queue closes the day rolls
            // (note the change of index from 18264 to 18265)
        }
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(RollCycles.DEFAULT)
                .build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            // add a message for the new day
            Bytes<byte[]> helloWorld = Bytes.from("Hello World 3");
            appender.writeBytes(RollCycles.DEFAULT.toIndex(18265, 0L), helloWorld);
            helloWorld.releaseLast();

            final ExcerptTailer tailer = queue.createTailer();

            final Bytes<?> forRead = Bytes.elasticByteBuffer();
            try {
                final List<String> results = new ArrayList<>();
                while (tailer.readBytes(forRead)) {
                    results.add(forRead.to8bitString());
                    forRead.clear();
                }
                assertTrue(results.contains("Hello World 1"), results.toString());
                assertTrue(results.contains("Hello World 2"), "tailer should read second message from same cycle");
                // The reader fails to read the third message. The reason for this is
                // that there was no EOF marker placed at end of the 18264 indexed file
                // so when the reader started reading through the queues it got stuck on
                // that file and never progressed to the latest queue file.
                assertTrue(results.contains("Hello World 3"), "tailer should read message from next cycle when EOF marker present");
            } finally {
                forRead.releaseLast();
            }
        }
    }

    @Test
    public void read5thMessageTest() {
        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(CLOCK.currentTimeMillis());
        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(getTmpDir())
                .rollCycle(TEST_SECONDLY)
                .timeProvider(stp)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            int i = 0;
            String msg = "world ";
            for (int j = 0; j < 8; j++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("hello").text(msg + (i++));
                }
                stp.advanceMillis(1500);
            }

            // get the current cycle
            int cycle;
            final ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext documentContext = tailer.readingDocument()) {
                long index = documentContext.index();
                cycle = queue.rollCycle().toCycle(index + 1);
            }

            long index = queue.rollCycle().toIndex(cycle, 5);
            assertFalse(tailer.moveToIndex(index), "tailer should not move to non-existent 5th message in cycle");
            try (DocumentContext dc = tailer.readingDocument()) {
                // there is no 5th message in that cycle.
                assertFalse(dc.isPresent(), "document context should not be present for non-existent message");
            }

            // wind to start
            long index0 = queue.rollCycle().toIndex(cycle, 0);
            assertTrue(tailer.moveToIndex(index0), "tailer should move to first message in cycle");

            // skip four messages
            for (int j = 0; j < 4; j++)
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent(), "document context should be present for message " + j + " in cycle");
                    final String hello = dc.wire().read("hello").text();
                    System.out.println(hello);
                }
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent(), "document context should be present for 5th message after skipping 4");
                String s5 = dc.wire().read("hello").text();
                assertEquals(msg + 4, s5, "5th message content should match expected value");
            }
        }
    }

    // https://github.com/OpenHFT/Chronicle-Queue/issues/822
    @Test
    public void writeReadMetadata() {
        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(getTmpDir())
                .rollCycle(TEST4_SECONDLY)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            final ExcerptTailer tailer = queue.createTailer();

            boolean metadata = true;
            try (DocumentContext dc = appender.writingDocument(metadata)) {
                dc.wire().write("a").text("hello");
            }
            try (DocumentContext dc = tailer.readingDocument(metadata)) {
                Assertions.assertTrue(dc.isPresent(), "tailer should read metadata document when includeMetaData is true");
            }
        }
    }

    private static final class ReadCounts {
        private final int allReadsCount;
        private final int dataReadsCount;

        private ReadCounts(int allReadsCount, int dataReadsCount) {
            this.allReadsCount = allReadsCount;
            this.dataReadsCount = dataReadsCount;
        }
    }

    private ReadCounts driver0(String[] strings, boolean[] meta, SetTimeProvider stp, long millis) {

        assert (strings.length == meta.length);

        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(getTmpDir())
                .rollCycle(TEST4_SECONDLY)
                .timeProvider(stp)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < strings.length; ++i) {
                try (DocumentContext dc = appender.writingDocument(meta[i])) {
                    dc.wire().write("key").text(strings[i]);
                }
                stp.advanceMillis(millis);
            }

            // read all (meta + data)
            List<String> allReads = readKeyed(queue, true);
            assertEquals(Arrays.asList(strings), allReads, "tailer with includeMetaData should read all written entries");

            // just data
            List<String> dataReads = readKeyed(queue, false);
            final List<String> expectedData = IntStream.range(0, strings.length)
                    .filter(i -> !meta[i])
                    .mapToObj(i -> strings[i])
                    .collect(Collectors.toList());
            assertEquals(expectedData, dataReads, "tailer without includeMetaData should read only data entries");
            return new ReadCounts(allReads.size(), dataReads.size());
        }
    }

    @NotNull
    private List<String> readKeyed(ChronicleQueue queue, boolean includeMetaData) {
        try (ExcerptTailer tailer = queue.createTailer()) {
            List<String> allReads = new ArrayList<>();
            for (; ; ) {
                try (DocumentContext dc = tailer.readingDocument(includeMetaData)) {
                    if (!dc.isPresent())
                        return allReads;

                    final Wire wire = dc.wire();
                    final String key = wire.readEvent(String.class);
                    if (!key.equals("key"))
                        continue;
                    String str = wire.getValueIn().text();
                    allReads.add(str);
                }
            }
        }
    }

    private ReadCounts[] driver(String[] strings, boolean[] meta) {
        // run each test twice - once with all entries in the same cycle, and again with just one entry per cycle
        SetTimeProvider stp = new SetTimeProvider(1000_000_000L);
        ReadCounts sameCycle = driver0(strings, meta, stp, 0);
        ReadCounts multiCycle = driver0(strings, meta, stp, 1500);
        return new ReadCounts[]{sameCycle, multiCycle};
    }

    @Test
    public void singleDataEntry() {
        String[] strings = new String[]{"data-1"};
        boolean[] meta = new boolean[]{false};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "single data entry should be readable via index in same cycle");
    }

    @Test
    public void singleMetaEntry() {
        String[] strings = new String[]{"data-1"};
        boolean[] meta = new boolean[]{true};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "single meta entry should be readable via index in same cycle");
    }

    @Test
    public void dataDataData() {
        String[] strings = new String[]{"data-1", "data-2", "data-3"};
        boolean[] meta = new boolean[]{false, false, false};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "three data entries should be readable via index in same cycle");
    }

    @Test
    public void dataDataMeta() {
        String[] strings = new String[]{"data-1", "data-2", "meta-1"};
        boolean[] meta = new boolean[]{false, false, true};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "data-data-meta pattern should be readable via index in same cycle");
    }

    @Test
    public void dataMetaData() {
        String[] strings = new String[]{"data-1", "meta-1", "data-2"};
        boolean[] meta = new boolean[]{false, true, false};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "data-meta-data pattern should be readable via index in same cycle");
    }

    @Test
    public void dataMetaMeta() {
        String[] strings = new String[]{"data-1", "meta-1", "meta-2"};
        boolean[] meta = new boolean[]{false, true, true};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "data-meta-meta pattern should be readable via index in same cycle");
    }

    @Test
    public void metaMetaMeta() {
        String[] strings = new String[]{"meta-1", "meta-2", "meta-3"};
        boolean[] meta = new boolean[]{true, true, true};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "three meta entries should be readable via index in same cycle");
    }

    @Test
    public void metaMetaData() {
        String[] strings = new String[]{"meta-1", "meta-2", "data-1"};
        boolean[] meta = new boolean[]{true, true, false};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "meta-meta-data pattern should be readable via index in same cycle");
    }

    @Test
    public void metaDataMeta() {
        String[] strings = new String[]{"meta-1", "data-1", "meta-2"};
        boolean[] meta = new boolean[]{true, false, true};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "meta-data-meta pattern should be readable via index in same cycle");
    }

    @Test
    public void metaDataData() {
        String[] strings = new String[]{"meta-1", "data-1", "data-2"};
        boolean[] meta = new boolean[]{true, false, false};
        ReadCounts[] counts = driver(strings, meta);
        assertEquals(strings.length, counts[0].allReadsCount, "meta-data-data pattern should be readable via index in same cycle");
    }
}
