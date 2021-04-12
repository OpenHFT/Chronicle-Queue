package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.openhft.chronicle.bytes.Bytes.from;
import static net.openhft.chronicle.core.time.SystemTimeProvider.CLOCK;
import static org.junit.Assert.*;

public class ChronicleQueueIndexTest extends ChronicleQueueTestBase {

    @Test
    public void checkTheEOFisWrittenToPreQueueFile() {
        checkTheEOFisWrittenToPreQueueFileInner(appender -> appender.writeBytes(appender.queue().rollCycle().toIndex(1, 0L), from("Hello World 1")),
                (tp, rc) -> { /* not required as we are increasing cycle in next write */ },
                appender -> appender.writeBytes(appender.queue().rollCycle().toIndex(3, 0L), from("Hello World 2")));
    }

    @Test
    public void checkTheEOFisWrittenToPreQueueFileWritingDocumentMetadata() {
        final Consumer<InternalAppender> writer = appender -> {
            try (DocumentContext wd = appender.writingDocument(true)) {
                wd.wire().write("key").writeDouble(1);
            }
        };
        checkTheEOFisWrittenToPreQueueFileInner(writer, (tp, rollCycle) -> tp.advanceMillis(2 * rollCycle.lengthInMillis()), writer);
    }

    @Test
    public void checkTheEOFisWrittenToPreQueueFileWritingDocument() {
        final Consumer<InternalAppender> writer = appender -> {
            try (DocumentContext wd = appender.writingDocument()) {
                wd.wire().write("key").writeDouble(1);
            }
        };
        checkTheEOFisWrittenToPreQueueFileInner(writer, (tp, rollCycle) -> tp.advanceMillis(2 * rollCycle.lengthInMillis()), writer);
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
                .build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            writer1.accept(appender);

            Assert.assertFalse(hasEOFAtEndOfFile(file1));
        }

        tpConsumer.accept(tp, rollCycle);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(rollCycle)
                .timeProvider(tp)
                .testBlockSize()
                .build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            assertFalse(hasEOFAtEndOfFile(file1));

            writer2.accept(appender);

            // Simulate the end of the day i.e the queue closes the day rolls
            // (note the change of index from 18264 to 18265)

            assertTrue(hasEOFAtEndOfFile(file1));
        }
    }

    @Test
    public void checkTheEOFisWrittenToPreQueueFileAfterPreTouch() {
        Assume.assumeTrue(!OS.isWindows());
        SetTimeProvider tp = new SetTimeProvider(1);

        File file1 = getTmpDir();
        RollCycles rollCycle = RollCycles.DEFAULT;
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(rollCycle)
                .timeProvider(tp)
                .testBlockSize()
                .build()) {
            ExcerptAppender appender = queue.acquireAppender();

            appender.writeText("Hello World 1");

            Assert.assertFalse(hasEOFAtEndOfFile(file1));
        }

        tp.advanceMillis(TimeUnit.DAYS.toMillis(1));

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(rollCycle)
                .timeProvider(tp)
                .testBlockSize()
                .build()) {

            queue.acquireAppender().pretouch();
        }

        tp.advanceMillis(rollCycle.lengthInMillis());

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(rollCycle)
                .timeProvider(tp)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

            appender.writeText("Hello World 2");

            // Simulate the end of the day i.e the queue closes the day rolls
            // (note the change of index from 18264 to 18265)

            assertTrue(hasEOFAtEndOfFile(file1));
            try (ChronicleQueue queue123 = SingleChronicleQueueBuilder.builder()
                    .path(file1)
                    .rollCycle(rollCycle)
                    .timeProvider(tp)
                    .build()) {
                final ExcerptTailer tailer = queue123.createTailer();
                assertEquals("Hello World 1", tailer.readText());
                assertEquals("Hello World 2", tailer.readText());
            }
        }
    }

    private boolean hasEOFAtEndOfFile(final File file) {

        try (ChronicleQueue queue123 = SingleChronicleQueueBuilder.builder()
                .path(file).build()) {
            String dump = queue123.dump();
            // System.out.println(dump);
            return dump.contains(" EOF") && dump.contains("--- !!not-ready-meta-data! #binary");
        }
    }

    @Test
    public void testIndexQueue() {

        File file1 = getTmpDir();
        file1.deleteOnExit();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(RollCycles.DEFAULT)
                .build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            Bytes<byte[]> hello_world = Bytes.from("Hello World 1");
            appender.writeBytes(RollCycles.DEFAULT.toIndex(18264, 0L), hello_world);
            hello_world.releaseLast();
            hello_world = Bytes.from("Hello World 2");
            appender.writeBytes(RollCycles.DEFAULT.toIndex(18264, 1L), hello_world);
            hello_world.releaseLast();

            // Simulate the end of the day i.e the queue closes the day rolls
            // (note the change of index from 18264 to 18265)
        }
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(file1)
                .rollCycle(RollCycles.DEFAULT)
                .build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            // add a message for the new day
            Bytes<byte[]> hello_world = Bytes.from("Hello World 3");
            appender.writeBytes(RollCycles.DEFAULT.toIndex(18265, 0L), hello_world);
            hello_world.releaseLast();

            final ExcerptTailer tailer = queue.createTailer();

            final Bytes<?> forRead = Bytes.elasticByteBuffer();
            try {
                final List<String> results = new ArrayList<>();
                while (tailer.readBytes(forRead)) {
                    results.add(forRead.to8bitString());
                    forRead.clear();
                }
                assertTrue(results.toString(), results.contains("Hello World 1"));
                assertTrue(results.contains("Hello World 2"));
                // The reader fails to read the third message. The reason for this is
                // that there was no EOF marker placed at end of the 18264 indexed file
                // so when the reader started reading through the queues it got stuck on
                // that file and never progressed to the latest queue file.
                assertTrue(results.contains("Hello World 3"));
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
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();

            int i = 0;
            String msg = "world ";
            for (int j = 0; j < 8; j++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("hello").text(msg + (i++));
                    // long indexWritten = dc.index();
                }
                stp.advanceMillis(1500);
            }

            // get the current cycle
            int cycle;
            final ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext documentContext = tailer.readingDocument()) {
                long index = documentContext.index();
                cycle = queue.rollCycle().toCycle(index);
            }

            long index = queue.rollCycle().toIndex(cycle, 5);
            assertFalse(tailer.moveToIndex(index));
            try (DocumentContext dc = tailer.readingDocument()) {
                // there is no 5th message in that cycle.
                assertFalse(dc.isPresent());
            }

            // wind to start
            long index0 = queue.rollCycle().toIndex(cycle, 0);
            assertTrue(tailer.moveToIndex(index0));

            // skip four messages
            for (int j = 0; j < 4; j++)
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                }
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                String s5 = dc.wire().read("hello").text();
                // System.out.println(s5);
                assertEquals(msg + 4, s5);
            }
        }
    }

    // https://github.com/OpenHFT/Chronicle-Queue/issues/822
    @Test
    public void writeReadMetadata() {
        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            final ExcerptTailer tailer = queue.createTailer();

            boolean metadata = true;
            try (DocumentContext dc = appender.writingDocument(metadata)) {
                dc.wire().write("a").text("hello");
            }
            try (DocumentContext dc = tailer.readingDocument(metadata)) {
                Assert.assertTrue(dc.isPresent());
            }
        }
    }

    // https://github.com/OpenHFT/Chronicle-Queue/issues/822
    private void driver0(String[] strings, boolean[] meta, long millis) {

        assert (strings.length == meta.length);

        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(getTmpDir())
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            final ExcerptTailer withMetaTailer = queue.createTailer();
            final ExcerptTailer withoutMetaTailer = queue.createTailer();

            for (int i = 0; i < strings.length; ++i) {
                try (DocumentContext dc = appender.writingDocument(meta[i])) {
                    dc.wire().write("key").text(strings[i]);
                }
                Thread.sleep(millis);
            }

            // read all (meta + data)
            int allReads = 0;
            for (String string : strings) {
                try (DocumentContext dc = withMetaTailer.readingDocument(true)) {
                    if (!dc.isPresent())
                        break;

                    ++allReads;
                    String str = dc.wire().read("key").text();
                    System.out.println("M+D Read: " + str + ", vs " + string + ", index = " + dc.index());

                    Assert.assertTrue(str.equals(string));
                }
            }
            Assert.assertTrue(allReads == strings.length);

            // just data
            int dataReads = 0;
            for (int i = 0; i < strings.length; ++i) {
                if (meta[i])
                    continue;

                try (DocumentContext dc = withoutMetaTailer.readingDocument(false)) {
                    if (!dc.isPresent())
                        break;

                    ++dataReads;

                    String str = dc.wire().read("key").text();
                    System.out.println("D Read: " + str + ", vs " + strings[i] + ", index = " + dc.index());

                    Assert.assertTrue(str.equals(strings[i]));
                }
            }
            int expectedData = 0;
            for( boolean b : meta ) if(!b) ++expectedData;
            Assert.assertTrue(expectedData == dataReads);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void driver(String[] strings, boolean[] meta) {
        // run each test twice - once with all entries in the same cycle, and again with just one entry per cycle
        driver0(strings, meta, 0);
        driver0(strings, meta, 1500);
    }

    @Test
    public void D() {
        driver(
                new String[]{"data-1"},
                new boolean[]{  false}
        );
    }

    @Test
    public void M() {
        driver(
                new String[]{"data-1"},
                new boolean[]{   true}
        );
    }

    @Test
    public void DDD() {
        driver(
                new String[]{"data-1", "data-2", "data-3"},
                new boolean[]{  false,    false,    false}
        );
    }

    @Test
    public void DDM() {
        driver(
                new String[]{"data-1", "data-2", "meta-1"},
                new boolean[]{  false,    false,     true}
        );
    }

    @Test
    public void DMD() {
        driver(
                new String[]{"data-1", "meta-1", "data-2"},
                new boolean[]{  false,     true,    false}
        );
    }

    @Test
    public void DMM() {
        driver(
                new String[]{"data-1", "meta-1", "meta-2"},
                new boolean[]{  false,     true,     true}
        );
    }

    @Test
    public void MMM() {
        driver(
                new String[]{"meta-1", "meta-2", "meta-3"},
                new boolean[]{   true,     true,     true}
        );
    }

    @Test
    public void MMD() {
        driver(
                new String[]{"meta-1", "meta-2", "data-1"},
                new boolean[]{   true,     true,    false}
        );
    }

    @Test
    public void MDM() {
        driver(
                new String[]{"meta-1", "data-1", "meta-2"},
                new boolean[]{   true,    false,     true}
        );
    }

    @Test
    public void MDD() {
        driver(
                new String[]{"meta-1", "data-1", "data-2"},
                new boolean[]{   true,    false,     false}
        );
    }
}
