package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.bytes.Bytes.from;
import static org.junit.Assert.*;

public class ChronicleQueueIndexTest extends ChronicleQueueTestBase {

    @Test
    public void checkTheEOFisWrittenToPreQueueFile() {

        SetTimeProvider tp = new SetTimeProvider(1_000_000_000);

        File file1 = getTmpDir();
        try {
            RollCycles rollCycle = RollCycles.DEFAULT;
            try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                    .path(file1)
                    .rollCycle(rollCycle)
                    .timeProvider(tp)
                    .testBlockSize()
                    .build()) {
                InternalAppender appender = (InternalAppender) queue.acquireAppender();

                appender.writeBytes(rollCycle.toIndex(1, 0L), from("Hello World 1"));

                Assert.assertFalse(hasEOFAtEndOfFile(file1));
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }

            tp.advanceMillis(2 * rollCycle.lengthInMillis());

            try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                    .path(file1)
                    .rollCycle(rollCycle)
                    .timeProvider(tp)
                    .testBlockSize()
                    .build()) {
                InternalAppender appender = (InternalAppender) queue.acquireAppender();

                appender.writeBytes(rollCycle.toIndex(3, 0L), from("Hello World 2"));

                // Simulate the end of the day i.e the queue closes the day rolls
                // (note the change of index from 18264 to 18265)

                assertTrue(hasEOFAtEndOfFile(file1));

            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        } finally {
            file1.deleteOnExit();
        }
    }

    @Test
    public void checkTheEOFisWrittenToPreQueueFileAfterPreTouch() {
        Assume.assumeTrue(!OS.isWindows());
        SetTimeProvider tp = new SetTimeProvider(1);

        File file1 = getTmpDir();
        try {
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
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }

            tp.advanceMillis(TimeUnit.DAYS.toMillis(1));

            try (ChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                    .path(file1)
                    .rollCycle(rollCycle)
                    .timeProvider(tp)
                    .testBlockSize()
                    .build()) {

                queue.acquireAppender().pretouch();

            } catch (Exception e) {
                e.printStackTrace();
                fail();
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
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        } finally {
            file1.deleteOnExit();
        }
    }

    private boolean hasEOFAtEndOfFile(final File file) {

        try (ChronicleQueue queue123 = SingleChronicleQueueBuilder.builder()
                .path(file).build()) {
            String dump = queue123.dump();
//            System.out.println(dump);
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

    @After
    public void checkRegisteredBytes() {
        AbstractReferenceCounted.assertReferencesReleased();
    }
}
