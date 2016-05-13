package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Rob Austin.
 */
public class CreateAtIndexTest {

    @Test
    public void testWriteBytesWithIndex() throws Exception {
        String tmp = OS.TMP + "/CreateAtIndexTest-" + System.nanoTime();
        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            ExcerptAppender appender = queue.createAppender();

            appender.writeBytes(0x421d00000000L, Bytes.from("hello world"));
            appender.writeBytes(0x421d00000001L, Bytes.from("hello world"));
        }
        // try again and fail.
        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            ExcerptAppender appender = queue.createAppender();

            try {
                appender.writeBytes(0x421d00000000L, Bytes.from("hello world"));
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Unable to move to index 421d00000000 as the index already exists",
                        e.getMessage());
            }
        }

        // try too far
        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            ExcerptAppender appender = queue.createAppender();

            try {
                appender.writeBytes(0x421d00000003L, Bytes.from("hello world"));
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Unable to move to index 421d00000003 beyond the end of the queue",
                        e.getMessage());
            }
        }

        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            ExcerptAppender appender = queue.createAppender();

            appender.writeBytes(0x421d00000002L, Bytes.from("hello world"));
            appender.writeBytes(0x421d00000003L, Bytes.from("hello world"));
        }

        try {
            IOTools.deleteDirWithFiles(tmp, 2);
        } catch (IORuntimeException ignored) {
        }
    }

    @Test
    public void testTailerReadingDocumentTest() throws Exception {
        String tmp = OS.TMP + "/CreateAtIndexTest-" + System.nanoTime();
        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            long queueIndex = queue.lastIndex();

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                long tailerIndex = dc.index();
                Assert.assertEquals(queueIndex, tailerIndex);
            }

            Assert.assertEquals(queueIndex, tailer.index());
        }
    }

    @Test
    public void testWrittenAndReadIndexesAreTheSameOfTheFirstExceprt() throws Exception {
        String tmp = OS.TMP + "/CreateAtIndexTest-" + System.nanoTime();

        long expected = 0;

        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {

            ExcerptAppender appender = queue.createAppender();

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().writeDocument(true, w -> w.write().text("some-data"));

                long index = dc.index();
                Assert.assertTrue(index > 0);

            }

            ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {

                long actualIndex = dc.index();
                Assert.assertTrue(actualIndex > 0);

                Assert.assertEquals(expected, actualIndex);

            }

        }
    }
}