package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.RollCycles.MINUTELY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

public class InternalAppenderWriteBytesTest {

    @Test
    public void test() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        Bytes result = Bytes.elasticByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("")).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);

            ((InternalAppender) appender).writeBytes(1, test);

            ExcerptTailer tailer = q.createTailer();

            tailer.readBytes(result);
            Assert.assertTrue("hello world".contentEquals(result));
            result.clear();

            tailer.readBytes(result);
            Assert.assertTrue("hello world".contentEquals(result));
            result.clear();

        } finally {
            result.releaseLast();
            test.releaseLast();
        }
    }

    @Test(expected = java.lang.IllegalStateException.class)
    public void test2() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        Bytes result = Bytes.elasticByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("")).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);

            ExcerptTailer tailer = q.createTailer();
            ((InternalAppender) appender).writeBytes(0, test);

            tailer.readBytes(result);
            Assert.assertTrue("hello world".contentEquals(result));
            Assert.assertEquals(1, tailer.index());
            result.clear();

            // this will throw because it is not in sequence
            ((InternalAppender) appender).writeBytes(2, test);

        } finally {
            result.releaseLast();
            test.releaseLast();
        }
    }

    @Test
    public void test3() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        Bytes result = Bytes.elasticByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(DirectoryUtils.tempDir("")).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);

            ExcerptTailer tailer = q.createTailer();
            ((InternalAppender) appender).writeBytes(0, test);

            try (DocumentContext documentContext = tailer.readingDocument()) {
                result.write(documentContext.wire().bytes());
            }

            Assert.assertTrue("hello world".contentEquals(result));
            Assert.assertEquals(1, tailer.index());
            result.clear();

            ((InternalAppender) appender).writeBytes(1, test);

            try (DocumentContext dc = tailer.readingDocument()) {
                dc.rollbackOnClose();
                // result.write(documentContext.wire().bytes());
                throw new NullPointerException();
            } catch (Exception e) {
                e.printStackTrace();
            }

            Assert.assertEquals(1, tailer.index());
            result.clear();

            ((InternalAppender) appender).writeBytes(2, test);

        } finally {
            result.releaseLast();
            test.releaseLast();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testJumpingAMessageThrowsAIllegalStateException() {

        try (SingleChronicleQueue q = binary(tempDir("q"))
                .rollCycle(MINUTELY)
                .timeProvider(() -> 0).build();

             ExcerptAppender appender = q.acquireAppender()) {
            appender.writeText("hello");
            appender.writeText("hello");
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeLong(1);
            }

            final long l = appender.lastIndexAppended();
            final RollCycle rollCycle = q.rollCycle();
            final long index = rollCycle.toIndex(rollCycle.toCycle(l) + 1, 1);
            ((InternalAppender) appender).writeBytes(index, Bytes.from("text"));
        }
    }
}
