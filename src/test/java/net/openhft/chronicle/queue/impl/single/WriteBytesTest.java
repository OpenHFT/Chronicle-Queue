package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.builder;

/**
 * @author Rob Austin.
 */
public class WriteBytesTest {

    @Test
    public void writeBytesAndIndexFiveTimesWithOverwriteTest() throws IOException {

        WireType wireType = WireType.BINARY;

        try (final SingleChronicleQueue sourceQueue =
                     builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).
                             testBlockSize().build()) {

            for (int i = 0; i < 5; i++) {
                ExcerptAppender excerptAppender = sourceQueue.acquireAppender();
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            ExcerptTailer tailer = sourceQueue.createTailer();

            try (final SingleChronicleQueue queue =
                         builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).testBlockSize().build()) {

                ExcerptAppender excerptAppender = queue.acquireAppender();
                SingleChronicleQueueExcerpts.InternalAppender appender = (SingleChronicleQueueExcerpts.InternalAppender) excerptAppender;

                List<BytesWithIndex> bytesWithIndies = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    bytesWithIndies.add(bytes(tailer));
                }

                for (int i = 0; i < 4; i++) {
                    BytesWithIndex b = bytesWithIndies.get(i);
                    appender.writeBytes(b.index, b.bytes);
                }

                for (int i = 0; i < 4; i++) {
                    BytesWithIndex b = bytesWithIndies.get(i);
                    appender.writeBytes(b.index, b.bytes);
                }

                BytesWithIndex b = bytesWithIndies.get(4);
                appender.writeBytes(b.index, b.bytes);
                ((SingleChronicleQueueExcerpts.StoreAppender) appender).checkWritePositionHeaderNumber();
                excerptAppender.writeText("hello");
            }

        }
    }


    @Test
    public void writeBytesAndIndexFiveTimesTest() throws IOException {

        WireType wireType = WireType.BINARY;

        try (final SingleChronicleQueue sourceQueue =
                     builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).
                             testBlockSize().build()) {

            for (int i = 0; i < 5; i++) {
                ExcerptAppender excerptAppender = sourceQueue.acquireAppender();
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("hello").text("world" + i);
                }
            }

            ExcerptTailer tailer = sourceQueue.createTailer();

            try (final SingleChronicleQueue queue =
                         builder(DirectoryUtils.tempDir("to-be-deleted"), wireType).testBlockSize().build()) {

                SingleChronicleQueueExcerpts.InternalAppender appender = (SingleChronicleQueueExcerpts.InternalAppender) queue.acquireAppender();

                for (int i = 0; i < 5; i++) {
                    final BytesWithIndex b = bytes(tailer);
                    appender.writeBytes(b.index, b.bytes);
                }

                Assert.assertTrue(queue.dump().contains("# position: 262616, header: 0\n" +
                        "--- !!data #binary\n" +
                        "hello: world0\n" +
                        "# position: 262633, header: 1\n" +
                        "--- !!data #binary\n" +
                        "hello: world1\n" +
                        "# position: 262650, header: 2\n" +
                        "--- !!data #binary\n" +
                        "hello: world2\n" +
                        "# position: 262667, header: 3\n" +
                        "--- !!data #binary\n" +
                        "hello: world3\n" +
                        "# position: 262684, header: 4\n" +
                        "--- !!data #binary\n" +
                        "hello: world4\n"));
            }

        }
    }

    private BytesWithIndex bytes(final ExcerptTailer tailer) {
        try (DocumentContext dc = tailer.readingDocument()) {


            if (!dc.isPresent())
                return null;

            Bytes<?> bytes = dc.wire().bytes();
            long index = dc.index();
            return new BytesWithIndex(bytes, index);
        }
    }

    private static class BytesWithIndex {
        private BytesStore bytes;
        private long index;

        public BytesWithIndex(Bytes<?> bytes, long index) {
            this.bytes = Bytes.allocateElasticDirect(bytes.readRemaining()).write(bytes);
            this.index = index;
        }

    }


}
