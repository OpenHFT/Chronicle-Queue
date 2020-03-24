package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NoBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.bytes.Bytes.fromString;

public class PeekDocumentTest {

    private static final String EXPECTED_MESSAGE = "hello world";

    @Test
    public void testReadWrite() {

        File tempDir = DirectoryUtils.tempDir("to-be-deleted");

        try {

            Bytes<byte[]> bytes = fromString(EXPECTED_MESSAGE);

            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {
                ExcerptAppender appender = queue.acquireAppender();

                appender.writeText("hello");

                ExcerptTailer tailer = queue.createTailer();

                Assert.assertTrue(tailer.peekDocument());

            }
        } finally {
            tempDir.deleteOnExit();
        }

    }

    @Test
    public void test2() {

        File tempDir = DirectoryUtils.tempDir("to-be-deleted");

        try {


            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {
                ExcerptAppender appender = queue.acquireAppender();

                appender.writeText("hello1");
                appender.writeText("hello2");

                ExcerptTailer tailer = queue.createTailer();
                long address1a = Jvm.getValue(tailer, "address");

                Assert.assertTrue(tailer.moveToIndex(tailer.index() + 1));

                long address1b = Jvm.getValue(tailer, "address");

                Assert.assertNotEquals(address1a, address1b);

                Assert.assertFalse(tailer.moveToIndex(tailer.index() + 1));
                long address1c = Jvm.getValue(tailer, "address");

                Assert.assertEquals(address1c, NoBytesStore.NO_PAGE);

            }
        } finally {
            tempDir.deleteOnExit();
        }

    }

}
