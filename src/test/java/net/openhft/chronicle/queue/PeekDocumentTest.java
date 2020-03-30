package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NoBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.bytes.Bytes.fromString;

public class PeekDocumentTest {

    private static final String EXPECTED_MESSAGE = "hello world";

    @Test
    public void testReadWrite10() {

        File tempDir = DirectoryUtils.tempDir("to-be-deleted");

        try {

            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {

                ExcerptAppender appender = queue.acquireAppender();
                for (int i = 0; i < 1024; i++) {
                    try (DocumentContext documentContext = appender.writingDocument()) {
                        documentContext.wire().write("value").text("hello" + i);
                    }

                }

                ExcerptTailer tailer = queue.createTailer();

                for (int i = 0; i < 1024; i++) {
                    Assert.assertTrue(tailer.peekDocument());
                    try (DocumentContext documentContext = tailer.readingDocument()) {
                        Assert.assertTrue(documentContext.isPresent());
                        Assert.assertTrue(tailer.peekDocument());

                        Wire wire = documentContext.wire();
                        long l = wire.bytes().readPosition();
                        try {
                            Assert.assertEquals("hello" + i, wire.read("value").text());
                        } finally {
                            // simulate if the message was read
                            if (l % 2 == 0)
                                wire.bytes().readPosition(l);
                        }
                    }

                }

                Assert.assertFalse(tailer.peekDocument());

                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().write("value").text("hello" + 10);
                }

                Assert.assertTrue(tailer.peekDocument());

            }
        } finally {
            tempDir.deleteOnExit();
        }

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadWrite10Backwards() {

        File tempDir = DirectoryUtils.tempDir("to-be-deleted");

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {

            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("value").text("hello");
            }

            ExcerptTailer tailer = queue.createTailer();

            Assert.assertTrue(tailer.peekDocument());
            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertTrue(documentContext.isPresent());
                Assert.assertTrue(tailer.peekDocument());

                Wire wire = documentContext.wire();

                String result = wire.read("value").text();
                Assert.assertEquals("hello", result);
                System.out.println(result);

            }

            Assert.assertFalse(tailer.peekDocument());

            tailer.direction(TailerDirection.BACKWARD);

            // throws UnsupportedOperationException
            Assert.assertFalse(tailer.peekDocument());

        } finally {
            tempDir.deleteOnExit();
        }

    }

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
