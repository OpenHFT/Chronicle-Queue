package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NoBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import java.io.File;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static net.openhft.chronicle.bytes.Bytes.from;
import static org.junit.Assert.*;

public class PeekDocumentTest extends ChronicleQueueTestBase {

    private static final String EXPECTED_MESSAGE = "hello world";

    @Test
    public void testReadWrite10() {

        File tempDir = getTmpDir();

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
                    assertTrue(tailer.peekDocument());
                    try (DocumentContext documentContext = tailer.readingDocument()) {
                        assertTrue(documentContext.isPresent());
                        assertTrue(tailer.peekDocument());

                        Wire wire = documentContext.wire();
                        long l = wire.bytes().readPosition();
                        try {
                            assertEquals("hello" + i, wire.read("value").text());
                        } finally {
                            // simulate if the message was read
                            if (l % 2 == 0)
                                wire.bytes().readPosition(l);
                        }
                    }
                }

                assertFalse(tailer.peekDocument());

                try (DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().write("value").text("hello" + 10);
                }

                assertTrue(tailer.peekDocument());

            }
        } finally {
            tempDir.deleteOnExit();
        }
    }

    @Test
    public void testReadWrite10Backwards() {

        File tempDir = getTmpDir();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {

            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("value").text("hello");
            }

            ExcerptTailer tailer = queue.createTailer();

            assertTrue(tailer.peekDocument());
            try (DocumentContext documentContext = tailer.readingDocument()) {
                assertTrue(documentContext.isPresent());
                assertTrue(tailer.peekDocument());

                Wire wire = documentContext.wire();

                String result = wire.read("value").text();
                assertEquals("hello", result);
                // System.out.println(result);

            }

            assertFalse(tailer.peekDocument());

            tailer.direction(TailerDirection.BACKWARD);

            assertTrue(tailer.peekDocument());

            try (DocumentContext documentContext = tailer.readingDocument()) {

            }
            assertFalse(tailer.peekDocument());

        } finally {
            tempDir.deleteOnExit();
        }
    }

    @Test
    public void testReadWrite() {

        File tempDir = getTmpDir();

        try {

            Bytes<byte[]> bytes = from(EXPECTED_MESSAGE);

            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {
                ExcerptAppender appender = queue.acquireAppender();

                appender.writeText("hello");

                ExcerptTailer tailer = queue.createTailer();

                assertTrue(tailer.peekDocument());

            }
        } finally {
            tempDir.deleteOnExit();
        }
    }

    @Test
    public void test2() {

        File tempDir = getTmpDir();

        try {

            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempDir).build()) {
                ExcerptAppender appender = queue.acquireAppender();

                appender.writeText("hello1");
                appender.writeText("hello2");

                ExcerptTailer tailer = queue.createTailer();
                long address1a = Jvm.getValue(tailer, "address");

                assertTrue(tailer.moveToIndex(tailer.index() + 1));

                long address1b = Jvm.getValue(tailer, "address");

                assertNotEquals(address1a, address1b);

                assertFalse(tailer.moveToIndex(tailer.index() + 1));
                long address1c = Jvm.getValue(tailer, "address");

                assertEquals(address1c, NoBytesStore.NO_PAGE);

            }
        } finally {
            tempDir.deleteOnExit();
        }
    }

    @Test
    public void testWhenNoDocument() {
        File tempDir = getTmpDir();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(tempDir).build()) {

            ExcerptTailer tailer = queue.createTailer();
            ExcerptAppender appender = queue.acquireAppender();

            boolean peekDocumentBeforeWrite = tailer.peekDocument();    // peekDocumentBeforeWrite   should be false.but returns true
            assertFalse(peekDocumentBeforeWrite);

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().writeText("testString");
            }

            boolean peekDocumentAfterWrite = tailer.peekDocument();
            assertTrue(peekDocumentAfterWrite);

            String text = null;
            try (DocumentContext dc = tailer.readingDocument()) {
                text = dc.wire().readText();
            }

            assertEquals("testString", text);

            boolean peekDocumentAfterRead = tailer.peekDocument();
            assertFalse(peekDocumentAfterRead);
        }
    }


    @Test
    public void soakTestPeekDocument() throws ExecutionException, InterruptedException {
        File tempDir = getTmpDir();

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.single(tempDir).rollCycle(RollCycles.MINUTELY).build();) {

            final long maxMessagesPerCycle = RollCycles.MINUTELY.maxMessagesPerCycle();
            System.out.println("maxMessagesPerCycle = " + DecimalFormat.getInstance().format(maxMessagesPerCycle));

            final ScheduledExecutorService es2 = newSingleThreadScheduledExecutor();
            es2.submit(() -> {
                try (final ExcerptAppender appender = q.acquireAppender()) {
                    for (int i = 0; i < maxMessagesPerCycle; i++) {
                        try (final DocumentContext documentContext = appender.writingDocument()) {
                            documentContext.wire().write("value").int64(i);
                        }
                    }
                }
            });

            final ScheduledExecutorService es = newSingleThreadScheduledExecutor();
            es.submit(() -> {
                try (final ExcerptTailer excerptTailer = q.createTailer()) {
                    long count = 0;
                    Thread.yield();
                    OUTER:
                    for (; ; ) {
                        while (excerptTailer.peekDocument()) {

                            try (DocumentContext dc = excerptTailer.readingDocument()) {
                                if (!dc.isPresent())
                                    continue OUTER;
                                assertEquals(count, dc.wire().read("value").int64());
                                count++;
                                if (count % 1_000_000 == 0)
                                    System.out.println("count = " + DecimalFormat.getInstance().format(count));
                                if (count == maxMessagesPerCycle)
                                    return null;
                            }
                        }
                    }
                }
            }).get();
            es.shutdownNow();
            es2.shutdownNow();
        }
    }
}
