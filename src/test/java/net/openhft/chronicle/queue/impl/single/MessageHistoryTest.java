package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public final class MessageHistoryTest {
    @Rule
    public final TestName testName = new TestName();
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private File inputQueueDir;
    private File middleQueueDir;
    private File outputQueueDir;

    @Before
    public void setUp() {
        inputQueueDir = DirectoryUtils.tempDir(testName.getMethodName());
        middleQueueDir = DirectoryUtils.tempDir(testName.getMethodName());
        outputQueueDir = DirectoryUtils.tempDir(testName.getMethodName());
        final VanillaMessageHistory messageHistory = new VanillaMessageHistory();
        messageHistory.addSourceDetails(true);
        MessageHistory.set(messageHistory);
    }

    @Test
    public void shouldAccessMessageHistory() {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue outputQueue = createQueue(outputQueueDir, 2)) {
            generateTestData(inputQueue, outputQueue);

            final ExcerptTailer tailer = outputQueue.createTailer();

            final ValidatingSecond validatingSecond = new ValidatingSecond();
            final MethodReader validator = tailer.methodReader(validatingSecond);

            assertTrue(validator.readOne());
            assertTrue(validatingSecond.messageHistoryPresent());
        }
    }

    @Test
    public void shouldAccessMessageHistoryWhenTailerIsMovedToEnd() {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue outputQueue = createQueue(outputQueueDir, 2)) {
            generateTestData(inputQueue, outputQueue);

            final ExcerptTailer tailer = outputQueue.createTailer();
            tailer.direction(TailerDirection.BACKWARD).toEnd();

            final ValidatingSecond validatingSecond = new ValidatingSecond();
            final MethodReader validator = tailer.methodReader(validatingSecond);

            assertTrue(validator.readOne());
            assertTrue(validatingSecond.messageHistoryPresent());
        }
    }

    @Test
    public void chainedMessageHistory() {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue middleQueue = createQueue(middleQueueDir, 2);
             final ChronicleQueue outputQueue = createQueue(middleQueueDir, 3)) {
            generateTestData(inputQueue, middleQueue);

            MethodReader reader = middleQueue.createTailer().methodReader(outputQueue.methodWriter(First.class));
            for (int i = 0; i < 3; i++)
                assertTrue(reader.readOne());
            MethodReader reader2 = outputQueue.createTailer().methodReader((First) this::say3);
            for (int i = 0; i < 3; i++)
                assertTrue(reader2.readOne());
        }
    }

    private void say3(String text) {
        final MessageHistory messageHistory = MessageHistory.get();
        assertNotNull(messageHistory);
        assertEquals(2, messageHistory.sources());
    }

    private void generateTestData(final ChronicleQueue inputQueue, final ChronicleQueue outputQueue) {
        final First first = inputQueue.acquireAppender()
                .methodWriterBuilder(First.class)
                .recordHistory(true)
                .get();
        first.say("one");
        first.say("two");
        first.say("three");

        final LoggingFirst loggingFirst =
                new LoggingFirst(outputQueue.acquireAppender().
                        methodWriterBuilder(Second.class).build());

        final MethodReader reader = inputQueue.createTailer().
                methodReaderBuilder().build(loggingFirst);

        assertTrue(reader.readOne());
        assertTrue(reader.readOne());

        // roll queue file
        clock.addAndGet(TimeUnit.DAYS.toMillis(2));

        assertTrue(reader.readOne());
        assertEquals(false, reader.readOne());
    }

    private ChronicleQueue createQueue(final File queueDir, final int sourceId) {
        return ChronicleQueue.singleBuilder(queueDir).sourceId(sourceId).
                timeProvider(clock::get).
                testBlockSize().build();
    }

    @FunctionalInterface
    interface First {
        void say(final String word);
    }

    @FunctionalInterface
    interface Second {
        void count(final int value);
    }

    private static final class LoggingFirst implements First {
        private final Second second;

        private LoggingFirst(final Second second) {
            this.second = second;
        }

        @Override
        public void say(final String word) {
            second.count(word.length());
        }
    }

    private static class ValidatingSecond implements Second {
        private boolean messageHistoryPresent = false;

        @Override
        public void count(final int value) {
            final MessageHistory messageHistory = MessageHistory.get();
            assertNotNull(messageHistory);
            assertEquals(1, messageHistory.sources());
            messageHistoryPresent = true;
        }

        boolean messageHistoryPresent() {
            return messageHistoryPresent;
        }
    }
}