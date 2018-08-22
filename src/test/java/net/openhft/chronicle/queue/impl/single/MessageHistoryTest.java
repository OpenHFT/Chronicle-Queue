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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public final class MessageHistoryTest {
    @Rule
    public final TestName testName = new TestName();
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private File inputQueueDir;
    private File outputQueueDir;

    @Before
    public void setUp() throws Exception {
        inputQueueDir = DirectoryUtils.tempDir(testName.getMethodName());
        outputQueueDir = DirectoryUtils.tempDir(testName.getMethodName());
        final VanillaMessageHistory messageHistory = new VanillaMessageHistory();
        messageHistory.addSourceDetails(true);
        MessageHistory.set(messageHistory);
    }

    @Test
    public void shouldAccessMessageHistory() throws Exception {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue outputQueue = createQueue(outputQueueDir, 2)) {
            generateTestData(inputQueue, outputQueue);

            final ExcerptTailer tailer = outputQueue.createTailer();

            final ValidatingSecond validatingSecond = new ValidatingSecond();
            final MethodReader validator = tailer.methodReader(validatingSecond);

            assertThat(validator.readOne(), is(true));
            assertThat(validatingSecond.messageHistoryPresent(), is(true));
        }
    }

    @Test
    public void shouldAccessMessageHistoryWhenTailerIsMovedToEnd() throws Exception {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue outputQueue = createQueue(outputQueueDir, 2)) {
            generateTestData(inputQueue, outputQueue);

            final ExcerptTailer tailer = outputQueue.createTailer();
            tailer.direction(TailerDirection.BACKWARD).toEnd();

            final ValidatingSecond validatingSecond = new ValidatingSecond();
            final MethodReader validator = tailer.methodReader(validatingSecond);

            assertThat(validator.readOne(), is(true));
            assertThat(validatingSecond.messageHistoryPresent(), is(true));
        }
    }

    private void generateTestData(final ChronicleQueue inputQueue, final ChronicleQueue outputQueue) {
        final First first = inputQueue.acquireAppender().
                methodWriterBuilder(First.class).recordHistory(true).build();
        first.say("one");
        first.say("two");
        first.say("three");

        final LoggingFirst loggingFirst =
                new LoggingFirst(outputQueue.acquireAppender().
                        methodWriterBuilder(Second.class).build());

        final MethodReader reader = inputQueue.createTailer().
                methodReaderBuilder().build(loggingFirst);

        assertThat(reader.readOne(), is(true));
        assertThat(reader.readOne(), is(true));

        // roll queue file
        clock.addAndGet(TimeUnit.DAYS.toMillis(2));

        assertThat(reader.readOne(), is(true));
        assertThat(reader.readOne(), is(false));
    }

    private ChronicleQueue createQueue(final File queueDir, final int sourceId) {
        return SingleChronicleQueueBuilder.binary(queueDir).sourceId(sourceId).
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
            assertThat(messageHistory.sources(), is(2));
            messageHistoryPresent = true;
        }

        boolean messageHistoryPresent() {
            return messageHistoryPresent;
        }
    }
}