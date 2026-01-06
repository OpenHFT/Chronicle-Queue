/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MessageHistoryTest.MessageHistoryTemplateProvider.class)
public final class MessageHistoryTest extends QueueTestCommon {
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private File inputQueueDir;
    private File middleQueueDir;
    private File outputQueueDir;
    private final boolean named;

    public MessageHistoryTest(boolean named) {
        this.named = named;
    }

    private static Stream<MessageHistoryCase> cases() {
        return Stream.of(new MessageHistoryCase(true), new MessageHistoryCase(false));
    }

    private static final class MessageHistoryCase {
        private final boolean named;

        private MessageHistoryCase(boolean named) {
            this.named = named;
        }
    }

    static final class MessageHistoryTemplateProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return cases().map(MessageHistoryInvocationContext::new);
        }
    }

    private static final class MessageHistoryInvocationContext implements TestTemplateInvocationContext {
        private final MessageHistoryCase testCase;

        private MessageHistoryInvocationContext(MessageHistoryCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "named=" + testCase.named;
        }

        @Override
        public java.util.List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new MessageHistoryParameterResolver(testCase));
        }
    }

    private static final class MessageHistoryParameterResolver implements ParameterResolver {
        private final MessageHistoryCase testCase;

        private MessageHistoryParameterResolver(MessageHistoryCase testCase) {
            this.testCase = testCase;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            Class<?> type = parameterContext.getParameter().getType();
            return type == boolean.class || type == Boolean.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
            return testCase.named;
        }
    }

    @BeforeEach
    public void setUp() {
        inputQueueDir = getTmpDir();
        middleQueueDir = getTmpDir();
        outputQueueDir = getTmpDir();
        final VanillaMessageHistory messageHistory = new VanillaMessageHistory();
        messageHistory.addSourceDetails(true);
        MessageHistory.set(messageHistory);
    }

    @TestTemplate
    @DisplayName("Tailer exposes message history for standard reads")
    public void shouldAccessMessageHistory() {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue outputQueue = createQueue(outputQueueDir, 2)) {
            generateTestData(inputQueue, outputQueue);

            final ExcerptTailer tailer = outputQueue.createTailer(named ? "named" : null);

            final ValidatingSecond validatingSecond = new ValidatingSecond();
            final MethodReader validator = tailer.methodReader(validatingSecond);

            assertTrue(validator.readOne(), "tailer should read one message with history");
            assertTrue(validatingSecond.messageHistoryPresent(), "message history should be present after read");
        }
    }

    @TestTemplate
    @DisplayName("Tailer exposes message history after moving to end")
    public void shouldAccessMessageHistoryWhenTailerIsMovedToEnd() {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue outputQueue = createQueue(outputQueueDir, 2)) {
            generateTestData(inputQueue, outputQueue);

            final ExcerptTailer tailer = outputQueue.createTailer(named ? "named" : null);
            tailer.direction(TailerDirection.BACKWARD).toEnd();

            final ValidatingSecond validatingSecond = new ValidatingSecond();
            final MethodReader validator = tailer.methodReader(validatingSecond);

            assertTrue(validator.readOne(), "tailer should read one message from end");
            assertTrue(validatingSecond.messageHistoryPresent(), "message history should be present after end read");
        }
    }

    @TestTemplate
    @DisplayName("Chained queues preserve message history across hops")
    public void chainedMessageHistory() {
        try (final ChronicleQueue inputQueue = createQueue(inputQueueDir, 1);
             final ChronicleQueue middleQueue = createQueue(middleQueueDir, 2);
             final ChronicleQueue outputQueue = createQueue(outputQueueDir, 3)) {
            generateTestData(inputQueue, middleQueue);

            ExcerptTailer tailerM1 = middleQueue.createTailer(named ? "named" : null);
            MethodReader reader = tailerM1.methodReader(outputQueue.methodWriter(First.class));
            assertTrue(reader.readOne(), "chained history should read one message");
            tailerM1.toStart();
            MethodReader reader2nd = tailerM1.methodReader(outputQueue.methodWriter(Second.class));
            for (int i = 0; i < 3; i++)
                assertTrue(reader2nd.readOne(), "second reader should read entry at iteration " + i);
            assertFalse(reader2nd.readOne(), "chained history: end of queue (second)");

            MethodReader reader2 = outputQueue.createTailer(named ? "named2" : null).methodReader((First) this::say3);
            for (int i = 0; i < 3; i++)
                assertTrue(reader2.readOne(), "first reader should read entry at iteration " + i);
            assertFalse(reader2.readOne(), "chained history: end of queue (first)");
        }
    }

    private void say3(String text) {
        final MessageHistory messageHistory = MessageHistory.get();
        assertNotNull(messageHistory, "say3 should access message history");
        assertEquals(2, messageHistory.sources(), "say3 should see two message history sources");
    }

    private void generateTestData(final ChronicleQueue inputQueue, final ChronicleQueue outputQueue) {
        final First first = inputQueue.methodWriterBuilder(First.class)
                .get();
        first.say("one");
        first.say("two");
        first.say("three");

        final LoggingFirst loggingFirst =
                new LoggingFirst(outputQueue.methodWriterBuilder(Second.class).build());

        final MethodReader reader = inputQueue.createTailer(named ? "named" : null).
                methodReaderBuilder().build(loggingFirst);

        assertTrue(reader.readOne(), "generate test data should read first entry");
        assertTrue(reader.readOne(), "generate test data should read second entry");

        // roll queue file
        clock.addAndGet(TimeUnit.DAYS.toMillis(2));

        assertTrue(reader.readOne(), "generate test data should read third entry");
        assertFalse(reader.readOne(), "generate test data should reach end of input");
    }

    private ChronicleQueue createQueue(final File queueDir, final int sourceId) {
        return ChronicleQueue.singleBuilder(queueDir)
                .sourceId(sourceId)
                .timeProvider(clock::get)
                .testBlockSize().build();
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
            assertNotNull(messageHistory, "count should receive message history");
            assertEquals(2, messageHistory.sources(), "count should see two message history sources");
            messageHistoryPresent = true;
        }

        boolean messageHistoryPresent() {
            return messageHistoryPresent;
        }
    }
}
