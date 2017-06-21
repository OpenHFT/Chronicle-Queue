package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class ChronicleReaderTest {
    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();
    private Path dataDir;

    @Before
    public void before() throws Exception {
        dataDir = DirectoryUtils.tempDir(ChronicleReaderTest.class.getSimpleName()).toPath();
        try(final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir).build()) {
            final StringEvents events = queue.acquireAppender().methodWriterBuilder(StringEvents.class).build();

            for (int i = 0; i < 24; i++) {
                events.say(i % 2 == 0 ? "hello" : "goodbye");
            }
        }
    }

    @Test
    public void shouldBeAbleToReadFromReadOnlyFile() throws Exception {
        final Path queueFile = Files.list(dataDir).findFirst().orElseThrow(() ->
                new AssertionError("Could not find queue file in directory " + dataDir));

        assertThat(queueFile.toFile().setWritable(false), is(true));

        basicReader().execute();
    }

    @Test
    public void shouldConvertEntriesToText() throws Exception {
        basicReader().execute();

        assertThat(capturedOutput.size(), is(48));
        assertThat(capturedOutput.stream().anyMatch(msg -> msg.contains("hello")), is(true));
    }

    @Test
    public void shouldFilterByInclusionRegex() throws Exception {
        basicReader().withInclusionRegex(".*good.*").execute();

        assertThat(capturedOutput.size(), is(24));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("goodbye")));
    }

    @Test
    public void shouldFilterByExclusionRegex() throws Exception {
        basicReader().withExclusionRegex(".*good.*").execute();

        assertThat(capturedOutput.size(), is(24));
        capturedOutput.forEach(msg -> assertThat(msg, not(containsString("goodbye"))));
    }

    @Test
    public void shouldReturnNoMoreThanTheSpecifiedNumberOfMaxRecords() throws Exception {
        basicReader().historyRecords(5).execute();

        assertThat(capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), is(5L));
    }

    @Test
    public void shouldForwardToSpecifiedIndex() throws Exception {
        final long knownIndex = Long.decode("0x43ba0000000a");
        basicReader().withStartIndex(knownIndex).execute();

        assertThat(capturedOutput.size(), is(29));
        // discard first message
        capturedOutput.poll();
        assertThat(capturedOutput.poll().contains(Long.toHexString(knownIndex)), is(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfSpecifiedIndexIsBeforeFirstIndex() throws Exception {
        basicReader().withStartIndex(1L).execute();
    }

    @Test
    public void shouldNotRewindPastStartOfQueueWhenDisplayingHistory() throws Exception {
        basicReader().historyRecords(Long.MAX_VALUE).execute();

        assertThat(capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), is(24L));
    }

    @Test
    public void shouldContinueToPollQueueWhenTailModeIsEnabled() throws Exception {
        final int expectedPollCountWhenDocumentIsEmpty = 3;
        final FiniteDocumentPollMethod pollMethod = new FiniteDocumentPollMethod(expectedPollCountWhenDocumentIsEmpty);
        try {
            basicReader().withDocumentPollMethod(pollMethod).tail().execute();
        } catch (ArithmeticException e) {
            // expected
        }

        assertThat(pollMethod.invocationCount, is(expectedPollCountWhenDocumentIsEmpty));
    }

    private static final class FiniteDocumentPollMethod implements Function<ExcerptTailer, DocumentContext> {

        private final int maxPollsReturningEmptyDocument;
        private int invocationCount;

        private FiniteDocumentPollMethod(final int maxPollsReturningEmptyDocument) {
            this.maxPollsReturningEmptyDocument = maxPollsReturningEmptyDocument;
        }

        @Override
        public DocumentContext apply(final ExcerptTailer excerptTailer) {
            final DocumentContext documentContext = excerptTailer.readingDocument();

            if (!documentContext.isPresent()) {
                invocationCount++;
                if (invocationCount >= maxPollsReturningEmptyDocument) {
                    throw new ArithmeticException("For testing purposes");
                }
            }

            return documentContext;
        }
    }

    private ChronicleReader basicReader() {
        return new ChronicleReader().
                withBasePath(dataDir).withMessageSink(capturedOutput::add);
    }

    private interface StringEvents {
        void say(final String msg);
    }
}