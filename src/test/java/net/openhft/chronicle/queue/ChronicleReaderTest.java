package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class ChronicleReaderTest {
    @NotNull
    private static final Path BASE_PATH = Paths.get("src/test/resources/reader");

    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();

    @Test
    public void shouldConvertEntriesToText() throws Exception {
        basicReader().execute();

        assertThat(capturedOutput.size(), is(24));
        assertThat(capturedOutput.stream().anyMatch(msg -> msg.contains("FIX.4.2")), is(true));
    }

    @Test
    public void shouldFilterByInclusionRegex() throws Exception {
        basicReader().withInclusionRegex("35=A").execute();

        assertThat(capturedOutput.size(), is(16));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("35=A")));
    }

    @Test
    public void shouldFilterByExclusionRegex() throws Exception {
        basicReader().withExclusionRegex("35=A").execute();

        assertThat(capturedOutput.size(), is(8));
        capturedOutput.forEach(msg -> assertThat(msg, not(containsString("35=A"))));
    }

    @Test
    public void shouldReturnNoMoreThanTheSpecifiedNumberOfMaxRecords() throws Exception {
        basicReader().historyRecords(5).execute();

        assertThat(capturedOutput.stream().
                filter(msg -> !msg.startsWith("0x")).count(), is(5L));
    }

    @Test
    public void shouldForwardToSpecifiedIndex() throws Exception {
        final long knownIndex = Long.parseLong("43b800000007", 16);
        basicReader().withStartIndex(knownIndex).execute();

        assertThat(capturedOutput.size(), is(11));
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
                filter(msg -> !msg.startsWith("0x")).count(), is(12L));
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
                withBasePath(BASE_PATH).withMessageSink(capturedOutput::add);
    }
}