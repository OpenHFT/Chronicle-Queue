package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.MethodId;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class ChronicleMethodReaderTest extends ChronicleQueueTestBase {

    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();
    private Path dataDir;

    @Before
    public void before() {
        dataDir = getTmpDir().toPath();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize()
                .build()) {
            final ExcerptAppender excerptAppender = queue.acquireAppender();
            final VanillaMethodWriterBuilder<All> methodWriterBuilder = excerptAppender.methodWriterBuilder(All.class);
            final All events = methodWriterBuilder.build();

            for (int i = 0; i < 24; ) {
                Method1Type m1 = new Method1Type();
                m1.text = "hello";
                m1.value = i;
                m1.number = i;
                events.method1(m1);
                i++;
                Method2Type m2 = new Method2Type();
                m2.text = "goodbye";
                m2.value = i;
                m2.number = i;
                events.method2(m2);
                i++;
            }
        }
        ignoreException("Overriding sourceId from existing metadata, was 0, overriding to");
    }

    @Test
    public void shouldNotFailOnEmptyQueue() {
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        basicReader(path).execute();
        assertTrue(capturedOutput.isEmpty());
    }

    @NotNull
    private ChronicleReader basicReader(Path path) {
        if (OS.isWindows())
            if (!testName.getMethodName().startsWith("shouldThrowExceptionIfInputDirectoryDoesNotExist"))
                expectException("Read-only mode is not supported on Windows");

        return new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add);
    }

    @NotNull
    private ChronicleReader basicReaderMethodReader(Path path) {
        return basicReader(path).asMethodReader(All.class.getName());
    }

    @Test
    public void shouldNotFailWhenNoMetadata() throws IOException {
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        Files.list(dataDir).filter(f -> f.getFileName().toString().endsWith(SingleTableStore.SUFFIX)).findFirst().ifPresent(path -> path.toFile().delete());
        basicReader().execute();
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
    }

    @Test
    public void shouldIncludeMessageHistoryByDefault() {
        basicReader().execute();

        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
    }

    @Test
    public void shouldApplyIncludeRegexToHistoryMessagesAndBusinessMessages() {
        basicReader().
                // matches goodbye, but not hello or history
                withInclusionRegex("goodbye").
                asMethodReader(null).
                execute();
        assertFalse(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")));
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("method2")));
    }

    @Test
    public void shouldBeAbleToReadFromReadOnlyFile() throws IOException {
        assumeFalse("#460 read-only not supported on Windows", OS.isWindows());
        final Path queueFile = Files.list(dataDir).
                filter(f -> f.getFileName().toString().endsWith(SingleChronicleQueue.SUFFIX)).findFirst().
                orElseThrow(() ->
                        new AssertionError("Could not find queue file in directory " + dataDir));

        assertTrue(queueFile.toFile().setWritable(false));

        basicReader().execute();
    }

    @Test
    public void shouldConvertEntriesToTextMethodReader() {
        basicReaderMethodReader().execute();
        long msgCount =
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
                        //.peek(System.out::println)
                        .count();
        assertEquals(24, msgCount);
        // "hello"
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("hello")));
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("method1")));
    }

    @Test
    public void shouldNotConvertEntriesToText() {
        basicReader().execute();
        long msgCount =
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
                        //.peek(System.out::println)
                        .count();
        assertEquals(24, msgCount);
        // "hello"
        assertTrue(capturedOutput.stream()
                .anyMatch(msg -> msg.contains("  5,\n" +
                        "  104,\n" +
                        "  101,\n" +
                        "  108,\n" +
                        "  108,\n" +
                        "  111,")));
    }

    @Test
    public void shouldFilterByInclusionRegex() {
        basicReader().withInclusionRegex(".*good.*").execute();

        assertEquals(24, capturedOutput.size());
        capturedOutput.stream()
                .filter(msg -> !msg.startsWith("0x"))
                .forEach(msg -> assertThat(msg, containsString("goodbye")));
    }

    @Ignore("TODO FIX")
    @Test
    public void shouldFilterByMultipleInclusionRegex() {
        basicReader().withInclusionRegex(".*bye$").withInclusionRegex(".*o.*").execute();

        assertEquals(24, capturedOutput.size());
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, containsString("goodbye")));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).
                forEach(msg -> assertThat(msg, not(containsString("hello"))));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfInputDirectoryDoesNotExist() {
        basicReader().withBasePath(Paths.get("/does/not/exist")).execute();
    }

    @Test
    public void shouldFilterByExclusionRegex() {
        basicReader().withExclusionRegex(".*good.*").execute();

        long msgCount =
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
//                        .peek(System.out::println)
                        .count();
        assertEquals(12, msgCount);
        capturedOutput.forEach(msg -> assertThat(msg, not(containsString("goodbye"))));
    }

    @Ignore("TODO FIX")
    @Test
    public void shouldFilterByMultipleExclusionRegex() {
        basicReaderMethodReader().withExclusionRegex(".*bye$").withExclusionRegex(".*ell.*").execute();

        assertEquals(0L, capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).count());
    }

    @Test
    public void shouldReturnNoMoreThanTheSpecifiedNumberOfMaxRecords() {
        basicReaderMethodReader().historyRecords(5).execute();

        assertEquals(5,
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x")).count());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfSpecifiedIndexIsBeforeFirstIndex() {
        basicReader().withStartIndex(1L).execute();
    }

    @Test
    public void shouldNotRewindPastStartOfQueueWhenDisplayingHistory() {
        basicReader().historyRecords(Long.MAX_VALUE).execute();

        assertEquals(24,
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
                        .count());
    }

    private ChronicleReader basicReader() {
        return basicReader(dataDir);
    }

    private ChronicleReader basicReaderMethodReader() {
        return basicReaderMethodReader(dataDir);
    }

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    interface Method1 {
        @MethodId(1)
        void method1(Method1Type method1Type);
    }

    interface Method2 {
        void method2(Method2Type method2Type);
    }

    interface All extends Method1, Method2 {

    }

    static class Method1Type extends BytesInBinaryMarshallable {
        String text;
        long value;
        double number;
    }

    static class Method2Type extends SelfDescribingMarshallable {
        String text;
        long value;
        double number;
    }
}