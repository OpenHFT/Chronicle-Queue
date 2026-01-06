/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.MethodId;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class ChronicleMethodReaderTest extends QueueTestCommon {

    private final Queue<String> capturedOutput = new ConcurrentLinkedQueue<>();
    private Path dataDir;
    private String testMethodName = "";

    @BeforeEach
    public void before(TestInfo testInfo) {
        testMethodName = testInfo.getTestMethod().map(method -> method.getName()).orElse("");
        dataDir = getTmpDir().toPath();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .sourceId(1)
                .testBlockSize()
                .build()) {
            final VanillaMethodWriterBuilder<All> methodWriterBuilder = queue.methodWriterBuilder(All.class);
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
    @DisplayName("Empty queue produces no reader output lines")
    public void shouldNotFailOnEmptyQueue() {
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        Path path = getTmpDir().toPath();
        path.toFile().mkdirs();
        basicReader(path).execute();
        assertTrue(capturedOutput.isEmpty(), "method reader should produce no output when reading from empty queue");
    }

    @NotNull
    private ChronicleReader basicReader(Path path) {
        if (OS.isWindows())
            if (!testMethodName.startsWith("shouldThrowExceptionIfInputDirectoryDoesNotExist"))
                expectException("Read-only mode is not supported on Windows");

        return new ChronicleReader().withBasePath(path).withMessageSink(capturedOutput::add);
    }

    @NotNull
    private ChronicleReader basicReaderMethodReader(Path path) {
        return basicReader(path).asMethodReader(All.class.getName());
    }

    // CPD-OFF - ChronicleReaderTest exercises the same behaviours
    @Test
    @DisplayName("Reader continues when metadata file is missing")
    public void shouldNotFailWhenNoMetadata() throws IOException {
        if (!OS.isWindows())
            expectException("Failback to readonly tablestore");
        Files.list(dataDir).filter(f -> f.getFileName().toString().endsWith(SingleTableStore.SUFFIX)).findFirst().ifPresent(path -> path.toFile().delete());
        basicReader().execute();
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), "reader should include message history when metadata file is missing");
    }

    @Test
    @DisplayName("Message history is included by default")
    public void shouldIncludeMessageHistoryByDefault() {
        basicReader().execute();

        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), "reader output should include message history by default");
    }

    @Test
    @DisplayName("Include regex applies to history and business messages")
    public void shouldApplyIncludeRegexToHistoryMessagesAndBusinessMessages() {
        basicReader()
                .withInclusionRegex("goodbye") // matches goodbye, but not hello or history
                .asMethodReader("")
                .execute();
        assertFalse(capturedOutput.stream().anyMatch(msg -> msg.contains("history:")), "inclusion regex should filter out history messages when pattern does not match");
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("method2")), "inclusion regex should include business messages matching the pattern");
    }

    @Test
    @DisplayName("Read-only queue file remains readable by reader")
    public void shouldBeAbleToReadFromReadOnlyFile() throws IOException {
        assumeFalse(OS.isWindows(), "#460 read-only not supported on Windows");
        final Path queueFile = Files.list(dataDir).
                filter(f -> f.getFileName().toString().endsWith(SingleChronicleQueue.SUFFIX)).findFirst().
                orElseThrow(() ->
                        new AssertionError("Could not find queue file in directory " + dataDir));

        assertTrue(queueFile.toFile().setWritable(false), "setting queue file to read-only mode should succeed for test setup");

        basicReader().execute();
    }

    @Test
    @DisplayName("Method reader converts entries to text")
    public void shouldConvertEntriesToTextMethodReader() {
        basicReaderMethodReader().execute();
        long msgCount =
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
                        //.peek(System.out::println)
                        .count();
        assertEquals(24, msgCount, "method reader should convert all queue entries to text output with expected message count");
        // "hello"
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("hello")), "method reader output should contain expected message content 'hello'");
        assertTrue(capturedOutput.stream().anyMatch(msg -> msg.contains("method1")), "method reader output should contain method name 'method1' for Method1Type messages");
    }

    @Test
    @DisplayName("Raw reader does not convert entries to text")
    public void shouldNotConvertEntriesToText() {
        basicReader().execute();
        long msgCount =
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
                        //.peek(System.out::println)
                        .count();
        assertEquals(24, msgCount, "raw reader should output all queue entries without method conversion");
        // "hello"
        assertTrue(capturedOutput.stream()
                        .anyMatch(msg -> msg.contains("  5,\n" +
                                "  104,\n" +
                                "  101,\n" +
                                "  108,\n" +
                                "  108,\n" +
                                "  111,")),
                "raw reader output should contain byte representation of 'hello' string without method conversion");
    }

    @Test
    @DisplayName("Inclusion regex filters matching messages only")
    public void shouldFilterByInclusionRegex() {
        basicReader().withInclusionRegex(".*good.*").execute();

        assertEquals(24, capturedOutput.size(), "inclusion regex should match expected number of messages with indices");
        capturedOutput.stream()
                .filter(msg -> !msg.startsWith("0x"))
                .forEach(msg -> assertTrue(msg.contains("goodbye"),
                        "filtered output should contain goodbye message, actual=" + msg));
    }

    @Test
    @DisplayName("Multiple inclusion regexes filter matching messages")
    public void shouldFilterByMultipleInclusionRegex() {
        basicReader()
                .withInclusionRegex(".*bye.*")
                .withInclusionRegex(".*o.*")
                .execute();

        assertEquals(24, capturedOutput.size(), "multiple inclusion regexes should match expected number of messages with indices");
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x"))
                .forEach(msg -> assertTrue(msg.contains("goodbye"),
                        "filtered output should include goodbye message, actual=" + msg));
        capturedOutput.stream().filter(msg -> !msg.startsWith("0x"))
                .forEach(msg -> assertFalse(msg.contains("hello"),
                        "filtered output should exclude hello message, actual=" + msg));
    }

    public void shouldThrowExceptionIfInputDirectoryDoesNotExist() {
        assertThrows(IllegalArgumentException.class, () -> basicReader().withBasePath(Paths.get("/does/not/exist")).execute(),
                "reader should throw IllegalArgumentException when input directory does not exist");
    }

    @Test
    @DisplayName("Exclusion regex removes matching business messages")
    public void shouldFilterByExclusionRegex() {
        basicReader().withExclusionRegex(".*good.*").execute();

        long msgCount =
                capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
                        // .peek(System.out::println)
                        .count();
        assertEquals(12, msgCount, "exclusion regex should produce expected number of filtered messages");
        capturedOutput.forEach(msg -> assertFalse(msg.contains("goodbye"),
                "filtered output should exclude goodbye message, actual=" + msg));
    }

    @Disabled("https://github.com/OpenHFT/Chronicle-Queue/issues/1150")
    public void shouldFilterByMultipleExclusionRegex() {
        basicReaderMethodReader().withExclusionRegex(".*bye$").withExclusionRegex(".*ell.*").execute();

        assertEquals(0L, capturedOutput.stream().filter(msg -> !msg.startsWith("0x")).count(), "multiple exclusion regexes matching all messages should produce no content output");
    }
    // CPD-ON

    @Test
    @DisplayName("History record limit caps output size")
    public void shouldReturnNoMoreThanTheSpecifiedNumberOfMaxRecords() {
        basicReaderMethodReader().historyRecords(5).execute();

        assertEquals(5, capturedOutput.stream()
                .filter(msg -> !msg.startsWith("0x")).count(), "history records limit should restrict output to specified number of messages");
    }

    public void shouldFailIfSpecifiedIndexIsBeforeFirstIndex() {
        assertThrows(IllegalArgumentException.class, () -> basicReader().withStartIndex(1L).execute(),
                "reader should throw IllegalArgumentException when start index is before first available index");
    }

    @Test
    @DisplayName("History display does not rewind before queue start")
    public void shouldNotRewindPastStartOfQueueWhenDisplayingHistory() {
        basicReader().historyRecords(Long.MAX_VALUE).execute();

        assertEquals(24, capturedOutput.stream()
                        .filter(msg -> !msg.startsWith("0x"))
                .count(), "history records with large limit should not rewind past queue start");
    }

    private ChronicleReader basicReader() {
        return basicReader(dataDir);
    }

    private ChronicleReader basicReaderMethodReader() {
        return basicReaderMethodReader(dataDir);
    }

    @AfterEach
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
