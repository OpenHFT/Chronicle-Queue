/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

public class ChronicleReaderMainCliTest extends QueueTestCommon {

    @Test
    public void mainReadsAndPrintsQueueRecords() {
        final java.io.File dir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("hello");
        }

        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream capture = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capture));
        try {
            ChronicleReaderMain.main(new String[]{"-d", dir.getAbsolutePath()});
        } finally {
            System.setOut(originalOut);
        }

        final String out = capture.toString();
        assertTrue(out.contains("hello"), "Expected output to contain written text");
    }

    @Test
    public void invalidContentBasedLimiterClassThrows() {
        final java.io.File dir = getTmpDir();
        ChronicleReaderMain main = new ChronicleReaderMain();
        assertThrows(IllegalArgumentException.class,
                () -> main.run(new String[]{"-d", dir.getAbsolutePath(), "-cbl", "not.a.RealClass"}),
                "content based limiter: invalid class");
    }

    @Test
    public void invalidBinarySearchComparatorClassThrows() {
        final java.io.File dir = getTmpDir();
        ChronicleReaderMain main = new ChronicleReaderMain();
        assertThrows(ClassNotFoundException.class,
                () -> main.run(new String[]{"-d", dir.getAbsolutePath(), "-b", "not.a.RealClass"}),
                "binary search comparator: invalid class");
    }

    @Test
    public void mainHonoursStartIndex() {
        final java.io.File dir = getTmpDir();
        long secondIndex;
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("first");
            appender.writeText("second");
            secondIndex = appender.lastIndexAppended();
            appender.writeText("third");
        }

        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream capture = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capture));
        try {
            ChronicleReaderMain.main(new String[]{
                    "-d", dir.getAbsolutePath(),
                    "-n", Long.toString(secondIndex)
            });
        } finally {
            System.setOut(originalOut);
        }

        final String out = capture.toString();
        assertTrue(out.contains("second"), "CLI output should contain second message when starting from second index");
        assertTrue(out.contains("third"), "CLI output should contain third message when starting from second index");
        assertFalse(out.contains("first"), "Start index should skip earlier entries");
    }

    @Test
    public void methodReaderOptionsEnableMessageHistory() {
        final java.io.File dir = getTmpDir();
        ChronicleReaderMainStub main = new ChronicleReaderMainStub();

        main.run(new String[]{
                "-d", dir.getAbsolutePath(),
                "-r", Runnable.class.getName(),
                "-g",
                "-w", "TEXT"
        });

        RecordingChronicleReader reader = main.reader;
        assertTrue(reader.executed, "ChronicleReader should have executed when run with method reader options");
        assertTrue(reader.showHistory, "Message history should be enabled when -g flag is provided");
        assertSame(Runnable.class, reader.methodReaderInterfaceSnapshot, "Method reader interface should be set to Runnable when specified via -r flag");
        assertSame(WireType.TEXT, reader.wireTypeSnapshot, "Wire type should be set to TEXT when specified via -w flag");
    }

    private static final class ChronicleReaderMainStub extends ChronicleReaderMain {
        final RecordingChronicleReader reader = new RecordingChronicleReader();

        @Override
        protected ChronicleReader chronicleReader() {
            return reader;
        }
    }

    private static final class RecordingChronicleReader extends ChronicleReader {
        boolean executed;
        boolean showHistory;
        Class<?> methodReaderInterfaceSnapshot;
        WireType wireTypeSnapshot;

        @Override
        public ChronicleReader showMessageHistory(boolean showMessageHistory) {
            this.showHistory = showMessageHistory;
            return super.showMessageHistory(showMessageHistory);
        }

        @Override
        public ChronicleReader asMethodReader(@NotNull String methodReaderInterface) {
            if (!methodReaderInterface.isEmpty()) {
                try {
                    methodReaderInterfaceSnapshot = Class.forName(methodReaderInterface);
                } catch (ClassNotFoundException e) {
                    throw Jvm.rethrow(e);
                }
            }
            return super.asMethodReader(methodReaderInterface);
        }

        @Override
        public ChronicleReader withWireType(@NotNull WireType wireType) {
            this.wireTypeSnapshot = wireType;
            return super.withWireType(wireType);
        }

        @Override
        public void execute() {
            executed = true;
        }
    }
}
