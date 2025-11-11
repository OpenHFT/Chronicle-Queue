/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.reader.ChronicleReader;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertTrue;

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
        assertTrue("Expected output to contain written text", out.contains("hello"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidContentBasedLimiterClassThrows() {
        final java.io.File dir = getTmpDir();
        ChronicleReaderMain main = new ChronicleReaderMain();
        main.run(new String[]{"-d", dir.getAbsolutePath(), "-cbl", "not.a.RealClass"});
    }

    @Test(expected = ClassNotFoundException.class)
    public void invalidBinarySearchComparatorClassThrows() {
        final java.io.File dir = getTmpDir();
        ChronicleReaderMain main = new ChronicleReaderMain();
        main.run(new String[]{"-d", dir.getAbsolutePath(), "-b", "not.a.RealClass"});
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
        assertTrue(out.contains("second"));
        assertTrue(out.contains("third"));
        assertTrue("Start index should skip earlier entries", !out.contains("first"));
    }

    @Test
    public void methodReaderOptionsEnableMessageHistory() {
        final java.io.File dir = getTmpDir();
        TestChronicleReaderMain main = new TestChronicleReaderMain();

        main.run(new String[]{
                "-d", dir.getAbsolutePath(),
                "-r", Runnable.class.getName(),
                "-g",
                "-w", "TEXT"
        });

        RecordingChronicleReader reader = main.reader;
        assertTrue(reader.executed);
        assertTrue(reader.showHistory);
        assertTrue(reader.methodReaderInterfaceSnapshot == Runnable.class);
        assertTrue(reader.wireTypeSnapshot == WireType.TEXT);
    }

    private static final class TestChronicleReaderMain extends ChronicleReaderMain {
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
