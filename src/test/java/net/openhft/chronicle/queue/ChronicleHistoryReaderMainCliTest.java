/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ChronicleHistoryReaderMainCliTest extends QueueTestCommon {

    @Test
    public void runConfiguresReaderFromArguments() throws Exception {
        final Path queueDir = Files.createTempDirectory("history-reader");
        final TestChronicleHistoryReaderMain main = new TestChronicleHistoryReaderMain();

        main.run(new String[]{"-d", queueDir.toString(), "-p", "-m", "-t", "SECONDS", "-i", "2", "-w", "5", "-u", "1"});

        RecordingChronicleHistoryReader reader = main.reader;
        assertTrue(reader.executeCalled);
        assertTrue(reader.closed);
        assertEquals(queueDir, reader.basePath());
        assertTrue(reader.progress());
        assertTrue(reader.histosByMethod());
        assertEquals(TimeUnit.SECONDS, reader.timeUnit());
        assertEquals(2L, reader.ignoreCount());
        assertEquals(TimeUnit.SECONDS.toNanos(5), reader.measurementWindowNanos());
        assertEquals(1, reader.summaryOutputOffset());
        assertNotNull(reader.messageSink());
    }

    @Test
    public void parseCommandLineWithHelpOption() {
        final TestChronicleHistoryReaderMain main = new TestChronicleHistoryReaderMain();

        try {
            main.parseCommandLine(new String[]{"-h"}, main.options());
            fail("Expected HelpExit");
        } catch (HelpExit e) {
            assertEquals(0, e.status);
            assertTrue(main.helpOutput.toString().contains("ChronicleHistoryReaderMain"));
        }
    }

    @Test
    public void parseCommandLineMissingDirectoryPrintsError() {
        final TestChronicleHistoryReaderMain main = new TestChronicleHistoryReaderMain();

        try {
            main.parseCommandLine(new String[]{"-t", "SECONDS"}, main.options());
            fail("Expected HelpExit");
        } catch (HelpExit e) {
            assertEquals(1, e.status);
            assertTrue(main.helpOutput.toString().contains("Missing required option"));
        }
    }

    private static final class TestChronicleHistoryReaderMain extends ChronicleHistoryReaderMain {
        final RecordingChronicleHistoryReader reader = new RecordingChronicleHistoryReader();
        final StringBuilder helpOutput = new StringBuilder();

        @Override
        protected ChronicleHistoryReader chronicleHistoryReader() {
            return reader;
        }

        @Override
        protected void printHelpAndExit(Options options, int status, String message) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            new org.apache.commons.cli.HelpFormatter().printHelp(
                    pw,
                    180,
                    this.getClass().getSimpleName(),
                    message,
                    options,
                    org.apache.commons.cli.HelpFormatter.DEFAULT_LEFT_PAD,
                    org.apache.commons.cli.HelpFormatter.DEFAULT_DESC_PAD,
                    null,
                    true
            );
            pw.flush();
            helpOutput.append(sw);
            throw new HelpExit(status);
        }

        @Override
        protected CommandLine parseCommandLine(String[] args, Options options) {
            return super.parseCommandLine(args, options);
        }
    }

    private static final class RecordingChronicleHistoryReader extends ChronicleHistoryReader {
        boolean executeCalled;
        boolean closed;

        @Override
        public ChronicleHistoryReader withMessageSink(java.util.function.Consumer<String> messageSink) {
            return super.withMessageSink(messageSink);
        }

        @Override
        public ChronicleHistoryReader withBasePath(Path path) {
            return super.withBasePath(path);
        }

        @Override
        public ChronicleHistoryReader withProgress(boolean p) {
            return super.withProgress(p);
        }

        @Override
        public ChronicleHistoryReader withTimeUnit(TimeUnit p) {
            return super.withTimeUnit(p);
        }

        @Override
        public ChronicleHistoryReader withHistosByMethod(boolean b) {
            return super.withHistosByMethod(b);
        }

        @Override
        public ChronicleHistoryReader withIgnore(long ignore) {
            return super.withIgnore(ignore);
        }

        @Override
        public ChronicleHistoryReader withMeasurementWindow(long measurementWindow) {
            return super.withMeasurementWindow(measurementWindow);
        }

        @Override
        public ChronicleHistoryReader withSummaryOutput(int offset) {
            return super.withSummaryOutput(offset);
        }

        @Override
        public void execute() {
            executeCalled = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        Path basePath() {
            return basePath;
        }

        boolean progress() {
            return progress;
        }

        boolean histosByMethod() {
            return histosByMethod;
        }

        TimeUnit timeUnit() {
            return timeUnit;
        }

        long ignoreCount() {
            return ignore;
        }

        long measurementWindowNanos() {
            return measurementWindowNanos;
        }

        int summaryOutputOffset() {
            return summaryOutputOffset;
        }

        java.util.function.Consumer<String> messageSink() {
            return messageSink;
        }
    }

    private static final class HelpExit extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final int status;

        HelpExit(int status) {
            this.status = status;
        }
    }
}
