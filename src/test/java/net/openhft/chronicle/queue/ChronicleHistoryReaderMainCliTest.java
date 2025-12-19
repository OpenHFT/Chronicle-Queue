/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ChronicleHistoryReaderMainCliTest extends QueueTestCommon {

    @Test
    public void runConfiguresReaderFromArguments() throws Exception {
        final Path queueDir = Files.createTempDirectory("history-reader");
        final ChronicleHistoryReaderMainStub main = new ChronicleHistoryReaderMainStub();

        main.run(new String[]{"-d", queueDir.toString(), "-p", "-m", "-t", "SECONDS", "-i", "2", "-w", "5", "-u", "1"});

        RecordingChronicleHistoryReader reader = main.reader;
        assertTrue(reader.executeCalled, "ChronicleHistoryReader execute method should be called when run completes");
        assertTrue(reader.closed, "ChronicleHistoryReader should be closed after run completes");
        assertEquals(queueDir, reader.basePath(), "Base path should be set to queue directory from -d argument");
        assertTrue(reader.progress(), "Progress reporting should be enabled when -p flag is provided");
        assertTrue(reader.histosByMethod(), "Histograms by method should be enabled when -m flag is provided");
        assertEquals(TimeUnit.SECONDS, reader.timeUnit(), "Time unit should be set to SECONDS from -t argument");
        assertEquals(2L, reader.ignoreCount(), "Ignore count should be set to 2 from -i argument");
        assertEquals(TimeUnit.SECONDS.toNanos(5), reader.measurementWindowNanos(), "Measurement window should be set to 5 seconds in nanos from -w argument");
        assertEquals(1, reader.summaryOutputOffset(), "Summary output offset should be set to 1 from -u argument");
        assertNotNull(reader.messageSink(), "Message sink should be configured for output");
    }

    @Test
    public void parseCommandLineWithHelpOption() {
        final ChronicleHistoryReaderMainStub main = new ChronicleHistoryReaderMainStub();

        try {
            main.parseCommandLine(new String[]{"-h"}, main.options());
            fail("Expected HelpExit");
        } catch (HelpExit e) {
            assertEquals(0, e.status, "Help option should trigger exit with success status code 0");
            assertTrue(main.helpOutput.toString().contains("ChronicleHistoryReaderMain"), "Help output should contain ChronicleHistoryReaderMain class name");
        }
    }

    @Test
    public void parseCommandLineMissingDirectoryPrintsError() {
        final ChronicleHistoryReaderMainStub main = new ChronicleHistoryReaderMainStub();

        try {
            main.parseCommandLine(new String[]{"-t", "SECONDS"}, main.options());
            fail("Expected HelpExit");
        } catch (HelpExit e) {
            assertEquals(1, e.status, "Missing required directory option should trigger exit with error status code 1");
            assertTrue(main.helpOutput.toString().contains("Missing required option"), "Help output should contain error message about missing required option");
        }
    }

    private static final class ChronicleHistoryReaderMainStub extends ChronicleHistoryReaderMain {
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
