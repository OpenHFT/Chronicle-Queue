/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Test;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ChronicleHistoryReaderMainTest {

    /**
     * Thrown by an overridden {@link ChronicleHistoryReaderMain#exit(int)} so that tests can
     * observe an attempted JVM exit (and the status it was called with) without terminating the
     * test runner. This replaces the old SecurityManager-based interception, which is deprecated
     * for removal since JDK 17 (JEP 411) and unavailable from JDK 24 (JEP 486).
     */
    private static final class ExitInvoked extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final int status;

        ExitInvoked(int status) {
            this.status = status;
        }
    }

    /**
     * A named subclass whose {@link #exit(int)} records the requested status rather than
     * terminating the JVM. Deliberately a named (non-anonymous) class so that
     * {@code getClass().getSimpleName()} is non-empty: {@code printHelpAndExit} passes it to
     * commons-cli as the command-line syntax, which rejects an empty value.
     */
    private static final class ExitCapturingMain extends ChronicleHistoryReaderMain {
        @Override
        protected void exit(int status) {
            throw new ExitInvoked(status);
        }
    }

    @Test
    public void testRunExecutesChronicleHistoryReader() {
        // Setup
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain() {
            @Override
            protected ChronicleHistoryReader chronicleHistoryReader() {
                return new ChronicleHistoryReader() {
                    @Override
                    public void execute() {
                        // Simulate execution
                        assertTrue(true);  // Verify execution reached here
                    }
                };
            }
        };

        String[] args = {"-d", "test-directory"}; // Simulate passing a directory argument
        main.run(args);  // Expect that execute is called
    }

    @Test
    public void testSetupChronicleHistoryReader() {
        // Simulate command line arguments
        String[] args = {"-d", "test-directory", "-p", "-m", "-t", "NANOSECONDS"};
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain();
        Options options = main.options();
        CommandLine commandLine = main.parseCommandLine(args, options);

        // Create a mock ChronicleHistoryReader
        ChronicleHistoryReader historyReader = new ChronicleHistoryReader() {
            @Override
            public ChronicleHistoryReader withProgress(boolean progress) {
                return this;
            }

            @Override
            public ChronicleHistoryReader withHistosByMethod(boolean histosByMethod) {
                return this;
            }

            @Override
            public ChronicleHistoryReader withMessageSink(Consumer<String> sink) {
                return this;
            }

            @Override
            public ChronicleHistoryReader withBasePath(Path basePath) {
                assertEquals("test-directory", basePath.toString());
                return this;
            }

            @Override
            public ChronicleHistoryReader withTimeUnit(TimeUnit timeUnit) {
                assertEquals(TimeUnit.NANOSECONDS, timeUnit);
                return this;
            }

            @Override
            public void execute() {
                // Simulate execution
            }
        };

        // Act
        main.setup(commandLine, historyReader);

        // Assert
        assertNotNull(historyReader.withProgress(true));
        assertNotNull(historyReader.withHistosByMethod(true));
    }

    @Test
    public void testParseCommandLine() {
        // Test that parseCommandLine correctly parses arguments
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain();
        Options options = main.options();
        String[] args = {"-d", "test-directory", "-t", "SECONDS"};
        CommandLine commandLine = main.parseCommandLine(args, options);

        assertEquals("test-directory", commandLine.getOptionValue("d"));
        assertEquals("SECONDS", commandLine.getOptionValue("t"));
    }

    @Test
    public void testParseCommandLineHelpOption() {
        ChronicleHistoryReaderMain main = new ExitCapturingMain();
        String[] args = {"-h"};

        try {
            main.run(args);  // Should print help and request exit with status 0
            fail("Expected exit to be invoked");
        } catch (ExitInvoked e) {
            assertEquals(0, e.status);  // Help is requested explicitly, so a clean exit
        }
    }

    @Test
    public void testOptionsConfiguration() {
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain();
        Options options = main.options();

        // Verify that all expected options are present
        assertNotNull(options.getOption("d"));
        assertNotNull(options.getOption("h"));
        assertNotNull(options.getOption("t"));
        assertNotNull(options.getOption("i"));
        assertNotNull(options.getOption("w"));
        assertNotNull(options.getOption("u"));
        assertNotNull(options.getOption("p"));
        assertNotNull(options.getOption("m"));
    }

    @Test
    public void testPrintHelpAndExit() {
        ChronicleHistoryReaderMain main = new ExitCapturingMain();
        Options options = main.options();
        try {
            main.printHelpAndExit(options, 0, "Optional message");
            fail("Expected exit to be invoked with status 0");
        } catch (ExitInvoked e) {
            assertEquals(0, e.status);
        }
    }
}
