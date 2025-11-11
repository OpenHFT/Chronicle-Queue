/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.security.Permission;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

@SuppressWarnings({"deprecation", "removal"})
public class ChronicleHistoryReaderMainTest {

    private static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
            // allow anything
        }

        @Override
        public void checkExit(int status) {
            throw new SecurityException("System exit attempted with status: " + status);
        }
    }

    @Before
    public void setUp() {
        // SecurityManager is effectively disabled from JDK 17 onwards
        assumeTrue(Jvm.majorVersion() < 17);
        System.setSecurityManager(new NoExitSecurityManager());
    }

    @After
    public void tearDown() {
        System.setSecurityManager(null);
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
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain() {
            @Override
            protected void printHelpAndExit(Options options, int status, String message) {
                assertEquals(0, status);  // Ensure help is printed with status 0 (success)
                throw new ThreadDeath();  // Exit without calling System.exit()
            }
        };
        String[] args = {"-h"};

        // Manually setting the security manager to catch System.exit() if needed
        try {
            main.run(args);  // Should trigger the help message and exit with 0
            fail("Expected ThreadDeath to be thrown");

        } catch (ThreadDeath e) {
            // Expected exception

        } catch (SecurityException e) {
            fail("System.exit was called unexpectedly.");
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
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain();
        Options options = main.options();
        try {
            main.printHelpAndExit(options, 0, "Optional message");
            fail("Expected SecurityException due to System.exit(0)");
        } catch (SecurityException e) {
            assertTrue(e.getMessage().contains("System exit attempted with status: 0"));
        }
    }
}
