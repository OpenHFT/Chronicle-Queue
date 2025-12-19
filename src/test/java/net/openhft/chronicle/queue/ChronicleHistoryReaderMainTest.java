/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.security.Permission;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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

    @BeforeEach
    public void setUp() {
        // SecurityManager is effectively disabled from JDK 17 onwards
        assumeTrue(Jvm.majorVersion() < 17);
        System.setSecurityManager(new NoExitSecurityManager());
    }

    @AfterEach
    public void tearDown() {
        if (Jvm.majorVersion() < 17)
            System.setSecurityManager(null);
    }

    @Test
    public void testRunExecutesChronicleHistoryReader() {
        AtomicBoolean executed = new AtomicBoolean();
        // Setup
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain() {
            @Override
            protected ChronicleHistoryReader chronicleHistoryReader() {
                return new ChronicleHistoryReader() {
                    @Override
                    public void execute() {
                        executed.set(true);
                    }
                };
            }
        };

        String[] args = {"-d", "test-directory"}; // Simulate passing a directory argument
        main.run(args);  // Expect that execute is called
        assertTrue(executed.get(), "ChronicleHistoryReader.execute should be invoked when run method is called with directory argument");
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
                assertEquals("test-directory", basePath.toString(), "Base path should be set to test-directory from command line argument");
                return this;
            }

            @Override
            public ChronicleHistoryReader withTimeUnit(TimeUnit timeUnit) {
                assertEquals(TimeUnit.NANOSECONDS, timeUnit, "Time unit should be set to NANOSECONDS from -t command line argument");
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
        assertNotNull(historyReader.withProgress(true), "withProgress should return non-null reader for method chaining");
        assertNotNull(historyReader.withHistosByMethod(true), "withHistosByMethod should return non-null reader for method chaining");
    }

    @Test
    public void testParseCommandLine() {
        // Test that parseCommandLine correctly parses arguments
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain();
        Options options = main.options();
        String[] args = {"-d", "test-directory", "-t", "SECONDS"};
        CommandLine commandLine = main.parseCommandLine(args, options);

        assertEquals("test-directory", commandLine.getOptionValue("d"), "Directory option -d should be parsed correctly from command line arguments");
        assertEquals("SECONDS", commandLine.getOptionValue("t"), "Time unit option -t should be parsed correctly from command line arguments");
    }

    @Test
    public void testParseCommandLineHelpOption() {
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain() {
            @Override
            protected void printHelpAndExit(Options options, int status, String message) {
                assertEquals(0, status, "Help option should exit with success status code 0");  // Ensure help is printed with status 0 (success)
                throw new ThreadDeath();  // Exit without calling System.exit()
            }
        };
        String[] args = {"-h"};

        // Manually setting the security manager to catch System.exit() if needed
        assertThrows(ThreadDeath.class, () -> main.run(args));  // Should trigger the help message and exit with 0
    }

    @Test
    public void testOptionsConfiguration() {
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain();
        Options options = main.options();

        // Verify that all expected options are present
        assertNotNull(options.getOption("d"), "Directory option -d should be configured in options");
        assertNotNull(options.getOption("h"), "Help option -h should be configured in options");
        assertNotNull(options.getOption("t"), "Time unit option -t should be configured in options");
        assertNotNull(options.getOption("i"), "Ignore count option -i should be configured in options");
        assertNotNull(options.getOption("w"), "Measurement window option -w should be configured in options");
        assertNotNull(options.getOption("u"), "Summary output offset option -u should be configured in options");
        assertNotNull(options.getOption("p"), "Progress option -p should be configured in options");
        assertNotNull(options.getOption("m"), "Histograms by method option -m should be configured in options");
    }

    @Test
    public void testPrintHelpAndExit() {
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain();
        Options options = main.options();
        try {
            main.printHelpAndExit(options, 0, "Optional message");
            fail("Expected SecurityException due to System.exit(0)");
        } catch (SecurityException e) {
            assertTrue(e.getMessage().contains("System exit attempted with status: 0"), "Security exception should indicate system exit was attempted with status 0");
        }
    }
}
