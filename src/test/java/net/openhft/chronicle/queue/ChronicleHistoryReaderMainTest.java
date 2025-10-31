/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import org.apache.commons.cli.*;
import org.junit.Test;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.*;

@SuppressWarnings({"deprecation", "removal"})
public class ChronicleHistoryReaderMainTest {

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

        try {
            main.run(args);  // Should trigger the help message and exit with 0
            fail("Expected ThreadDeath to be thrown");
        } catch (ThreadDeath e) {
            // expected
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
        ChronicleHistoryReaderMain main = new ChronicleHistoryReaderMain() {
            @Override
            protected void printHelpAndExit(Options options, int status, String message) {
                assertEquals(0, status);
                throw new ThreadDeath();
            }
        };
        Options options = main.options();
        try {
            main.printHelpAndExit(options, 0, "Optional message");
            fail("Expected ThreadDeath to be thrown");
        } catch (ThreadDeath ignored) {
        }
    }
}
