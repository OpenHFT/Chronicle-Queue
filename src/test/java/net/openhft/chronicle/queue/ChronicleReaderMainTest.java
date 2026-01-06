/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.cli.Options;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests cover ChronicleReaderMain CLI option wiring and execution behaviour.
 */
public class ChronicleReaderMainTest extends QueueTestCommon {

    @Test
    @DisplayName("CLI main runs with a temporary directory")
    public void testMainWithValidArguments() {
        ignoreException("Metadata file not found in readOnly mode");
        try {
            // Create a temporary directory for the test
            Path tempDir = Files.createTempDirectory("testDirectory");

            String[] args = {"-d", tempDir.toString()};

            // Capture System.out and System.err using try-with-resources
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            try (ByteArrayOutputStream outContent = new ByteArrayOutputStream();
                 ByteArrayOutputStream errContent = new ByteArrayOutputStream();
                 PrintStream outPs = new PrintStream(outContent);
                 PrintStream errPs = new PrintStream(errContent)) {
                System.setOut(outPs);
                System.setErr(errPs);

                ChronicleReaderMain.main(args);  // Run the main method with valid args

                assertEquals(0, errContent.size(), "stderr should remain empty for valid arguments");
            } finally {
                // Reset System.out and System.err
                System.setOut(originalOut);
                System.setErr(originalErr);
            }

            // Clean up: delete the temporary directory
            File dir = tempDir.toFile();
            if (dir.exists()) {
                dir.delete();
            }

        } catch (Exception e) {
            fail("No exception should be thrown with valid arguments: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Reader options include all supported flags")
    public void testOptionsConfiguration() {
        ChronicleReaderMain main = new ChronicleReaderMain();
        Options options = main.options();

        // Verify options are set correctly
        assertNotNull(options.getOption("d"), "options should include -d directory flag");  // Directory option
        assertNotNull(options.getOption("i"), "options should include -i include regex flag");  // Include regex
        assertNotNull(options.getOption("e"), "options should include -e exclude regex flag");  // Exclude regex
        assertNotNull(options.getOption("f"), "options should include -f follow flag");  // Follow (tail) option
        assertNotNull(options.getOption("m"), "options should include -m max history flag");  // Max history
        assertNotNull(options.getOption("n"), "options should include -n start index flag");  // Start index
        assertNotNull(options.getOption("b"), "options should include -b binary search flag");  // Binary search
        assertNotNull(options.getOption("a"), "options should include -a binary argument flag");  // Binary argument
        assertNotNull(options.getOption("r"), "options should include -r method reader flag");  // As method reader
        assertNotNull(options.getOption("g"), "options should include -g message history flag");  // Message history
        assertNotNull(options.getOption("w"), "options should include -w wire type flag");  // Wire type
        assertNotNull(options.getOption("s"), "options should include -s suppress index flag");  // Suppress index
        assertNotNull(options.getOption("l"), "options should include -l single line flag");  // Single line squash
        assertNotNull(options.getOption("z"), "options should include -z local timezone flag");  // Use local timezone
        assertNotNull(options.getOption("k"), "options should include -k reverse order flag");  // Reverse order
        assertNotNull(options.getOption("x"), "options should include -x max results flag");  // Max results
        assertNotNull(options.getOption("cbl"), "options should include -cbl content limiter flag");  // Content-based limiter
        assertNotNull(options.getOption("named"), "options should include --named tailer flag");  // Named tailer ID
    }
}
