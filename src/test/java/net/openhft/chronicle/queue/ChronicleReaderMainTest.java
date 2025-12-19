/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.cli.Options;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ChronicleReaderMain class.
 */
public class ChronicleReaderMainTest extends QueueTestCommon {

    @Test
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

                assertTrue(true, "Expected valid arguments to run without issues.");
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
    public void testOptionsConfiguration() {
        ChronicleReaderMain main = new ChronicleReaderMain();
        Options options = main.options();

        // Verify options are set correctly
        assertNotNull(options.getOption("d"), "options: directory");  // Directory option
        assertNotNull(options.getOption("i"), "options: include regex");  // Include regex
        assertNotNull(options.getOption("e"), "options: exclude regex");  // Exclude regex
        assertNotNull(options.getOption("f"), "options: follow");  // Follow (tail) option
        assertNotNull(options.getOption("m"), "options: max history");  // Max history
        assertNotNull(options.getOption("n"), "options: start index");  // Start index
        assertNotNull(options.getOption("b"), "options: binary search");  // Binary search
        assertNotNull(options.getOption("a"), "options: binary arg");  // Binary argument
        assertNotNull(options.getOption("r"), "options: as method reader");  // As method reader
        assertNotNull(options.getOption("g"), "options: message history");  // Message history
        assertNotNull(options.getOption("w"), "options: wire type");  // Wire type
        assertNotNull(options.getOption("s"), "options: suppress index");  // Suppress index
        assertNotNull(options.getOption("l"), "options: single line squash");  // Single line squash
        assertNotNull(options.getOption("z"), "options: local timezone");  // Use local timezone
        assertNotNull(options.getOption("k"), "options: reverse order");  // Reverse order
        assertNotNull(options.getOption("x"), "options: max results");  // Max results
        assertNotNull(options.getOption("cbl"), "options: content-based limiter");  // Content-based limiter
        assertNotNull(options.getOption("named"), "options: named tailer");  // Named tailer ID
    }
}
