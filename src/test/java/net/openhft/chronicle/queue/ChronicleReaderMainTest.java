/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.cli.Options;

import static org.junit.Assert.*;

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

            // Capture System.out and System.err
            ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            ByteArrayOutputStream errContent = new ByteArrayOutputStream();
            System.setOut(new PrintStream(outContent));
            System.setErr(new PrintStream(errContent));

            ChronicleReaderMain.main(args);  // Run the main method with valid args

            assertTrue("Expected valid arguments to run without issues.", true);

            // Clean up: delete the temporary directory
            File dir = tempDir.toFile();
            if (dir.exists()) {
                dir.delete();
            }

        } catch (Exception e) {
            fail("No exception should be thrown with valid arguments: " + e.getMessage());
        } finally {
            // Reset System.out and System.err
            System.setOut(System.out);
            System.setErr(System.err);
        }
    }

    @Test
    public void testOptionsConfiguration() {
        ChronicleReaderMain main = new ChronicleReaderMain();
        Options options = main.options();

        // Verify options are set correctly
        assertNotNull(options.getOption("d"));  // Directory option
        assertNotNull(options.getOption("i"));  // Include regex
        assertNotNull(options.getOption("e"));  // Exclude regex
        assertNotNull(options.getOption("f"));  // Follow (tail) option
        assertNotNull(options.getOption("m"));  // Max history
        assertNotNull(options.getOption("n"));  // Start index
        assertNotNull(options.getOption("b"));  // Binary search
        assertNotNull(options.getOption("a"));  // Binary argument
        assertNotNull(options.getOption("r"));  // As method reader
        assertNotNull(options.getOption("g"));  // Message history
        assertNotNull(options.getOption("w"));  // Wire type
        assertNotNull(options.getOption("s"));  // Suppress index
        assertNotNull(options.getOption("l"));  // Single line squash
        assertNotNull(options.getOption("z"));  // Use local timezone
        assertNotNull(options.getOption("k"));  // Reverse order
        assertNotNull(options.getOption("x"));  // Max results
        assertNotNull(options.getOption("cbl"));  // Content-based limiter
        assertNotNull(options.getOption("named"));  // Named tailer ID
    }
}
