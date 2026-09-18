/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import org.apache.commons.cli.Options;

import static org.junit.Assert.*;

/**
 * Unit tests for ChronicleReaderMain class.
 */
public class ChronicleReaderMainTest extends QueueTestCommon {

    @Test
    public void testMainWithValidArguments() throws Exception {
        ignoreException("Metadata file not found in readOnly mode");
        // The reader may create metadata; the common fixture drains and deletes the whole directory.
        String[] args = {"-d", Files.createDirectories(getTmpDir().toPath()).toString()};
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        try (ByteArrayOutputStream outContent = new ByteArrayOutputStream();
             ByteArrayOutputStream errContent = new ByteArrayOutputStream();
             PrintStream outPs = new PrintStream(outContent);
             PrintStream errPs = new PrintStream(errContent)) {
            System.setOut(outPs);
            System.setErr(errPs);
            ChronicleReaderMain.main(args);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
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
