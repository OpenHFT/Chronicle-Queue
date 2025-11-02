/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertTrue;

public class ChronicleReaderMainCliTest extends QueueTestCommon {

    @Test
    public void mainReadsAndPrintsQueueRecords() {
        final java.io.File dir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("hello");
        }

        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream capture = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capture));
        try {
            ChronicleReaderMain.main(new String[]{"-d", dir.getAbsolutePath()});
        } finally {
            System.setOut(originalOut);
        }

        final String out = capture.toString();
        assertTrue("Expected output to contain written text", out.contains("hello"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidContentBasedLimiterClassThrows() {
        final java.io.File dir = getTmpDir();
        ChronicleReaderMain main = new ChronicleReaderMain();
        main.run(new String[]{"-d", dir.getAbsolutePath(), "-cbl", "not.a.RealClass"});
    }

    @Test(expected = ClassNotFoundException.class)
    public void invalidBinarySearchComparatorClassThrows() {
        final java.io.File dir = getTmpDir();
        ChronicleReaderMain main = new ChronicleReaderMain();
        main.run(new String[]{"-d", dir.getAbsolutePath(), "-b", "not.a.RealClass"});
    }
}
