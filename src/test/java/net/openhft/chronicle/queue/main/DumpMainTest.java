/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"deprecation", "removal"})
public class DumpMainTest extends QueueTestCommon {

    @Test
    @DisplayName("Dump prints queue messages to output")
    public void dumpDirectoryPrintsQueueContents() {
        final File dir = getTmpDir();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).build()) {
            // write a couple of simple messages
            try (DocumentContext dc = q.createAppender().writingDocument()) {
                dc.wire().write("msg").text("hello");
            }
            try (DocumentContext dc = q.createAppender().writingDocument()) {
                dc.wire().write("msg").text("world");
            }
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(baos);
        DumpMain.dump(dir, out, Long.MAX_VALUE);
        final String dump = baos.toString();

        assertFalse(dump.trim().isEmpty(), "Dump output should not be empty after writing messages");
        assertTrue(dump.contains("## "), "Dump output should include header prefix '## '");
        assertTrue(dump.contains("hello"), "Dump output should include message text 'hello'");
        assertTrue(dump.contains("world"), "Dump output should include message text 'world'");
    }
}
