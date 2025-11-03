/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DumpMainTest extends QueueTestCommon {

    @Test
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

        assertFalse("Dump should not be empty", dump.trim().isEmpty());
        assertTrue("Should include header with file path", dump.contains("## "));
        assertTrue("Should include first message", dump.contains("hello"));
        assertTrue("Should include second message", dump.contains("world"));
    }
}
