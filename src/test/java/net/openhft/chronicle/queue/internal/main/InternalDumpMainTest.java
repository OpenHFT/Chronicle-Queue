/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InternalDumpMainTest extends QueueTestCommon {

    @Test
    public void shouldDumpDirectoryAndIncludeMetadataAndQueueFiles() throws Exception {
        final File dir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("hello");
            appender.writeText("world");
        }

        // locate a queue file to also test single-file dump
        final Path cq4 = Files.list(dir.toPath())
                .filter(p -> p.toString().endsWith(SingleChronicleQueue.SUFFIX))
                .findFirst().orElseThrow(() -> new AssertionError("no cq4 file in " + dir));

        // dump directory
        final ByteArrayOutputStream captureDir = new ByteArrayOutputStream();
        InternalDumpMain.dump(dir, new PrintStream(captureDir), Long.MAX_VALUE);
        final String outDir = captureDir.toString();
        assertTrue(outDir.contains("## "), "dump: includes markdown heading");
        assertTrue(outDir.contains(".cq4"), "dump: includes cq4 files");
        assertTrue(outDir.contains("metadata.cq4t"), "dump: includes metadata file");

        // dump single file
        final ByteArrayOutputStream captureFile = new ByteArrayOutputStream();
        InternalDumpMain.dump(cq4.toFile(), new PrintStream(captureFile), Long.MAX_VALUE);
        assertNotEquals(0, captureFile.size());
    }
}
