/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Stackoveflow52274284Test extends QueueTestCommon {
    @Test
    @DisplayName("Stackoverflow 52274284 reproducer reads all records")
    public void fails() throws IOException {
        String basePath = OS.getTarget();
        String path = Files.createTempDirectory(Paths.get(basePath), "chronicle-")
                .toAbsolutePath()
                .toString();

        try (ChronicleQueue chronicleQueue = ChronicleQueue.singleBuilder(path).testBlockSize().build();

             // Create Appender
             ExcerptAppender appender = chronicleQueue.createAppender();

             // Create Tailer
             ExcerptTailer tailer = chronicleQueue.createTailer()) {
            tailer.toStart();

            int numberOfRecords = 10;

            // Write
            for (int i = 0; i <= numberOfRecords; i++) {
                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("msg").text("Hello World!");
                } catch (Exception e) {
                    System.err.println("Unable to store value to chronicle");
                    e.printStackTrace();
                }
            }
            // Read
            int reads = 0;
            for (int i = 0; i <= numberOfRecords; i++) {
                try (DocumentContext documentContext = tailer.readingDocument()) {
                    assertTrue(documentContext.isPresent(), "stackoverflow-52274284: document present i=" + i);
                    Wire wire = documentContext.wire();
                    assertNotNull(wire, "stackoverflow-52274284: wire i=" + i);
                    String msg = wire.read("msg").text();
                    assertEquals("Hello World!", msg, "stackoverflow-52274284: msg i=" + i);
                    reads++;
                }
            }
            assertEquals(numberOfRecords + 1, reads, "stackoverflow-52274284: documents read");
        }
        IOTools.deleteDirWithFiles(path);
    }
}
