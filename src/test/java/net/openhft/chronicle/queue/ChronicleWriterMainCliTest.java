/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class ChronicleWriterMainCliTest extends QueueTestCommon {

    @Test
    public void mainWritesYamlFilesToQueue() throws Exception {
        final Path queueDir = getTmpDir().toPath();
        final Path payload = Files.createTempFile(Paths.get(OS.getTarget()), "writer-cli", ".yaml");
        Files.write(payload, "!int 42\n".getBytes(StandardCharsets.UTF_8));

        ChronicleWriterMain.main(new String[] {
                "-d", queueDir.toString(),
                "-m", "value",
                payload.toString()
        });

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            final ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(42, dc.wire().read("value").int32());
            }
            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
    }

    @Test
    public void mainAcceptsMultipleFiles() throws Exception {
        final Path queueDir = getTmpDir().toPath();
        final Path payloadOne = Files.createTempFile(Paths.get(OS.getTarget()), "writer-cli", ".yaml");
        final Path payloadTwo = Files.createTempFile(Paths.get(OS.getTarget()), "writer-cli", ".yaml");
        Files.write(payloadOne, "!int 1\n".getBytes(StandardCharsets.UTF_8));
        Files.write(payloadTwo, "!int 2\n".getBytes(StandardCharsets.UTF_8));

        ChronicleWriterMain.main(new String[]{
                "-d", queueDir.toString(),
                "-m", "value",
                payloadOne.toString(),
                payloadTwo.toString()
        });

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            final ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(1, dc.wire().read("value").int32());
            }
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals(2, dc.wire().read("value").int32());
            }
        }
    }
}
