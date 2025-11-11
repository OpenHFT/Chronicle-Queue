/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.DAILY;
import static org.junit.Assert.assertEquals;

public class ChunkCountTest extends QueueTestCommon {
    @Test
    public void chunks() {
        File tempFile = IOTools.createTempFile("chunks");
        Assume.assumeFalse("Ignored on hugetlbfs as chunk count will vary under huge pages", PageUtil.isHugePage(tempFile.getAbsolutePath()));
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder
                .binary(tempFile)
                .testBlockSize()
                .rollCycle(DAILY);
        try (SingleChronicleQueue queue = builder.build();
             ExcerptAppender appender = queue.createAppender()) {
            assertEquals(0, queue.chunkCount());
            appender.writeText("Hello");
            assertEquals(1, queue.chunkCount());

            for (int i = 0; i < 100; i++) {
                long pos;
                try (DocumentContext dc = appender.writingDocument()) {
                    pos = dc.wire().bytes().writePosition();
                    dc.wire().bytes().writeSkip(16000);
                }
                final long expected = 1 + (pos >> 18);

                assertEquals("i: " + i, expected, queue.chunkCount());
            }
        } finally {
            IOTools.deleteDirWithFiles(tempFile);
        }
    }
}
