/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.DAILY;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({"deprecation", "removal"})
public class ChunkCountTest extends QueueTestCommon {
    @Test
    public void chunks() {
        File tempFile = IOTools.createTempFile("chunks");
        Assumptions.assumeFalse(PageUtil.isHugePage(tempFile.getAbsolutePath()), "Ignored on hugetlbfs as chunk count will vary under huge pages");
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder
                .binary(tempFile)
                .testBlockSize()
                .rollCycle(DAILY);
        try (SingleChronicleQueue queue = builder.build();
             ExcerptAppender appender = queue.createAppender()) {
            assertEquals(0, queue.chunkCount(), "chunkCount: empty queue");
            appender.writeText("Hello");
            assertEquals(1, queue.chunkCount(), "chunkCount: after first write");

            for (int i = 0; i < 100; i++) {
                long pos;
                try (DocumentContext dc = appender.writingDocument()) {
                    pos = dc.wire().bytes().writePosition();
                    dc.wire().bytes().writeSkip(16000);
                }
                final long expected = 1 + (pos >> 18);

                assertEquals(expected, queue.chunkCount(), "i: " + i);
            }
        } finally {
            IOTools.deleteDirWithFiles(tempFile);
        }
    }
}
