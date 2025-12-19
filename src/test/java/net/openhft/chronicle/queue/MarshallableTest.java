/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.jupiter.api.Test;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MarshallableTest extends QueueTestCommon {
    @Test
    public void testWriteText() {
        File dir = getTmpDir();
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {

            ExcerptTailer tailer = queue.createTailer();
            ExcerptTailer tailer2 = queue.createTailer();

            int runs = 1000;
            for (int i = 0; i < runs; i++)
                appender.writeText("" + i);
            for (int i = 0; i < runs; i++)
                assertEquals("" + i, tailer.readText(), "readText at i=" + i);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < runs; i++) {
                assertTrue(tailer2.readText(sb), "readText into StringBuilder at i=" + i);
                assertEquals("" + i, sb.toString(), "StringBuilder content at i=" + i);
            }
        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }
}
