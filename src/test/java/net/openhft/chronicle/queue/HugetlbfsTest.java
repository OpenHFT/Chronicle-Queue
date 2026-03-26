/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.util.HugetlbfsTestUtil;
import org.junit.jupiter.api.Test;

import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.getHugetlbfsQueueDirectory;
import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.isHugetlbfsAvailable;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class HugetlbfsTest extends QueueTestCommon {

    @Test
    void queueHugetlbfsEndToEndSimpleAcceptanceTest() {
        assumeTrue(isHugetlbfsAvailable());
        String path = getHugetlbfsQueueDirectory(testMethodName);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single()
                .path(path)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("1");
            assertEquals("1", tailer.readText());
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
