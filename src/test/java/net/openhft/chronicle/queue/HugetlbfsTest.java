/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.getHugetlbfsQueueDirectory;
import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.isHugetlbfsAvailable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class HugetlbfsTest extends QueueTestCommon {

    @Test
    public void queueHugetlbfsEndToEndSimpleAcceptanceTest(TestInfo testInfo) {
        assumeTrue(isHugetlbfsAvailable());
        String methodName = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        String path = getHugetlbfsQueueDirectory(methodName);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single()
                .path(path)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("1");
            assertEquals("1", tailer.readText(), "hugetlbfs: read back written message");
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
