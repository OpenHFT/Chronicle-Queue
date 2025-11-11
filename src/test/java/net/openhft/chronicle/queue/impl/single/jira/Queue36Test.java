/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.jira;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-36
 */
public class Queue36Test extends QueueTestCommon {
    @Test
    public void testTail() {
        File basePath = getTmpDir();
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(basePath)
                .testBlockSize()
                .build()) {

            checkNoFiles(basePath);
            ExcerptTailer tailer = queue.createTailer();
            checkNoFiles(basePath);
            tailer.toStart();
            checkNoFiles(basePath);

            assertFalse(tailer.readDocument(d -> {
            }));

            checkNoFiles(basePath);
        }
    }

    private void checkNoFiles(@NotNull File basePath) {
        String[] fileNames = basePath.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX));
        assertTrue(fileNames == null || fileNames.length == 0);
    }
}
