/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.testframework.GcControls;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class TailerPollingEmptyQueueTest extends QueueTestCommon {

    @Test
    public void shouldNotGenerateExcessGarbage() {
        // Perform a GC prior to the test to ensure an unrelated GC does not occur which would devalue this test
        GcControls.waitForGcCycle();

        try (final SingleChronicleQueue queue = createQueue()) {
            queue.path.mkdirs();
            assertEquals(0, queue.path.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)).length);

            final ExcerptTailer tailer = queue.createTailer();

            for (int i = 0; i < 50; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            final long startCollectionCount = GcControls.getGcCount();

            for (int i = 0; i < 1_000_000; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            assertEquals(0L, GcControls.getGcCount() - startCollectionCount);
        }
    }

    private SingleChronicleQueue createQueue() {
        return ChronicleQueue.singleBuilder(
                getTmpDir()).
                testBlockSize().
                build();
    }
}
