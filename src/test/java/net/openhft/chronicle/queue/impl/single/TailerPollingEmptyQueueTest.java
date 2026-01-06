/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.testframework.GcControls;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public final class TailerPollingEmptyQueueTest extends QueueTestCommon {

    @Test
    @DisplayName("Empty queue polling avoids excess garbage")
    public void shouldNotGenerateExcessGarbage() {
        // Perform a GC prior to the test to ensure an unrelated GC does not occur which would devalue this test
        GcControls.waitForGcCycle();

        try (final SingleChronicleQueue queue = createQueue()) {
            queue.path.mkdirs();
            assertEquals(0, queue.path.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)).length, "setup: no queue files exist");

            final ExcerptTailer tailer = queue.createTailer();

            for (int i = 0; i < 50; i++) {
                assertFalse(tailer.readingDocument().isPresent(),
                        "Empty queue should not return a document at iteration " + i);
            }

            final long startCollectionCount = GcControls.getGcCount();

            for (int i = 0; i < 1_000_000; i++) {
                assertFalse(tailer.readingDocument().isPresent(),
                        "Empty queue should remain empty during long polling at iteration " + i);
            }

            assertEquals(0L, GcControls.getGcCount() - startCollectionCount, "polling should not trigger GC");
        }
    }

    private SingleChronicleQueue createQueue() {
        return ChronicleQueue.singleBuilder(
                getTmpDir()).
                testBlockSize().
                build();
    }
}
