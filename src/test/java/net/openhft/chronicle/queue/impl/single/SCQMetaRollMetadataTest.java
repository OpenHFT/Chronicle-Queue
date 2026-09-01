/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SCQMetaRollMetadataTest extends QueueTestCommon {

    // Cycle parsing must use the Queue's persisted format and reject non-roll paths.
    @Test
    public void cycleForFileUsesTheQueueRollGeometry() {
        SetTimeProvider time = new SetTimeProvider(1_700_000_000_000L);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(getTmpDir())
                .rollCycle(TestRollCycles.TEST_DAILY)
                .timeProvider(time)
                .build()) {
            queue.createAppender().writeText("one");
            time.advanceMillis(TestRollCycles.TEST_DAILY.lengthInMillis() * 2L);
            queue.createAppender().writeText("two");
            File[] rolls = Objects.requireNonNull(queue.file().listFiles(
                    file -> file.getName().endsWith(SingleChronicleQueue.SUFFIX)));
            Arrays.sort(rolls);
            File roll = rolls[rolls.length - 1];

            assertEquals(queue.lastCycle(), queue.cycleForFile(roll));
            assertThrows(IllegalArgumentException.class,
                    () -> queue.cycleForFile(new File(queue.file(), "metadata.cq4t")));
        }
    }
}
