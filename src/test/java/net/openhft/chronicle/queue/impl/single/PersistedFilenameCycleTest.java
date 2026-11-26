/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import org.junit.Test;
import java.io.File;
import static org.junit.Assert.*;

public class PersistedFilenameCycleTest extends QueueTestCommon {
    @Test
    public void filenamesUsePersistedEpochAfterReopeningWithoutConfiguration() {
        ignoreException("Overriding roll length");
        ignoreException("Overriding roll epoch");
        for (RollCycle cycle : new RollCycle[]{RollCycles.FAST_HOURLY, RollCycles.FAST_DAILY}) {
            File dir = getTmpDir();
            long epoch = cycle.defaultEpoch() + 3L * cycle.lengthInMillis();
            SetTimeProvider time = new SetTimeProvider();
            time.currentTimeMillis(epoch);
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                    .rollCycle(cycle).epoch(epoch).timeProvider(time).testBlockSize().build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("zero");
                time.currentTimeMillis(epoch + 2L * cycle.lengthInMillis());
                appender.writeText("two");
            }
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).timeProvider(time).testBlockSize().build()) {
                File[] files = dir.listFiles((path, name) -> name.endsWith(".cq4"));
                assertNotNull(files);
                assertEquals(2, files.length);
                int[] observed = java.util.Arrays.stream(files).mapToInt(queue::cycleForFile).sorted().toArray();
                assertArrayEquals(new int[]{0, 2}, observed);
                assertEquals(epoch, queue.epoch());
            }
        }
    }

    @Test
    public void rejectsNonRollSuffixAndMalformedName() {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).testBlockSize().build()) {
            File metadata = new File("metadata.cq4t");
            File malformed = new File("not-a-cycle.cq4");
            assertThrows(IllegalArgumentException.class, () -> queue.cycleForFile(metadata));
            assertThrows(RuntimeException.class, () -> queue.cycleForFile(malformed));
        }
    }
}
