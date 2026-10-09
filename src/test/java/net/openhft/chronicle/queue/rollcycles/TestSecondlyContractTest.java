/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.junit.Test;
import java.io.File;
import static org.junit.Assert.*;

public class TestSecondlyContractTest extends QueueTestCommon {
    @Test
    public void preservesSecondlyCapacityWithSmallInitialMapping() {
        TestRollCycles cycle = TestRollCycles.TEST_SECONDLY;
        assertEquals("index count", 2048, cycle.defaultIndexCount());
        assertEquals("index spacing", 4, cycle.defaultIndexSpacing());
        assertEquals("one second", 1000, cycle.lengthInMillis());
        assertEquals("per-cycle capacity", 16_777_216L, cycle.maxMessagesPerCycle());
        long last = cycle.toIndex(7, 16_777_215L);
        assertEquals(7, cycle.toCycle(last));
        assertEquals(16_777_215L, cycle.toSequenceNumber(last));
        try (SingleChronicleQueue queue = ChronicleQueue.singleBuilder(getTmpDir())
                .rollCycle(cycle).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {
            assertEquals("minimum mapping", 64L << 10, queue.blockSize());
            appender.writeText("capacity boundary is encoded without writing millions of messages");
            File[] rolls = queue.file().listFiles((dir, name) -> name.endsWith(".cq4"));
            assertNotNull(rolls);
            assertEquals(1, rolls.length);
            assertEquals("initial logical extent including overlap", 128L << 10, rolls[0].length());
        }
    }
}
