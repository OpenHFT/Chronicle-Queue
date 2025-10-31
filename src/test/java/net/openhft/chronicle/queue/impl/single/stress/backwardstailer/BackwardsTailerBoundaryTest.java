/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BackwardsTailerBoundaryTest extends QueueTestCommon {

    private static final Logger log = LoggerFactory.getLogger(BackwardsTailerBoundaryTest.class);

    private SetTimeProvider timeProvider;

    private final RollCycle rollCycle;

    public BackwardsTailerBoundaryTest(RollCycle rollCycle) {
        this.rollCycle = rollCycle;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        final List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{TestRollCycles.TEST4_DAILY});
        return data;
    }

    @Before
    public void before() {
        timeProvider = new SetTimeProvider();
    }

    @Test
    public void verifyConsistency() {
        @NotNull File path = getTmpDir();
        IOTools.deleteDirWithFiles(path);
        try (SingleChronicleQueue queue = createQueue(path, rollCycle);
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD)) {

            assertEquals("Backwards tailer should start at index 0 when no queue data", 0, tailer.index());

            long messagesPerCycle = (long) rollCycle.defaultIndexSpacing() * rollCycle.defaultIndexCount() * 5;

            for (int i = 0; i < messagesPerCycle * 5; i++) {
                advanceTimeBeforeRollCycleFills(i, messagesPerCycle, queue);
                long lastIndexAppended = writeDataToQueue(appender, i, queue);

                // Move to end
                tailer.toEnd();
                assertEquals(lastIndexAppended, tailer.index());

                // Move to beginning
                tailer.moveToIndex(0);
            }

        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    private static long writeDataToQueue(ExcerptAppender appender, int i, SingleChronicleQueue queue) {
        appender.writeText(Integer.toString(i));
        long lastIndexAppended = appender.lastIndexAppended();
        int cycle = queue.rollCycle().toCycle(lastIndexAppended);
        long sequenceNumber = queue.rollCycle().toSequenceNumber(lastIndexAppended);
        log.debug("cycle={}, sequenceNumber={}", cycle, sequenceNumber);
        return lastIndexAppended;
    }

    private void advanceTimeBeforeRollCycleFills(int i, long messagesPerCycle, SingleChronicleQueue queue) {
        if (i > 0 && i % messagesPerCycle == 0) {
            log.info("Advancing time to move to next cycle. Current cycle={}", queue.cycle());
            timeProvider.advanceMillis(rollCycle.lengthInMillis());
        }
    }

    @NotNull
    private SingleChronicleQueue createQueue(File path,
                                             RollCycle rollCycle) {
        return SingleChronicleQueueBuilder
                .builder()
                .timeProvider(timeProvider)
                .path(path)
                .rollCycle(rollCycle)
                .build();
    }
}
