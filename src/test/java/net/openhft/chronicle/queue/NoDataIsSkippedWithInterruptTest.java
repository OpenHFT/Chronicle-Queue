/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;

public class NoDataIsSkippedWithInterruptTest extends QueueTestCommon {

    private static final String EXPECTED = "Hello World";

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    public void test() {
        final SetTimeProvider timeProvider = new SetTimeProvider();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.single(DirectoryUtils.tempDir("."))
                .rollCycle(MINUTELY)
                .timeProvider(timeProvider)
                .testBlockSize()
                .build();
             final ExcerptAppender excerptAppender = q.acquireAppender();
             final ExcerptTailer tailer = q.createTailer()) {

            Thread.currentThread().interrupt();
            excerptAppender.writeText(EXPECTED);
            // TODO: restore the below
            Assert.assertTrue(Thread.currentThread().isInterrupted());

            timeProvider.advanceMillis(60_000);

            excerptAppender.writeText(EXPECTED);

            Assert.assertEquals(EXPECTED, tailer.readText());
            Assert.assertEquals(EXPECTED, tailer.readText());
        }
    }
}

