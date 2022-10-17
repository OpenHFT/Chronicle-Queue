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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultipleNamedTailersTest extends QueueTestCommon {
    @Test
    public void multipleTailers() {
        File tmpDir = new File(OS.getTarget(), "multipleTailers" + System.nanoTime());

        try (ChronicleQueue q1 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().rollCycle(TEST_SECONDLY).build();
             ExcerptAppender appender = q1.acquireAppender();
             ExcerptTailer tailer1 = q1.createTailer();
             ExcerptTailer namedTailer1 = q1.createTailer("named1");
             ChronicleQueue q2 = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().build();
             ExcerptTailer tailer2 = q2.createTailer();
             ExcerptTailer namedTailer2 = q2.createTailer("named2")) {
            for (int i = 0; i < 1_000_000; i++) {
                final String id0 = "" + i;
                appender.writeText(id0);
                final long index0 = appender.lastIndexAppended();
                check(tailer1, id0, index0);
                check(namedTailer1, id0, index0);
                check(tailer2, id0, index0);
                check(namedTailer2, id0, index0);
            }
        }
    }

    private void check(ExcerptTailer tailer1, String id0, long index0) {
        try (DocumentContext dc = tailer1.readingDocument()) {
            assertTrue(dc.isPresent());
            assertEquals(index0, tailer1.index());
            assertEquals(id0, dc.wire().getValueIn().text());
        }
    }
}
