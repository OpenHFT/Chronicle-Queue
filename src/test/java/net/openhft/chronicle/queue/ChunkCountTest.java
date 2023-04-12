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

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.DAILY;
import static org.junit.Assert.assertEquals;

public class ChunkCountTest extends QueueTestCommon {
    @Test
    public void chunks() {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(IOTools.createTempFile("chunks")).blockSize(64 << 10).rollCycle(DAILY);
        try (SingleChronicleQueue queue = builder.build();
             ExcerptAppender appender = queue.acquireAppender()) {
            assertEquals(0, queue.chunkCount());
            appender.writeText("Hello");
            assertEquals(1, queue.chunkCount());

            for (int i = 0; i < 100; i++) {
                long pos;
                try (DocumentContext dc = appender.writingDocument()) {
                    pos = dc.wire().bytes().writePosition();
                    dc.wire().bytes().writeSkip(16000);
                }
                final long expected = builder.useSparseFiles() ? 1 : 1 + (pos >> 18);

                assertEquals("i: " + i, expected, queue.chunkCount());
            }
        }
    }
}
