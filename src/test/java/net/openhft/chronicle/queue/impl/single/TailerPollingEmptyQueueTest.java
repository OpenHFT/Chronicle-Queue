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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class TailerPollingEmptyQueueTest extends QueueTestCommon {

    @Test
    public void shouldNotGenerateExcessGarbage() {
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