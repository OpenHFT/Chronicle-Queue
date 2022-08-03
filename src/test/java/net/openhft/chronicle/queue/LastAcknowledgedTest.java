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
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.*;

@RequiredForClient
public class LastAcknowledgedTest extends QueueTestCommon {
    @Test
    public void testLastAcknowledge() {
        String name = OS.getTarget() + "/testLastAcknowledge-" + Time.uniqueId();
        long lastIndexAppended;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build()) {
            ExcerptAppender excerptAppender = q.acquireAppender();
            excerptAppender.writeText("Hello World");
            lastIndexAppended = excerptAppender.lastIndexAppended();

            ExcerptTailer tailer = q.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isData());
                assertEquals(lastIndexAppended, tailer.index());
            }

            ExcerptTailer tailer2 = q.createTailer();
            tailer2.readAfterReplicaAcknowledged(true);
            try (DocumentContext dc = tailer2.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build()) {
            assertEquals(-1, q.lastAcknowledgedIndexReplicated());

            q.lastAcknowledgedIndexReplicated(lastIndexAppended - 1);

            ExcerptTailer tailer2 = q.createTailer();
            tailer2.readAfterReplicaAcknowledged(true);
            try (DocumentContext dc = tailer2.readingDocument()) {
                assertFalse(dc.isPresent());
            }

            q.lastAcknowledgedIndexReplicated(lastIndexAppended);

            try (DocumentContext dc = tailer2.readingDocument()) {
                assertTrue(dc.isData());
                assertEquals(lastIndexAppended, tailer2.index());
            }
        }
    }
}
