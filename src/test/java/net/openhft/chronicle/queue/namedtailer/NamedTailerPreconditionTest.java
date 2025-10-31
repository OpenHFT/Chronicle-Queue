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
package net.openhft.chronicle.queue.namedtailer;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.NamedTailerNotAvailableException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NamedTailerPreconditionTest extends QueueTestCommon {

    @Test
    public void canCreateNonReplicatedNamedTailerOnSink() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build()) {
            setQueueAsSink(queue);
            try (ExcerptTailer tailer = queue.createTailer("named_1")) {
                assertEquals(0, tailer.index());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    public void cannotCreateNonReplicatedNamedTailerOnSink() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build()) {
            setQueueAsSink(queue);
            NamedTailerNotAvailableException exception = assertThrows(
                    NamedTailerNotAvailableException.class,
                    () -> queue.createTailer("replicated:named_1")
            );
            assertEquals("replicated:named_1", exception.tailerName());
            assertEquals(NamedTailerNotAvailableException.Reason.NOT_AVAILABLE_ON_SINK, exception.reason());
            assertTrue(exception.getMessage().contains("Replicated named tailers cannot be instantiated on a replication sink"));
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    private void setQueueAsSink(SingleChronicleQueue queue) {
        queue.appendLock().lock();
    }
}
