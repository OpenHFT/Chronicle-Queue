/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.namedtailer;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.NamedTailerNotAvailableException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"deprecation", "removal"})
class NamedTailerPreconditionTest extends QueueTestCommon {

    @Test
    @DisplayName("Named tailer can be created on sink when not replicated")
    public void canCreateNonReplicatedNamedTailerOnSink() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build()) {
            setQueueAsSink(queue);
            try (ExcerptTailer tailer = queue.createTailer("named_1")) {
                assertEquals(0, tailer.index(), "tailer initial index should be zero on sink");
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    @DisplayName("Replicated named tailer cannot be created on sink")
    public void cannotCreateNonReplicatedNamedTailerOnSink() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build()) {
            setQueueAsSink(queue);
            NamedTailerNotAvailableException exception = assertThrows(NamedTailerNotAvailableException.class, () -> queue.createTailer("replicated:named_1"), "sink: cannot create replicated named tailer");
            assertEquals("replicated:named_1", exception.tailerName(),
                    "tailer name should match the requested replicated tailer");
            assertEquals(NamedTailerNotAvailableException.Reason.NOT_AVAILABLE_ON_SINK, exception.reason(),
                    "tailer reason should indicate not available on sink");
            assertTrue(exception.getMessage().contains("Replicated named tailers cannot be instantiated on a replication sink"),
                    "tailer message should explain replicated tailers are not allowed on sink");
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    private void setQueueAsSink(SingleChronicleQueue queue) {
        queue.appendLock().lock();
    }
}
