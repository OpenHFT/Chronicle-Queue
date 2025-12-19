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
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"deprecation", "removal"})
class NamedTailerPreconditionTest extends QueueTestCommon {

    @Test
    public void canCreateNonReplicatedNamedTailerOnSink() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build()) {
            setQueueAsSink(queue);
            try (ExcerptTailer tailer = queue.createTailer("named_1")) {
                assertEquals(0, tailer.index(), "tailer: initial index");
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
            NamedTailerNotAvailableException exception = assertThrows(NamedTailerNotAvailableException.class, () -> queue.createTailer("replicated:named_1"), "sink: cannot create replicated named tailer");
            assertEquals("replicated:named_1", exception.tailerName(), "tailer name");
            assertEquals(NamedTailerNotAvailableException.Reason.NOT_AVAILABLE_ON_SINK, exception.reason(), "tailer reason");
            assertTrue(exception.getMessage().contains("Replicated named tailers cannot be instantiated on a replication sink"), "tailer message");
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    private void setQueueAsSink(SingleChronicleQueue queue) {
        queue.appendLock().lock();
    }
}
