/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.*;

public class ClosedMetadataBindingTest extends QueueTestCommon {
    @Test
    public void readOnlyLookupDoesNotAcquireAnExclusiveFileLock() throws Exception {
        File path = getTmpDir();
        String key = "test.readonly.value";
        try (SingleChronicleQueue writer = SingleChronicleQueueBuilder.binary(path).testBlockSize().build()) {
            writer.tableStorePut(key, 37);
        }
        // The public builder disables read-only queues on Windows. This fixture exercises
        // only the mapped metadata reader, which supports read-only mappings on every OS.
        SingleChronicleQueueBuilder builder = new SingleChronicleQueueBuilder() {
            @Override
            public boolean readOnly() {
                return true;
            }
        };
        try (SingleChronicleQueue reader = builder.path(path).build();
             FileChannel channel = FileChannel.open(new File(path, SingleChronicleQueue.QUEUE_METADATA_FILE).toPath(),
                     StandardOpenOption.WRITE);
             FileLock lock = channel.lock()) {
            assertTrue(lock.isValid());
            assertEquals(37, reader.tableStoreGet(key));
            assertThrows(IllegalStateException.class, () -> reader.tableStoreGet("absent"));
        }
    }

    @Test
    public void closedCachedBindingIsReacquiredWithoutReplacingPersistedValue() {
        File path = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).testBlockSize().build()) {
            LongValue first = queue.tableStoreAcquire("test.closed.value", 41);
            assertEquals(41, first.getVolatileValue());
            first.close();
            LongValue replacement = queue.tableStoreAcquire("test.closed.value", 0);
            assertNotSame(first, replacement);
            assertFalse(replacement.isClosed());
            assertEquals(41, replacement.getVolatileValue());
            replacement.setVolatileValue(42);
            assertSame(replacement, queue.tableStoreAcquire("test.closed.value", 99));
            assertEquals(42, queue.tableStoreGet("test.closed.value"));
        }
        try (SingleChronicleQueue reopened = SingleChronicleQueueBuilder.binary(path).testBlockSize().build()) {
            assertEquals(42, reopened.tableStoreGet("test.closed.value"));
        }
    }

    @Test
    public void closedWatermarkIsReacquiredBeforeTheAppenderReusesIt() {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).build()) {
            String key = "normalisedEOFsTo";
            LongValue first = queue.tableStoreAcquireWithExclusiveLock(key, 41);
            first.close();
            LongValue replacement = queue.tableStoreAcquireWithExclusiveLock(key, 0);
            assertNotSame(first, replacement);
            assertEquals(41, replacement.getVolatileValue());
            assertSame(replacement, queue.tableStoreAcquireWithExclusiveLock(key, 99));
        }
    }
}
