/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class ClosedMetadataBindingTest extends QueueTestCommon {
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
