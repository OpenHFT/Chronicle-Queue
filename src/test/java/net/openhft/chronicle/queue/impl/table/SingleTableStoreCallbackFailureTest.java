/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class SingleTableStoreCallbackFailureTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test(timeout = 20_000)
    public void sharedCallbackFailureIsNotRetried() throws Exception {
        assertNotRetried(true);
    }

    @Test(timeout = 20_000)
    public void exclusiveCallbackFailureIsNotRetried() throws Exception {
        assertNotRetried(false);
    }

    private void assertNotRetried(boolean shared) throws Exception {
        File file = folder.newFile("metadata.cq4t");
        AtomicInteger calls = new AtomicInteger();
        OverlappingFileLockException failure = new OverlappingFileLockException();
        Function<Object, Object> callback = ignored -> {
            calls.incrementAndGet();
            throw failure;
        };
        ThrowingRunnable operation = shared
                ? () -> SingleTableStore.doWithSharedLock(file, callback, () -> null)
                : () -> SingleTableStore.doWithExclusiveLock(file, callback, () -> null);
        assertSame(failure, assertThrows(OverlappingFileLockException.class, operation));
        assertEquals("a callback may have performed non-repeatable work", 1, calls.get());
        assertEquals("released", SingleTableStore.doWithExclusiveLock(file, ignored -> "released", () -> null));
    }
}
