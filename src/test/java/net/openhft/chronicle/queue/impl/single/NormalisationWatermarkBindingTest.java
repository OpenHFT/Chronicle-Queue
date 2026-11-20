/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class NormalisationWatermarkBindingTest extends QueueTestCommon {
    private static final String KEY = "normalisedEOFsTo";

    @Test
    public void readOnlyLookupsKeepTheirReleasedBehaviour() throws Exception {
        assumeFalse(OS.isWindows());
        File directory = getTmpDir();
        try (SingleChronicleQueue writer = SingleChronicleQueueBuilder.binary(directory).build()) {
            writer.tableStorePut(KEY, 37L);
        }
        Path metadata = directory.toPath().resolve(SingleChronicleQueue.QUEUE_METADATA_FILE);
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(metadata);
        try {
            Files.setPosixFilePermissions(metadata, PosixFilePermissions.fromString("r--r--r--"));
            assumeFalse("requires a user without write permission", Files.isWritable(metadata));
            try (SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(directory).readOnly(true).build()) {
                assertEquals(37L, reader.tableStoreGet(KEY));
                assertThrows(IllegalStateException.class, () -> reader.tableStoreGet("absent"));
            }
        } finally {
            Files.setPosixFilePermissions(metadata, original);
        }
    }

    @Test
    public void closeListenerCannotPublishAnotherBinding() {
        AtomicBoolean rejected = new AtomicBoolean();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).build()) {
            queue.addCloseListener(new AbstractCloseable() {
                @Override
                protected void performClose() {
                    try {
                        queue.tableStoreAcquire(KEY, 0L);
                    } catch (ClosedIllegalStateException expected) {
                        rejected.set(true);
                    }
                }
            });
        }
        assertTrue("a queue in performClose must reject cache misses", rejected.get());
    }

    @Test
    public void firstNormalisationHonoursTheMetadataLockAndCachesTheExistingValue() throws Exception {
        File directory = getTmpDir();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        LongValue cached;
        try (SingleChronicleQueue owner = SingleChronicleQueueBuilder.binary(directory).build();
             SingleChronicleQueue writer = SingleChronicleQueueBuilder.binary(directory).build()) {
            try (ExcerptAppender seed = owner.createAppender()) {
                seed.writeText("one cycle");
            }
            long watermark = owner.firstCycle() + 1L;
            try (LongValue initial = owner.metaStore().doWithExclusiveLock(t -> t.acquireValueFor(KEY, watermark))) {
                assertEquals(watermark, initial.getVolatileValue());
            }

            CountDownLatch ready = new CountDownLatch(1);
            Future<?> pending = owner.metaStore().doWithExclusiveLock(table -> {
                Future<?> task = worker.submit(() -> {
                    try (ExcerptAppender appender = writer.createAppender()) {
                        ready.countDown();
                        appender.normaliseEOFs();
                    }
                });
                try {
                    assertTrue("appender was ready", ready.await(5, TimeUnit.SECONDS));
                    task.get(200, TimeUnit.MILLISECONDS);
                    fail("first watermark binding completed while another instance held the structural lock");
                } catch (TimeoutException expected) {
                    // The worker can finish once this callback releases the structural lock.
                } catch (InterruptedException | ExecutionException e) {
                    throw new AssertionError(e);
                }
                return task;
            });
            pending.get(5, TimeUnit.SECONDS);

            cached = writer.tableStoreAcquire(KEY, -1);
            assertEquals(watermark, cached.getVolatileValue());
            owner.metaStore().doWithExclusiveLock(table -> {
                try (ExcerptAppender appender = writer.createAppender()) {
                    appender.normaliseEOFs();
                    assertSame(cached, writer.tableStoreAcquire(KEY, -1));
                }
                return null;
            });
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
        assertTrue("queue owns the binding", cached.isClosed());
    }
}
