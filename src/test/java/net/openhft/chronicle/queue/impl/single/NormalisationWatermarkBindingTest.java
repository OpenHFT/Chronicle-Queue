/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
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
    public void firstWriteAndNamedTailerOnSeparateQueuesUseTheSameFileLock() throws Exception {
        File directory = getTmpDir();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CompletionService<Void> finished = new ExecutorCompletionService<>(workers);
        CountDownLatch appenderCreated = new CountDownLatch(1);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch attempting = new CountDownLatch(2);
        try (SingleChronicleQueue writer = SingleChronicleQueueBuilder.binary(directory).build();
             SingleChronicleQueue reader = SingleChronicleQueueBuilder.binary(directory).build()) {
            Future<Void> write = finished.submit(() -> {
                try (ExcerptAppender appender = writer.createAppender()) {
                    appenderCreated.countDown();
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    attempting.countDown();
                    appender.writeText("second");
                }
                return null;
            });
            // The appender was constructed before the first cycle existed.
            assertTrue(appenderCreated.await(10, TimeUnit.SECONDS));
            try (ExcerptAppender seed = reader.createAppender()) {
                seed.writeText("first");
            }
            Future<Void> register = finished.submit(() -> {
                assertTrue(start.await(10, TimeUnit.SECONDS));
                attempting.countDown();
                try (ExcerptTailer tailer = reader.createTailer("orders")) {
                    assertEquals("first", tailer.readText());
                }
                return null;
            });
            reader.metaStore().doWithExclusiveLock(table -> {
                table.forEachKey(null, (unused, key, value) -> {
                    assertFalse("watermark already exists", KEY.contentEquals(key));
                    assertFalse("tailer index already exists", "index.orders".contentEquals(key));
                });
                start.countDown();
                try {
                    assertTrue(attempting.await(10, TimeUnit.SECONDS));
                    assertNull("both writers must wait for the metadata file lock",
                            finished.poll(250, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return null;
            });
            write.get(10, TimeUnit.SECONDS);
            register.get(10, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
        try (SingleChronicleQueue reopened = SingleChronicleQueueBuilder.binary(directory).build();
             ExcerptTailer resumed = reopened.createTailer("orders")) {
            assertEquals(reopened.firstCycle(), reopened.tableStoreGet(KEY));
            assertEquals("second", resumed.readText());
        }
    }

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
