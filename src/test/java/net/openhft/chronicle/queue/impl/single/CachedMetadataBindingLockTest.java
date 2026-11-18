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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CachedMetadataBindingLockTest extends QueueTestCommon {
    @Test
    public void nestedMetadataWriteKeepsOtherProcessesExcluded() throws Exception {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).testBlockSize().build()) {
            queue.metaStore().doWithExclusiveLock(table -> {
                queue.tableStorePut("nested.writer", 17);
                Process probe = null;
                try {
                    probe = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").getPath(),
                            "-cp", System.getProperty("java.class.path"), LockProbe.class.getName(),
                            table.file().getAbsolutePath()).inheritIO().start();
                    assertTrue("Lock probe did not finish", probe.waitFor(10, TimeUnit.SECONDS));
                    assertEquals("Nested metadata access released the outer process lock", 0, probe.exitValue());
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    if (probe != null && probe.isAlive()) {
                        probe.destroyForcibly();
                        try {
                            assertTrue(probe.waitFor(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                }
                return null;
            });
        }
    }

    public static class LockProbe {
        public static void main(String[] args) throws Exception {
            try (FileChannel channel = FileChannel.open(Paths.get(args[0]), StandardOpenOption.WRITE);
                 FileLock lock = channel.tryLock(Long.MAX_VALUE - 1, 1, false)) {
                if (lock != null)
                    throw new AssertionError("Metadata writer lock is not held");
            }
        }
    }

    @Test
    public void independentQueueWaitsForTheMetadataWriter() throws Exception {
        File directory = getTmpDir();
        try (SingleChronicleQueue first = SingleChronicleQueueBuilder.binary(directory).testBlockSize().build();
             SingleChronicleQueue second = SingleChronicleQueueBuilder.binary(directory).testBlockSize().build()) {
            ExecutorService writer = Executors.newSingleThreadExecutor();
            try {
                CountDownLatch started = new CountDownLatch(1);
                Future<?> pending = first.metaStore().doWithExclusiveLock(table -> {
                    Future<?> task = writer.submit(() -> {
                        started.countDown();
                        second.tableStorePut("second.writer", 29);
                    });
                    try {
                        assertTrue(started.await(5, TimeUnit.SECONDS));
                        assertThrows("Another queue must respect the held metadata file lock",
                                TimeoutException.class, () -> task.get(200, TimeUnit.MILLISECONDS));
                        try (LongValue value = table.acquireValueFor("first.writer", 17)) {
                            assertEquals(17, value.getVolatileValue());
                        }
                        return task;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
                pending.get(5, TimeUnit.SECONDS);
                assertEquals(17, second.tableStoreGet("first.writer"));
                assertEquals(29, first.tableStoreGet("second.writer"));
                second.tableStorePut("second.writer", 31);
                assertEquals(31, first.tableStoreGet("second.writer"));
            } finally {
                writer.shutdownNow();
                assertTrue(writer.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }
}
