/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.DiskSpaceMonitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.FileStore;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Whilst {@link DiskSpaceMonitor} is defined in the Threads module Queue is heavily dependent on it. Of particular
 * importance is ensuring that queue paths are added to the monitor appropriately by calling the
 * {@link DiskSpaceMonitor#pollDiskSpace(File)} method.
 */
@SuppressWarnings("try")
class DiskSpaceMonitoringIntegrationTest extends QueueTestCommon {

    /**
     * Stores which paths are being monitored by the {@link net.openhft.chronicle.threads.DiskSpaceMonitor}.
     */
    private Map<String, FileStore> monitoredPaths;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void beforeEach() throws NoSuchFieldException, IllegalAccessException {
        Field fileStoreCacheMap = DiskSpaceMonitor.class.getDeclaredField("fileStoreCacheMap");
        fileStoreCacheMap.setAccessible(true);
        monitoredPaths = (Map<String, FileStore>) fileStoreCacheMap.get(DiskSpaceMonitor.INSTANCE);
    }

    @Nested
    class EnsureThatPollIsCalledInDifferentScenarioTests {

        @Test
        void newQueueNoWrite() {
            File tmpDir = getTmpDir();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(tmpDir).build();
                 ExcerptAppender appender = queue.createAppender()) {
                assertMapped(tmpDir);
            } finally {
                IOTools.deleteDirWithFiles(tmpDir);
            }
        }

        @Test
        void newQueueWithWrite() {
            File tmpDir = getTmpDir();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(tmpDir).build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("Test");
                assertMapped(tmpDir);
            } finally {
                IOTools.deleteDirWithFiles(tmpDir);
            }
        }

        @Test
        void newQueueJustTail() {
            File tmpDir = getTmpDir();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(tmpDir).build();
                 ExcerptTailer tailer = queue.createTailer()) {
                assertMapped(tmpDir);
            } finally {
                IOTools.deleteDirWithFiles(tmpDir);
            }
        }

        @Test
        void existingQueueNoWrite() {
            File tmpDir = getTmpDir();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(tmpDir).build();
                 ExcerptAppender appender = queue.createAppender()) {
                // Intentional no-op
            }

            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(tmpDir).build();
                 ExcerptAppender appender = queue.createAppender()) {
                assertMapped(tmpDir);
            }
        }

        @Test
        void existingQueueWithWrite() {
            File tmpDir = getTmpDir();
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(tmpDir).build();
                 ExcerptAppender appender = queue.createAppender()) {
                // Intentional no-op
            }

            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(tmpDir).build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("Test");
                assertMapped(tmpDir);
            }
        }
    }

    private void assertMapped(File queuePath) {
        String path = queuePath.getAbsolutePath().toString();
        assertTrue(monitoredPaths.containsKey(path), () -> "Expected that the following queue path should be monitored by the disk space monitor, but it was not. Path: " + path);
    }
}
