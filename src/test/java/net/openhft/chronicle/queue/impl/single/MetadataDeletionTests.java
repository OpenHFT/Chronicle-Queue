/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Tests to demonstrate recovery from metadata file deletion.
 */
class MetadataDeletionTests extends QueueTestCommon {

    @Test
    void singleCycleFile() {
        File queuePath = getTmpDir();
        try {

            // Create the queue
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queuePath).build();
                 ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("hello world");
            } finally {
                // Force release of resources to ensure that they are truly released by the time we clean up metadata
                BackgroundResourceReleaser.releasePendingResources();
            }

            // Imagine that system has shut down, delete metadata
            boolean delete = new File(queuePath, "metadata.cq4t").delete();
            assertTrue(delete, "metadata file should be deleted");

            // Verify it has really been deleted
            assertFalse(new File(queuePath, "metadata.cq4t").exists(), "metadata file should not exist");

            // Open again and let's see what we get
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queuePath).build();
                 ExcerptTailer tailer = queue.createTailer()) {
                assertEquals("hello world", tailer.readText(), "tailer: readText after metadata deletion");
            }

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    void multipleCycleFiles() {
        File queuePath = getTmpDir();
        try {

            // Create a custom time provider
            SetTimeProvider setTimeProvider = new SetTimeProvider();

            writeFourCycleFiles(queuePath, setTimeProvider);

            // Imagine that system has shut down, delete metadata
            boolean delete = new File(queuePath, "metadata.cq4t").delete();
            assertTrue(delete, "metadata file should be deleted");

            // Verify it has really been deleted
            assertFalse(new File(queuePath, "metadata.cq4t").exists(), "metadata file should not exist");

            // Verify that there are 4 cycle files
            assertEquals(4, Objects.requireNonNull(queuePath.listFiles((dir, name) -> name.endsWith(".cq4"))).length, "There should be 4 cycle files");

            // Open again and let's see what we get
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queuePath).timeProvider(setTimeProvider).build();
                 ExcerptTailer tailer = queue.createTailer()) {
                assertTailerReadsValuesWithCycle(tailer, 0, "1", "2", "3", "4");
            }

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    void multipleCycleFiles_deleteMetaDataWhilstTailing() {
        assumeFalse(OS.isWindows(), "Skip this test on Windows because we can't delete the metadata file while it's open.");
        File queuePath = getTmpDir();
        try {

            // Create a custom time provider
            SetTimeProvider setTimeProvider = new SetTimeProvider();

            writeFourCycleFiles(queuePath, setTimeProvider);

            // Open again and let's see what we get
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queuePath).timeProvider(setTimeProvider).build();
                 ExcerptTailer tailer = queue.createTailer()) {
                assertTailerReadsValuesWithCycle(tailer, 0, "1", "2");

                // Delete metadata here
                boolean delete = new File(queuePath, "metadata.cq4t").delete();
                assertTrue(delete, "metadata file should be deleted");
                assertFalse(new File(queuePath, "metadata.cq4t").exists(), "metadata file should not exist");
                assertEquals(4, Objects.requireNonNull(queuePath.listFiles((dir, name) -> name.endsWith(".cq4"))).length, "There should be 4 cycle files");

                assertTailerReadsValuesWithCycle(tailer, 2, "3", "4");
            }

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    private void writeFourCycleFiles(File queuePath, SetTimeProvider setTimeProvider) {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(queuePath).timeProvider(setTimeProvider).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("1");
            setTimeProvider.advanceMillis(Duration.ofDays(1).toMillis());
            appender.writeText("2");
            setTimeProvider.advanceMillis(Duration.ofDays(1).toMillis());
            appender.writeText("3");
            setTimeProvider.advanceMillis(Duration.ofDays(1).toMillis());
            appender.writeText("4");
        } finally {
            BackgroundResourceReleaser.releasePendingResources();
        }
    }

    private void assertTailerReadsValuesWithCycle(ExcerptTailer tailer, int startingCycle, String... expected) {
        int cycle = startingCycle;
        for (String value : expected) {
            assertEquals(value, tailer.readText(), "tailer should read expected value '" + value + "' from queue");
            assertEquals(cycle++, tailer.cycle(), "tailer should be at cycle " + (cycle - 1) + " after reading value");
        }
    }
}
