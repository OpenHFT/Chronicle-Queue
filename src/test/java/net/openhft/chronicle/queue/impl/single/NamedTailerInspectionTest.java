/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class NamedTailerInspectionTest extends QueueTestCommon {
    @Test
    public void returnsDetachedPositionsWithoutAddingMetadata() throws Exception {
        File directory = getTmpDir();
        String replicated = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "peer";
        try (SingleChronicleQueue queue = builder(directory).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer consumer = queue.createTailer("consumer");
             ExcerptTailer replica = queue.createTailer(replicated);
             ExcerptTailer unstarted = queue.createTailer("unstarted")) {
            appender.writeText("one");
            assertEquals("one", consumer.readText());
            assertEquals("one", replica.readText());
            assertEquals(0L, unstarted.index());
            File metadata = new File(directory, SingleChronicleQueue.QUEUE_METADATA_FILE);
            byte[] before = Files.readAllBytes(metadata.toPath());
            NavigableMap<String, Long> positions = queue.namedTailerIndexes();
            assertEquals(3, positions.size());
            assertEquals(Long.valueOf(consumer.index()), positions.get("consumer"));
            assertEquals(Long.valueOf(replica.index()), positions.get(replicated));
            assertEquals(Long.valueOf(0), positions.get("unstarted"));
            assertArrayEquals(before, Files.readAllBytes(metadata.toPath()));
            positions.clear();
            assertEquals(3, queue.namedTailerIndexes().size());
            appender.writeText("two");
            assertEquals("two", consumer.readText());
            assertEquals(Long.valueOf(consumer.index()), queue.namedTailerIndexes().get("consumer"));
        }
    }

    @Test
    public void copiedQueueMetadataCanBeScannedWithoutGrowingTheFile() throws Exception {
        File original = getTmpDir();
        File publishedRoll;
        try (SingleChronicleQueue queue = builder(original).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("consumer")) {
            appender.writeText("record");
            assertEquals("record", tailer.readText());
            publishedRoll = appender.currentFile();
        }
        BackgroundResourceReleaser.releasePendingResources();
        File directory = getTmpDir();
        Files.createDirectories(directory.toPath());
        File copy = new File(directory, SingleChronicleQueue.QUEUE_METADATA_FILE);
        Files.copy(new File(original, SingleChronicleQueue.QUEUE_METADATA_FILE).toPath(), copy.toPath());
        // A writable Queue snapshot must retain the roll published by its metadata.
        Files.copy(publishedRoll.toPath(), new File(directory, publishedRoll.getName()).toPath());
        try (SingleChronicleQueue queue = builder(directory).build()) {
            long size = Files.size(copy.toPath());
            assertTrue(queue.namedTailerIndexes().containsKey("consumer"));
            assertEquals(size, Files.size(copy.toPath()));
        }
    }

    @Test
    public void metadataTableCanBeScannedWithoutRollFiles() throws Exception {
        File original = getTmpDir();
        long index;
        try (SingleChronicleQueue queue = builder(original).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("consumer")) {
            appender.writeText("record");
            assertEquals("record", tailer.readText());
            index = tailer.index();
        }
        BackgroundResourceReleaser.releasePendingResources();
        File directory = getTmpDir();
        Files.createDirectories(directory.toPath());
        File copy = new File(directory, SingleChronicleQueue.QUEUE_METADATA_FILE);
        Files.copy(new File(original, SingleChronicleQueue.QUEUE_METADATA_FILE).toPath(), copy.toPath());
        byte[] before = Files.readAllBytes(copy.toPath());
        try (TableStore<?> table =
                     SingleTableBuilder.binary(copy,
                             Metadata.NoMeta.INSTANCE).readOnly(!OS.isWindows()).build()) {
            assertEquals(Long.valueOf(index), SingleChronicleQueue.scanNamedTailerIndexes(table).get("consumer"));
            assertFalse(table.isClosed());
        }
        BackgroundResourceReleaser.releasePendingResources();
        assertArrayEquals(before, Files.readAllBytes(copy.toPath()));
        assertEquals(1, directory.listFiles().length);
    }

    @Test
    public void readOnlyMetadataCopyKeepsPersistedBounds() throws Exception {
        assumeFalse("Full read-only Queue opening remains unsupported on Windows", OS.isWindows());
        File original = getTmpDir();
        SetTimeProvider time = new SetTimeProvider(0);
        long index;
        try (SingleChronicleQueue queue = builder(original).timeProvider(time).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("consumer")) {
            appender.writeText("first");
            assertEquals("first", tailer.readText());
            index = tailer.index();
            time.advanceMillis(2L * RollCycles.FAST_HOURLY.lengthInMillis());
            appender.writeText("last");
        }
        BackgroundResourceReleaser.releasePendingResources();
        File copyDirectory = getTmpDir();
        Files.createDirectories(copyDirectory.toPath());
        File copy = new File(copyDirectory, SingleChronicleQueue.QUEUE_METADATA_FILE);
        Files.copy(new File(original, SingleChronicleQueue.QUEUE_METADATA_FILE).toPath(), copy.toPath());
        byte[] before = Files.readAllBytes(copy.toPath());
        try (SingleChronicleQueue queue = builder(copyDirectory).readOnly(true).build()) {
            assertTrue(queue.metaStore().readOnly());
            assertEquals(0, queue.firstCycle());
            assertEquals(2, queue.lastCycle());
            assertEquals(Long.valueOf(index), queue.namedTailerIndexes().get("consumer"));
        }
        BackgroundResourceReleaser.releasePendingResources();
        assertArrayEquals(before, Files.readAllBytes(copy.toPath()));
    }

    @Test
    public void missingReadOnlyMetadataReturnsEmptyWithoutCreatingIt() throws Exception {
        assumeFalse("Full read-only Queue opening remains unsupported on Windows", OS.isWindows());
        File directory = getTmpDir();
        Files.createDirectories(directory.toPath());
        expectException("Metadata file not found in readOnly mode");
        try (SingleChronicleQueue queue = builder(directory).readOnly(true).build()) {
            assertTrue(queue.namedTailerIndexes().isEmpty());
        }
        assertFalse(new File(directory, SingleChronicleQueue.QUEUE_METADATA_FILE).exists());
    }

    @Test
    public void preservesRawCaseAndDistinguishableLegacyNames() {
        String owner = SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX + "peer";
        Map<String, Long> raw = new TreeMap<>();
        raw.put("consumer", 7L);
        raw.put("Consumer", 9L);
        raw.put(owner, 11L);
        raw.put(owner + ".version", 3L);
        raw.put(owner + ".lock", 13L);
        raw.put(owner + ".lock.version", 5L);
        raw.put(owner + ".lock.lock", Long.MIN_VALUE);
        expectException("uses a metadata-shaped suffix");
        NavigableMap<String, Long> selected = SingleChronicleQueue.selectNamedTailerIndexes(raw);
        assertEquals(4, selected.size());
        assertEquals(Long.valueOf(7), selected.get("consumer"));
        assertEquals(Long.valueOf(9), selected.get("Consumer"));
        assertEquals(Long.valueOf(11), selected.get(owner));
        assertEquals(Long.valueOf(13), selected.get(owner + ".lock"));
    }

    private static SingleChronicleQueueBuilder builder(File directory) {
        return SingleChronicleQueueBuilder.binary(directory).testBlockSize()
                .rollCycle(RollCycles.FAST_HOURLY).timeProvider(new SetTimeProvider(0));
    }
}
