/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.NavigableMap;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class ReadonlyNamedTailerIndexesTest extends QueueTestCommon {

    @Test
    public void readOnlyQueueWithoutMetadataHasNoNamedTailers() {
        assumeFalse(OS.isWindows());
        File directory = getTmpDir();
        Path metadata = directory.toPath().resolve(SingleChronicleQueue.QUEUE_METADATA_FILE);
        assertFalse(Files.exists(metadata));
        expectException("Failback to readonly tablestore");

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(directory)
                .readOnly(true)
                .build()) {
            assertTrue(queue.metaStore().readOnly());
            assertTrue(queue.namedTailerIndexes().isEmpty());
        }

        assertFalse(Files.exists(metadata));
    }

    @Test
    public void readsNamedTailersWithoutWriteAccessOrMetadataMutation() throws Exception {
        assumeFalse(OS.isWindows());
        File directory = getTmpDir();
        long index;
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(directory).build();
             ExcerptTailer tailer = queue.createTailer("readonly-consumer")) {
            queue.createAppender().writeText("one");
            assertTrue(tailer.readText().equals("one"));
            index = tailer.index();
        }

        Path metadata = directory.toPath().resolve(SingleChronicleQueue.QUEUE_METADATA_FILE);
        byte[] before = Files.readAllBytes(metadata);
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(metadata);
        Set<PosixFilePermission> readOnly = EnumSet.copyOf(original);
        readOnly.remove(PosixFilePermission.OWNER_WRITE);
        readOnly.remove(PosixFilePermission.GROUP_WRITE);
        readOnly.remove(PosixFilePermission.OTHERS_WRITE);
        Files.setPosixFilePermissions(metadata, readOnly);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(directory)
                .readOnly(true)
                .build()) {
            assertTrue(queue.metaStore().readOnly());
            NavigableMap<String, Long> indexes = queue.namedTailerIndexes();
            assertEquals(Long.valueOf(index), indexes.get("readonly-consumer"));
        } finally {
            Files.setPosixFilePermissions(metadata, original);
        }
        assertArrayEquals(before, Files.readAllBytes(metadata));
    }
}
