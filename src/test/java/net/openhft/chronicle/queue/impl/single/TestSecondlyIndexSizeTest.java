/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

public class TestSecondlyIndexSizeTest extends QueueTestCommon {

    @Test
    public void smallRollFilesStaySmallAfterReopeningAndSeeking() {
        final File path = getTmpDir();
        final SetTimeProvider time = new SetTimeProvider();
        time.currentTimeMillis(0);
        final long[] indexes = new long[100];

        try (SingleChronicleQueue queue = smallBuilder(path, time).build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < indexes.length; i++) {
                appender.writeText("message-" + i);
                indexes[i] = appender.lastIndexAppended();
                time.advanceMillis(300);
            }
        }

        // The requested Windows-safe block and its overlap each occupy at least
        // one filesystem page, including when this suite runs on hugetlbfs.
        final long expectedFileLength = 2L * Math.max(OS.SAFE_PAGE_SIZE,
                PageUtil.getPageSize(path.getAbsolutePath()));
        assertRollFileLengths(path, 30, expectedFileLength);

        try (SingleChronicleQueue queue = smallBuilder(path, time).build();
             ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < indexes.length; i++)
                assertEquals("message-" + i, tailer.readText());
            assertNull(tailer.readText());
            assertTrue(tailer.moveToIndex(indexes[50]));
            assertEquals("message-50", tailer.readText());
        }
        assertRollFileLengths(path, 30, expectedFileLength);
    }

    @Test
    public void reopensLegacyLargeIndexAndAppendsAcrossRolls() {
        final File path = getTmpDir();
        final SetTimeProvider time = new SetTimeProvider();
        time.currentTimeMillis(0);
        final long oldIndex;

        // Explicitly retain the previous TEST_SECONDLY index geometry so this
        // file remains a legacy-layout fixture when the enum default changes.
        try (SingleChronicleQueue queue = smallBuilder(path, time).indexCount(32_768).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("legacy");
            oldIndex = appender.lastIndexAppended();
            assertEquals(32_768, ((StoreAppender) appender).store.indexing.indexCount());
        }

        final long newIndex;
        try (SingleChronicleQueue queue = smallBuilder(path, time).build();
             ExcerptTailer tailer = queue.createTailer();
             ExcerptAppender appender = queue.createAppender()) {
            assertTrue(tailer.moveToIndex(oldIndex));
            assertEquals("legacy", tailer.readText());
            appender.writeText("appended-to-legacy");
            assertEquals(32_768, ((StoreAppender) appender).store.indexing.indexCount());
            time.advanceMillis(1_000);
            appender.writeText("next-roll");
            newIndex = appender.lastIndexAppended();
            assertEquals(2_048, ((StoreAppender) appender).store.indexing.indexCount());
            assertEquals("appended-to-legacy", tailer.readText());
            assertEquals("next-roll", tailer.readText());
            assertNull(tailer.readText());
        }

        try (SingleChronicleQueue queue = smallBuilder(path, time).build();
             ExcerptTailer tailer = queue.createTailer()) {
            assertEquals("legacy", tailer.readText());
            assertEquals("appended-to-legacy", tailer.readText());
            assertEquals("next-roll", tailer.readText());
            assertTrue(tailer.moveToIndex(newIndex));
            assertEquals("next-roll", tailer.readText());
            assertTrue(tailer.moveToIndex(oldIndex));
            assertEquals("legacy", tailer.readText());
        }
    }

    private static SingleChronicleQueueBuilder smallBuilder(File path, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.binary(path)
                .rollCycle(TEST_SECONDLY)
                .blockSize(OS.SAFE_PAGE_SIZE)
                .timeProvider(time);
    }

    private static void assertRollFileLengths(File path, int count, long length) {
        final File[] files = path.listFiles((dir, name) -> name.endsWith(".cq4"));
        assertNotNull(files);
        assertEquals(count, files.length);
        for (File file : files)
            assertEquals(file.getName(), length, file.length());
    }
}
