/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBlockSizeCapTest extends QueueTestCommon {
    private String oldMode;
    private String oldCap;

    @Before
    public void saveProperties() {
        oldMode = System.getProperty("chronicle.queue.testMode");
        oldCap = System.getProperty("chronicle.queue.maxBlockSize");
        System.clearProperty("chronicle.queue.testMode");
        System.clearProperty("chronicle.queue.maxBlockSize");
    }

    @After
    public void restoreProperties() {
        restore("chronicle.queue.testMode", oldMode);
        restore("chronicle.queue.maxBlockSize", oldCap);
    }

    private static void restore(String key, String value) {
        if (value == null)
            System.clearProperty(key);
        else
            System.setProperty(key, value);
    }

    private void enable(String cap) {
        System.setProperty("chronicle.queue.testMode", "true");
        System.setProperty("chronicle.queue.maxBlockSize", cap);
    }

    private SingleChronicleQueueBuilder builder() {
        return SingleChronicleQueueBuilder.binary(getTmpDir()).indexCount(8);
    }

    @Test
    public void productionIgnoresCapEvenIfMalformed() {
        long normalDefault = builder().blockSize();
        System.setProperty("chronicle.queue.maxBlockSize", "not-a-number");
        assertEquals(normalDefault, builder().blockSize());
        assertEquals(8L << 20, builder().blockSize(8L << 20).blockSize());
    }

    @Test
    public void testModeWithoutCapPreservesExistingBehaviour() {
        long original = builder().blockSize();
        System.setProperty("chronicle.queue.testMode", "true");
        assertEquals(original, builder().blockSize());
        assertEquals(8L << 20, builder().blockSize(8L << 20).blockSize());
    }

    @Test
    public void capsDefaultAndExplicitSizesWithoutIncreasingSmallRequests() {
        long original = builder().blockSize();
        enable("1048576");
        assertEquals(Math.min(original, 1L << 20), builder().blockSize());
        assertEquals(1L << 20, builder().blockSize(8L << 20).blockSize());
        assertEquals(128L << 10, builder().blockSize(128L << 10).blockSize());
        assertEquals(Math.max(SingleChronicleQueueBuilder.SMALL_BLOCK_SIZE, 256L), builder().testBlockSize().blockSize());
    }

    @Test
    public void rejectsCapBelowIndexMinimumAndHonoursExactMinimum() {
        enable("65536");
        SingleChronicleQueueBuilder builder = builder().indexCount(4096);
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, builder::blockSize);
        assertTrue(failure.getMessage().contains("131072"));
        assertThrows(IllegalArgumentException.class, builder::build);
        enable("131072");
        assertEquals(131072, builder().indexCount(4096).blockSize());
    }

    @Test
    public void rejectsInvalidCapsOnlyInTestMode() {
        for (String cap : new String[]{"0", "-1", "64k", "", "9223372036854775808"}) {
            enable(cap);
            assertThrows(IllegalArgumentException.class, this::builder);
        }
    }

    @Test
    public void capturesPolicyAtBuilderCreationButRechecksGeometry() {
        enable("1048576");
        SingleChronicleQueueBuilder builder = builder().blockSize(8L << 20);
        enable("2097152");
        assertEquals(1L << 20, builder.blockSize());
        assertEquals(2L << 20, builder().blockSize(8L << 20).blockSize());
        builder.indexCount(65536);
        assertThrows(IllegalArgumentException.class, builder::blockSize);
    }

    @Test
    public void cappedQueueReadsAcrossMappingBoundaries() {
        enable("65536");
        String payload = new String(new char[1024]).replace('\0', 'x');
        try (SingleChronicleQueue queue = builder().blockSize(4L << 20).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            assertEquals(65536, queue.blockSize());
            for (int i = 0; i < 256; i++)
                appender.writeText(payload);
            for (int i = 0; i < 256; i++)
                assertEquals(payload, tailer.readText());
            assertNull(tailer.readText());
        }
    }
}
