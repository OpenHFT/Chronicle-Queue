/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;

/**
 * see https://github.com/OpenHFT/Chronicle-Queue/issues/535
 * Created by Rob Austin
 */
public class MappedFileSafeLimitTooSmallTest extends QueueTestCommon {

    @Test
    public void testMappedFileSafeLimitTooSmall() {

        final int arraySize = 40_000;
        final int blockSize = arraySize * 6;
        byte[] data = new byte[arraySize];
        Arrays.fill(data, (byte) 'x');
        File tmpDir = getTmpDir();

        try (final ChronicleQueue queue =
                     SingleChronicleQueueBuilder.builder(tmpDir, WireType.BINARY).blockSize(blockSize).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    dc.wire().write("data").bytes(data);
                }
            }
        }

        try (final ChronicleQueue queue =
                     SingleChronicleQueueBuilder.builder(tmpDir, WireType.BINARY).blockSize(blockSize).build()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = queue.createTailer().readingDocument()) {
                    Assertions.assertArrayEquals(data, dc.wire().read("data").bytes(), "mapped file: round-trip data");
                }
            }
        }
    }
}
