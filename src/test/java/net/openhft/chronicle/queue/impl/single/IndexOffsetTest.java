/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class IndexOffsetTest extends QueueTestCommon {

    private static final SCQIndexing indexing = new SCQIndexing(WireType.BINARY, 1 << 17, 1 << 6);
    private static final SCQIndexing indexing2 = new SCQIndexing(WireType.BINARY, 1 << 7, 1 << 3);

    @Test
    @DisplayName("toAddress0 maps offsets to index buckets")
    public void testFindExcerpt2() {
        Assertions.assertEquals(1, indexing.toAddress0(1L << (17L + 6L)),
                "indexing toAddress0 should map index block 1 for offset 1<<(17+6)");
        Assertions.assertEquals(1, indexing2.toAddress0(1L << (7L + 3L)),
                "indexing2 toAddress0 should map index block 1 for offset 1<<(7+3)");
    }

    @Test
    @DisplayName("toAddress1 maps sequence numbers to index offsets")
    public void testFindExcerpt() {
        Assertions.assertEquals(1, indexing.toAddress1(64), "indexing toAddress1 should map seq 64 to offset 1");
        Assertions.assertEquals(1, indexing.toAddress1(65), "indexing toAddress1 should map seq 65 to offset 1");
        Assertions.assertEquals(2, indexing.toAddress1(128), "indexing toAddress1 should map seq 128 to offset 2");
        Assertions.assertEquals(2, indexing.toAddress1(129), "indexing toAddress1 should map seq 129 to offset 2");
        Assertions.assertEquals(3, indexing.toAddress1(128 + 64), "indexing toAddress1 should map seq 192 to offset 3");

        Assertions.assertEquals(1, indexing2.toAddress1(8), "indexing2 toAddress1 should map seq 8 to offset 1");
        Assertions.assertEquals(1, indexing2.toAddress1(9), "indexing2 toAddress1 should map seq 9 to offset 1");
        Assertions.assertEquals(16, indexing2.toAddress1(128), "indexing2 toAddress1 should map seq 128 to offset 16");
        Assertions.assertEquals(16, indexing2.toAddress1(129), "indexing2 toAddress1 should map seq 129 to offset 16");
        Assertions.assertEquals(17, indexing2.toAddress1(128 + 8), "indexing2 toAddress1 should map seq 136 to offset 17");
    }
}
