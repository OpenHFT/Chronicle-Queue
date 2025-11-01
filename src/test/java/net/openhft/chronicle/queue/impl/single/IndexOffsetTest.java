/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

public class IndexOffsetTest extends QueueTestCommon {

    static final SCQIndexing indexing = new SCQIndexing(WireType.BINARY, 1 << 17, 1 << 6);
    static final SCQIndexing indexing2 = new SCQIndexing(WireType.BINARY, 1 << 7, 1 << 3);

    @Test
    public void testFindExcerpt2() {
        Assert.assertEquals(1, indexing.toAddress0(1L << (17L + 6L)));
        Assert.assertEquals(1, indexing2.toAddress0(1L << (7L + 3L)));
    }

    @Test
    public void testFindExcerpt() {
        Assert.assertEquals(1, indexing.toAddress1(64));
        Assert.assertEquals(1, indexing.toAddress1(65));
        Assert.assertEquals(2, indexing.toAddress1(128));
        Assert.assertEquals(2, indexing.toAddress1(129));
        Assert.assertEquals(3, indexing.toAddress1(128 + 64));

        Assert.assertEquals(1, indexing2.toAddress1(8));
        Assert.assertEquals(1, indexing2.toAddress1(9));
        Assert.assertEquals(16, indexing2.toAddress1(128));
        Assert.assertEquals(16, indexing2.toAddress1(129));
        Assert.assertEquals(17, indexing2.toAddress1(128 + 8));
    }
}
