package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.impl.Indexer.IndexOffset;
import org.junit.Assert;
import org.junit.Test;

public class IndexOffsetTest {


    @Test
    public void testFindExcerpt2() throws Exception {

        Assert.assertEquals(1 * 8, IndexOffset.toAddress0(1L << (17L + 6L)));

    }

    @Test
    public void testFindExcerpt() throws Exception {

        Assert.assertEquals(1 * 8, IndexOffset.toAddress1(64));
        Assert.assertEquals(1 * 8, IndexOffset.toAddress1(65));
        Assert.assertEquals(2 * 8, IndexOffset.toAddress1(128));
        Assert.assertEquals(2 * 8, IndexOffset.toAddress1(129));
        Assert.assertEquals(8 + (2 * 8), IndexOffset.toAddress1(128 + 64));

    }


    public static void main(String[] args) {

        long index = 63;
        Assert.assertEquals(0, IndexOffset.toAddress0(index));
        Assert.assertEquals(0, IndexOffset.toAddress1(index));
    }

}