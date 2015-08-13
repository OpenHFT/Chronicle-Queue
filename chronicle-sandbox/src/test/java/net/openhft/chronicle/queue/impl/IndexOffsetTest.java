/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.impl.Indexer.IndexOffset;
import org.junit.Assert;
import org.junit.Test;

public class IndexOffsetTest {

    @Test
    public void testFindExcerpt2()   {

        Assert.assertEquals(1 * 8, IndexOffset.toAddress0(1L << (17L + 6L)));
    }

    @Test
    public void testFindExcerpt()   {

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