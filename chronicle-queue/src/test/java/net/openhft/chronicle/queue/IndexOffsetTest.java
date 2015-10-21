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

package net.openhft.chronicle.queue;


import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.queue.impl.single.work.in.progress.Indexer.IndexOffset.*;

public class IndexOffsetTest {


    @Test
    public void testFindExcerpt2() throws Exception {

        Assert.assertEquals(1 * 8, toAddress0(1L << (17L + 6L)));

    }

    @Test
    public void testFindExcerpt() throws Exception {

        Assert.assertEquals(1 * 8, toAddress1(64));
        Assert.assertEquals(1 * 8, toAddress1(65));
        Assert.assertEquals(2 * 8, toAddress1(128));
        Assert.assertEquals(2 * 8, toAddress1(129));
        Assert.assertEquals(8 + (2 * 8), toAddress1(128 + 64));

    }
}