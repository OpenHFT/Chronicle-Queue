/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.chronicle;

import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.IOTools;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests aimed to check the consistency of the interfaces:
 * - toStart
 * - toEnd
 */
public class ChronicleTest  {

    protected static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    @Rule
    public final TestName testName = new TestName();

    protected synchronized String getIndexedTestPath() {
        final String path = TMP_DIR + "/ic-" + testName.getMethodName();
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getVanillaTestPath() {
        final String path = TMP_DIR + "/vc-" + testName.getMethodName();
        IOTools.deleteDir(path);

        return path;
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected void testChronicleToStartToEndBehavior(final Chronicle ch) throws IOException {
        final long items = 10;
        final ExcerptAppender ap = ch.createAppender();

        for(long i=1; i <= items; i++) {
            ap.startExcerpt();
            ap.writeLong(i);
            ap.finish();
        }

        ap.close();

        final ExcerptTailer t1 = ch.createTailer().toStart();
        assertTrue(t1.nextIndex());
        assertEquals(1, t1.readLong());
        t1.finish();
        t1.close();

        final ExcerptTailer t2 = ch.createTailer().toEnd();
        assertFalse(t2.nextIndex());
        assertEquals(items, t2.readLong());
        t2.finish();
        t2.close();
    }

    @Test
    public void testIndexedChronicleToStartToEndBehavior()  throws IOException {
       final Chronicle ch = new IndexedChronicle(getIndexedTestPath());

        testChronicleToStartToEndBehavior(ch);

        ch.close();
        ch.clear();
    }

    @Test
    public void testVanillaChronicleToStartToEndBehavior()  throws IOException {
        final Chronicle ch = new VanillaChronicle(getVanillaTestPath());

        testChronicleToStartToEndBehavior(ch);

        ch.close();
        ch.clear();
    }
}
