/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexedChronicle2Test extends IndexedChronicleTestBase {
    private static final long   WARMUP = 20000;


    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testIndexedChronicle_001() throws IOException {
        final String basePath = getTestPath();
        final Chronicle ch1 = new IndexedChronicle(basePath);

        ExcerptAppender app = ch1.createAppender();
        for(long i=0;i<100;i++) {
            app.startExcerpt();
            app.writeLong(i);
            app.finish();
        }

        app.close();

        ExcerptTailer tail1 = ch1.createTailer().toStart();
        for(long i=0;i<100;i++) {
            assertTrue(tail1.nextIndex());
            assertEquals(i, tail1.readLong());
            assertEquals(i, tail1.readLong(0));
            tail1.finish();
        }

        final Chronicle ch2 = new IndexedChronicle(basePath);
        ExcerptTailer tail2 = ch1.createTailer().toStart();
        for(long i=0;i<100;i++) {
            assertTrue(tail2.nextIndex());
            assertEquals(i, tail2.readLong());
            assertEquals(i, tail2.readLong(0));
            tail2.finish();
        }

        tail1.close();
        tail2.close();


        ch1.close();
        ch2.close();
    }

    @Test
    public void testIndexedChronicle_002() throws IOException {
        ChronicleConfig config = ChronicleConfig.TEST.clone();
        config.indexBlockSize(64);
        config.dataBlockSize(64);

        final String basePath = getTestPath();
        final Chronicle ch1 = new IndexedChronicle(basePath,config);

        ExcerptAppender app = ch1.createAppender();
        for(long i=0;i<100;i++) {
            app.startExcerpt();
            app.writeLong(i);
            app.finish();
        }

        app.close();

        ExcerptTailer tail1 = ch1.createTailer().toStart();
        for(long i=0;i<100;i++) {
            assertTrue(tail1.nextIndex());
            assertEquals(i, tail1.readLong());
            assertEquals(i, tail1.readLong(0));
            tail1.finish();
        }

        final Chronicle ch2 = new IndexedChronicle(basePath,config);
        ExcerptTailer tail2 = ch1.createTailer().toStart();
        for(long i=0;i<100;i++) {
            assertTrue(tail2.nextIndex());
            assertEquals(i, tail2.readLong());
            assertEquals(i, tail2.readLong(0));
            tail2.finish();
        }

        tail1.close();
        tail2.close();

        ch1.close();
        ch2.close();
    }

    @Test
    public void testIndexedChronicle_003() throws IOException {
        final String basePath = getTestPath();

        final long            nb = 50 * 1000 * 1000;
        final Chronicle       ch = new IndexedChronicle(basePath);
        final ExcerptAppender ap = ch.createAppender();

        for(long i=0; i < nb; i++) {
            ap.startExcerpt();
            ap.writeLong(i);
            ap.finish();
        }

        ap.close();
        ch.close();
    }
}
