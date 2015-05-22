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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class IndexedChronicle2Test extends IndexedChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testIndexedChronicle_001() throws IOException {
        final String basePath = getTestPath();
        final Chronicle ch1 = ChronicleQueueBuilder.indexed(basePath).build();

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

        tail1.close();

        final Chronicle ch2 = ChronicleQueueBuilder.indexed(basePath).build();
        ExcerptTailer tail2 = ch1.createTailer().toStart();
        for(long i=0;i<100;i++) {
            assertTrue(tail2.nextIndex());
            assertEquals(i, tail2.readLong());
            assertEquals(i, tail2.readLong(0));
            tail2.finish();
        }

        tail2.close();

        ch1.close();
        ch2.close();

        assertClean(basePath);
    }

    @Test
    public void testIndexedChronicle_002() throws IOException {
        final String basePath = getTestPath();

        final Chronicle ch1 = ChronicleQueueBuilder.indexed(basePath)
            .test()
            .indexBlockSize(64)
            .dataBlockSize(64)
            .build();

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

        tail1.close();

        final Chronicle ch2 = ChronicleQueueBuilder.indexed(basePath)
            .test()
            .indexBlockSize(64)
            .dataBlockSize(64)
            .build();

        ExcerptTailer tail2 = ch1.createTailer().toStart();
        for(long i=0;i<100;i++) {
            assertTrue(tail2.nextIndex());
            assertEquals(i, tail2.readLong());
            assertEquals(i, tail2.readLong(0));
            tail2.finish();
        }

        tail2.close();

        ch1.close();
        ch2.close();

        assertClean(basePath);
    }

    @Test
    public void testIndexedChronicle_003() throws IOException {
        final String basePath = getTestPath();

        final long nb = 50 * 1000 * 1000;
        final Chronicle ch = ChronicleQueueBuilder.indexed(basePath).build();
        final ExcerptAppender ap = ch.createAppender();

        for(long i=0; i < nb; i++) {
            ap.startExcerpt();
            ap.writeLong(i);
            ap.finish();
        }

        ap.close();
        ch.close();

        assertClean(basePath);
    }

    @Test
    public void testExceptionSerialization() throws IOException {
        final String basePath = getTestPath();

        final Chronicle ch = ChronicleQueueBuilder.indexed(basePath)
                .useCompressedObjectSerializer(false)
                .build();

        final ExcerptAppender ap = ch.createAppender();
        final ExcerptTailer tl = ch.createTailer();

        ap.startExcerpt();
        ap.writeObject(new UnsupportedOperationException("UOE-1"));
        ap.finish();
        ap.startExcerpt();
        ap.writeObject(new UnsupportedOperationException("UOE-2", new IllegalStateException("ISE")));
        ap.finish();
        ap.close();

        {
            assertTrue(tl.nextIndex());
            Object obj1 = tl.readObject();
            assertNotNull(obj1);
            assertTrue(obj1 instanceof Throwable);
            assertTrue(obj1 instanceof UnsupportedOperationException);
            Throwable th1 = (Throwable) obj1;
            assertEquals("UOE-1", th1.getMessage());
            assertNull(th1.getCause());
            tl.finish();
        }

        {
            assertTrue(tl.nextIndex());
            Object obj2 = tl.readObject();
            assertNotNull(obj2);
            assertTrue(obj2 instanceof Throwable);
            assertTrue(obj2 instanceof UnsupportedOperationException);
            Throwable th2 = (Throwable) obj2;
            assertEquals("UOE-2", th2.getMessage());
            assertNotNull(th2.getCause());
            assertTrue(th2.getCause() instanceof Throwable);
            assertTrue(th2.getCause() instanceof IllegalStateException);
            assertEquals("ISE", th2.getCause().getMessage());
            tl.finish();
        }

        assertFalse(tl.nextIndex());

        tl.close();
        ch.close();

        assertClean(basePath);
    }

    @Test
    public void testUncompressedExceptionSerialization() throws IOException {
        final String basePath = getTestPath();

        final Chronicle ch = ChronicleQueueBuilder.indexed(basePath)
                .useCompressedObjectSerializer(false)
                .build();

        final ExcerptAppender ap = ch.createAppender();
        final ExcerptTailer tl = ch.createTailer();

        ap.startExcerpt();
        ap.writeObject(new UnsupportedOperationException("UOE-1"));
        ap.finish();
        ap.startExcerpt();
        ap.writeObject(new UnsupportedOperationException("UOE-2", new IllegalStateException("ISE")));
        ap.finish();
        ap.close();

        {
            assertTrue(tl.nextIndex());
            Object obj1 = tl.readObject();
            assertNotNull(obj1);
            assertTrue(obj1 instanceof Throwable);
            assertTrue(obj1 instanceof UnsupportedOperationException);
            Throwable th1 = (Throwable) obj1;
            assertEquals("UOE-1", th1.getMessage());
            assertNull(th1.getCause());
            tl.finish();
        }

        {
            assertTrue(tl.nextIndex());
            Object obj2 = tl.readObject();
            assertNotNull(obj2);
            assertTrue(obj2 instanceof Throwable);
            assertTrue(obj2 instanceof UnsupportedOperationException);
            Throwable th2 = (Throwable) obj2;
            assertEquals("UOE-2", th2.getMessage());
            assertNotNull(th2.getCause());
            assertTrue(th2.getCause() instanceof Throwable);
            assertTrue(th2.getCause() instanceof IllegalStateException);
            assertEquals("ISE", th2.getCause().getMessage());
            tl.finish();
        }

        assertFalse(tl.nextIndex());

        tl.close();
        ch.close();

        assertClean(basePath);
    }
}
