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

import net.openhft.affinity.AffinitySupport;
import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.VanillaMappedBytes;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class VanillaDataCacheTest extends VanillaChronicleTestBase {
    @Test
    public void testDataFor() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaDateCache dateCache = new VanillaDateCache("yyyyMMddHHmmss", 1000);
        final VanillaDataCache dataCache = new VanillaDataCache(baseDir, 10 + 6, dateCache, 32, false);

        try {
            int cycle = (int) (System.currentTimeMillis() / 1000);
            VanillaMappedBytes vanillaBuffer0 = dataCache.dataFor(cycle, AffinitySupport.getThreadId(), 0, true);
            vanillaBuffer0.writeLong(0, 0x12345678);
            File file0 = dataCache.fileFor(cycle, AffinitySupport.getThreadId(), 0, true);
            assertEquals(64 << 10, file0.length());
            assertEquals(0x12345678L, vanillaBuffer0.readLong(0));
            vanillaBuffer0.release();

            VanillaMappedBytes vanillaBuffer1 = dataCache.dataFor(cycle, AffinitySupport.getThreadId(), 1, true);
            File file1 = dataCache.fileFor(cycle, AffinitySupport.getThreadId(), 1, true);
            assertEquals(64 << 10, file1.length());
            vanillaBuffer1.release();
            assertNotEquals(file1, file0);

            VanillaMappedBytes vanillaBuffer2 = dataCache.dataFor(cycle, AffinitySupport.getThreadId(), 2, true);
            File file2 = dataCache.fileFor(cycle, AffinitySupport.getThreadId(), 2, true);
            assertEquals(64 << 10, file2.length());
            vanillaBuffer2.release();

            assertNotEquals(file2, file0);
            assertNotEquals(file2, file1);
            dataCache.close();

            assertEquals(0, vanillaBuffer0.refCount());
            assertEquals(0, vanillaBuffer1.refCount());
            assertEquals(0, vanillaBuffer2.refCount());

            // check you can delete after closing.
            assertTrue(file0.delete());
            assertTrue(file1.delete());
            assertTrue(file2.delete());
            assertTrue(file0.getParentFile().delete());

            dataCache.checkCounts(1, 1);
        } finally {
            dataCache.close();
            IOTools.deleteDir(baseDir);

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testDataForPerf() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaDateCache dateCache = new VanillaDateCache("yyyyMMddHHmmss", 1000);
        final VanillaDataCache dataCache = new VanillaDataCache(baseDir, 10 + 7, dateCache, 10000, false);

        try {
            int cycle = (int) (System.currentTimeMillis() / 1000);
            File file = null;
            VanillaMappedBytes buffer = null;

            for (int j = 0; j < 5; j++) {
                long start = System.nanoTime();
                int runs = 10000;
                for (int i = 0; i < runs; i++) {
                    buffer = dataCache.dataFor(cycle, AffinitySupport.getThreadId(), i, true);
                    buffer.writeLong(0, 0x12345678);
                    file = dataCache.fileFor(cycle, AffinitySupport.getThreadId(), i, true);

                    assertEquals(128 << 10, file.length());
                    assertEquals(0x12345678L, buffer.readLong(0));

                    buffer.release(); // held by VanillaMappedCache
                    buffer.release(); // VanillaDataCache always call ackquire()
                    buffer.close();

                    assertTrue(file.delete());
                }

                long time = System.nanoTime() - start;
                System.out.printf("The average time was %,d us%n", time / runs / 1000);

                dataCache.checkCounts(0, 0);
            }
        } finally {
            dataCache.close();
            IOTools.deleteDir(baseDir);

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testFindNextDataCount() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        try {
            final VanillaDateCache dateCache = new VanillaDateCache("yyyyMMddHHmmss", 1000);
            final VanillaDataCache dataCache = new VanillaDataCache(baseDir, 10 + 6, dateCache, 32 , false);

            int cycle = (int) (System.currentTimeMillis() / 1000);
            final int threadId = AffinitySupport.getThreadId();

            // Check that the data file count starts at 0 when the data directory is empty
            assertEquals(0, dataCache.findNextDataCount(cycle, threadId));

            // Add some more data files into the directory - use discontinuous numbers to test reading
            VanillaMappedBytes vanillaBuffer1 = dataCache.dataFor(cycle, threadId, 1, true);
            vanillaBuffer1.release();

            VanillaMappedBytes vanillaBuffer2 = dataCache.dataFor(cycle, threadId, 2, true);
            vanillaBuffer2.release();

            VanillaMappedBytes vanillaBuffer4 = dataCache.dataFor(cycle, threadId, 4, true);
            vanillaBuffer4.release();

            dataCache.checkCounts(1, 1);
            dataCache.close();

            // Open a new cache and check that it reads the existing data files that were created above
            final VanillaDataCache dataCache2 = new VanillaDataCache(baseDir, 10 + 6, dateCache, 32, false);

            assertEquals(5, dataCache2.findNextDataCount(cycle, threadId));

            dataCache.checkCounts(1, 1);
            dataCache2.close();
        } finally {
            IOTools.deleteDir(baseDir);
            assertFalse(new File(baseDir).exists());
        }
    }
}
