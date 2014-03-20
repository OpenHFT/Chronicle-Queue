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

package net.openhft.chronicle.sandbox;

import net.openhft.affinity.AffinitySupport;
import net.openhft.lang.io.IOTools;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class VanillaDataCacheTest {
    @Test
    public void testDataFor() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testDataFor");
        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        VanillaDataCache cache = new VanillaDataCache(dir.getAbsolutePath(), 10 + 6, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);
        VanillaFile vanillaFile0 = cache.dataFor(cycle, AffinitySupport.getThreadId(), 0, true);
        vanillaFile0.bytes().writeLong(0, 0x12345678);
        File file0 = vanillaFile0.file();
        assertEquals(64 << 10, file0.length());
        assertEquals(0x12345678L, vanillaFile0.bytes().readLong(0));
        vanillaFile0.decrementUsage();

        VanillaFile vanillaFile1 = cache.dataFor(cycle, AffinitySupport.getThreadId(), 1, true);
        File file1 = vanillaFile1.file();
        assertEquals(64 << 10, file1.length());
        vanillaFile1.decrementUsage();
        assertNotEquals(vanillaFile1.file(), vanillaFile0.file());

        VanillaFile vanillaFile2 = cache.dataFor(cycle, AffinitySupport.getThreadId(), 2, true);
        File file2 = vanillaFile2.file();
        assertEquals(64 << 10, file2.length());
        vanillaFile2.decrementUsage();

        assertNotEquals(vanillaFile2.file(), vanillaFile0.file());
        assertNotEquals(vanillaFile2.file(), vanillaFile1.file());
        cache.close();
        assertEquals(0, vanillaFile0.usage());
        assertEquals(0, vanillaFile1.usage());
        assertEquals(0, vanillaFile2.usage());
        // check you can delete after closing.
        assertTrue(file0.delete());
        assertTrue(file1.delete());
        assertTrue(file2.delete());
        assertTrue(file0.getParentFile().delete());
        dir.delete();
    }

    @Test
    public void testDataForPerf() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testDataForPerf");
        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        VanillaDataCache cache = new VanillaDataCache(dir.getAbsolutePath(), 10 + 7, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);
        File file0 = null;
        for (int j = 0; j < 5; j++) {
            long start = System.nanoTime();
            int runs = 10000;
            for (int i = 0; i < runs; i++) {
                VanillaFile vanillaFile0 = cache.dataFor(cycle, AffinitySupport.getThreadId(), i, true);
                vanillaFile0.bytes().writeLong(0, 0x12345678);
                file0 = vanillaFile0.file();
                assertEquals(128 << 10, file0.length());
                assertEquals(0x12345678L, vanillaFile0.bytes().readLong(0));
                vanillaFile0.decrementUsage();
                vanillaFile0.close();
                assertTrue(file0.delete());
            }
            file0.getParentFile().getParentFile().delete();
            long time = System.nanoTime() - start;
            System.out.printf("The average time was %,d us%n", time / runs / 1000);
        }
        assertTrue(file0.getParentFile().delete());
        dir.delete();
    }

    @Test
    public void testDataForLast() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testDataForLast");
        IOTools.deleteDir(dir.getAbsolutePath());

        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        VanillaDataCache cache = new VanillaDataCache(dir.getAbsolutePath(), 10 + 6, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);
        final int threadId = AffinitySupport.getThreadId();

        // Check that the data file count starts at 0 when the data directory is empty
        final VanillaFile vanillaFile0 = cache.dataForLast(cycle, threadId);
        assertEquals("data-" + threadId + "-0", vanillaFile0.file().getName());
        vanillaFile0.decrementUsage();

        // Add some more data files into the directory - use discontinuous numbers to test reading
        VanillaFile vanillaFile1 = cache.dataFor(cycle, threadId, 1, true);
        vanillaFile1.decrementUsage();

        VanillaFile vanillaFile2 = cache.dataFor(cycle, threadId, 2, true);
        vanillaFile2.decrementUsage();

        VanillaFile vanillaFile4 = cache.dataFor(cycle, threadId, 4, true);
        vanillaFile4.decrementUsage();

        cache.close();

        // Open a new cache and check that it reads the existing data files that were created above
        VanillaDataCache cache2 = new VanillaDataCache(dir.getAbsolutePath(), 10 + 6, dateCache);

        final VanillaFile lastVanillaFile = cache2.dataForLast(cycle, threadId);
        assertEquals("data-" + threadId + "-4", lastVanillaFile.file().getName());
        lastVanillaFile.decrementUsage();

        cache2.close();

        IOTools.deleteDir(dir.getAbsolutePath());
    }
}
