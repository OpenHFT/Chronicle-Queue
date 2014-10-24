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

import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.VanillaMappedBytes;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class VanillaIndexCacheTest extends VanillaChronicleTestBase {
    @Test
    public void testIndexFor() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        final VanillaIndexCache cache = new VanillaIndexCache(baseDir, 10 + 3, dateCache);

        try {
            int cycle = (int) (System.currentTimeMillis() / 1000);
            VanillaMappedBytes vanillaBuffer0 = cache.indexFor(cycle, 0, true);
            vanillaBuffer0.writeLong(0, 0x12345678);
            File file0 = cache.fileFor(cycle, 0, true);
            assertEquals(8 << 10, file0.length());
            assertEquals(0x12345678L, vanillaBuffer0.readLong(0));
            vanillaBuffer0.release();

            VanillaMappedBytes vanillaBuffer1 = cache.indexFor(cycle, 1, true);
            File file1 = cache.fileFor(cycle, 1, true);
            assertEquals(8 << 10, file1.length());
            vanillaBuffer1.release();
            assertNotEquals(file1, file0);

            VanillaMappedBytes vanillaBuffer2 = cache.indexFor(cycle, 2, true);
            File file2 = cache.fileFor(cycle, 2, true);
            assertEquals(8 << 10, file2.length());
            vanillaBuffer2.release();

            assertNotEquals(file2, file0);
            assertNotEquals(file2, file1);
            cache.close();
            assertEquals(0, vanillaBuffer0.refCount());
            assertEquals(0, vanillaBuffer1.refCount());
            assertEquals(0, vanillaBuffer2.refCount());

            // check you can delete after closing.
            assertTrue(file0.delete());
            assertTrue(file1.delete());
            assertTrue(file2.delete());
            assertTrue(file0.getParentFile().delete());

            cache.checkCounts(1, 1);
        } finally {
            cache.close();
            IOTools.deleteDir(baseDir);

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testLastIndexFile() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        final VanillaIndexCache cache = new VanillaIndexCache(baseDir, 10 + 3, dateCache);

        final int cycle = (int) (System.currentTimeMillis() / 1000);

        try {
            // Check that the index file count starts at 0 when the data directory is empty
            assertEquals(0, cache.lastIndexFile(cycle));

            final VanillaMappedBytes vanillaBuffer0 = cache.indexFor(cycle, 0, true);
            assertEquals("index-0", cache.fileFor(cycle, 0, true).getName());
            vanillaBuffer0.release();
            assertEquals(0, cache.lastIndexFile(cycle));

            final VanillaMappedBytes vanillaBuffer1 = cache.indexFor(cycle, 1, true);
            assertEquals("index-1", cache.fileFor(cycle, 1, true).getName());
            vanillaBuffer1.release();
            assertEquals(1, cache.lastIndexFile(cycle));

            final VanillaMappedBytes vanillaBuffer3 = cache.indexFor(cycle, 3, true);
            assertEquals("index-3", cache.fileFor(cycle, 3, true).getName());
            vanillaBuffer3.release();
            assertEquals(3, cache.lastIndexFile(cycle));

            cache.checkCounts(1, 1);
        } finally {
            cache.close();
            IOTools.deleteDir(baseDir);

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testConcurrentAppend() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);

        // Use a small index file size so that the test frequently generates new index files
        final VanillaIndexCache cache = new VanillaIndexCache(baseDir, 5, dateCache);

        final int cycle = (int) (System.currentTimeMillis() / 1000);
        final int numberOfTasks = 2;
        final int countPerTask = 1000;

        try {
            // Create tasks that append to the index
            final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
            long nextValue = countPerTask;
            for (int i = 0; i < numberOfTasks; i++) {
                final long endValue = nextValue + countPerTask;
                tasks.add(createAppendTask(cache, cycle, nextValue, endValue));
                nextValue = endValue;
            }

            // Execute tasks using a thread per task
            TestTaskExecutionUtil.executeConcurrentTasks(tasks, 30000L);

            // Verify that all values can be read back from the index
            final Set<Long> indexValues = readAllIndexValues(cache, cycle);
            assertEquals(createRangeSet(countPerTask, nextValue), indexValues);

            cache.checkCounts(1, 1);
        } finally {
            cache.close();
            IOTools.deleteDir(baseDir);

            assertFalse(new File(baseDir).exists());
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private Set<Long> readAllIndexValues(final VanillaIndexCache cache, final int cycle) throws IOException {
        final Set<Long> indexValues = new TreeSet<Long>();
        for (int i = 0; i <= cache.lastIndexFile(cycle); i++) {
            final VanillaMappedBytes vanillaBuffer = cache.indexFor(cycle, i, false);
            indexValues.addAll(readAllIndexValues(vanillaBuffer));
            vanillaBuffer.release();
        }
        return indexValues;
    }

    private Set<Long> readAllIndexValues(final VanillaMappedBytes vanillaBuffer) {
        final Set<Long> indexValues = new TreeSet<Long>();
        vanillaBuffer.position(0);
        while (vanillaBuffer.remaining() >= 8) {
            indexValues.add(vanillaBuffer.readLong());
        }
        return indexValues;
    }

    private static Set<Long> createRangeSet(final long start, final long end) {
        final Set<Long> values = new TreeSet<Long>();
        long counter = start;
        while (counter < end) {
            values.add(counter);
            counter++;
        }
        return values;
    }

    private Callable<Void> createAppendTask(final VanillaIndexCache cache, final int cycle, final long startValue, final long endValue) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                long counter = startValue;
                while (counter < endValue) {
                    final VanillaMappedBytes vmb = cache.append(cycle, counter, false, new long[1]);
                    if(vmb != null) {
                        vmb.release();
                    }

                    counter++;
                }
                return null;
            }
        };
    }

}
