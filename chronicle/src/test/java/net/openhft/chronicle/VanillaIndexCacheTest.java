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

import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.NativeBytes;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class VanillaIndexCacheTest {
    @Test
    public void testIndexFor() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testIndexFor");
        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        VanillaIndexCache cache = new VanillaIndexCache(dir.getAbsolutePath(), 10 + 3, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);
        VanillaFile vanillaFile0 = cache.indexFor(cycle, 0, true);
        vanillaFile0.bytes().writeLong(0, 0x12345678);
        File file0 = vanillaFile0.file();
        assertEquals(8 << 10, file0.length());
        assertEquals(0x12345678L, vanillaFile0.bytes().readLong(0));
        vanillaFile0.decrementUsage();

        VanillaFile vanillaFile1 = cache.indexFor(cycle, 1, true);
        File file1 = vanillaFile1.file();
        assertEquals(8 << 10, file1.length());
        vanillaFile1.decrementUsage();
        assertNotEquals(vanillaFile1.file(), vanillaFile0.file());

        VanillaFile vanillaFile2 = cache.indexFor(cycle, 2, true);
        File file2 = vanillaFile2.file();
        assertEquals(8 << 10, file2.length());
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
        file0.getParentFile().getParentFile().delete();
    }

    @Test
    public void testLastIndexFile() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testLastIndexFile");
        IOTools.deleteDir(dir.getAbsolutePath());

        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        VanillaIndexCache cache = new VanillaIndexCache(dir.getAbsolutePath(), 10 + 3, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);

        // Check that the index file count starts at 0 when the data directory is empty
        assertEquals(0, cache.lastIndexFile(cycle));

        VanillaFile vanillaFile0 = cache.indexFor(cycle, 0, true);
        assertEquals("index-0", vanillaFile0.file().getName());
        vanillaFile0.decrementUsage();
        assertEquals(0, cache.lastIndexFile(cycle));

        VanillaFile vanillaFile1 = cache.indexFor(cycle, 1, true);
        assertEquals("index-1", vanillaFile1.file().getName());
        assertEquals(1, cache.lastIndexFile(cycle));

        VanillaFile vanillaFile3 = cache.indexFor(cycle, 3, true);
        assertEquals("index-3", vanillaFile3.file().getName());
        assertEquals(3, cache.lastIndexFile(cycle));

        IOTools.deleteDir(dir.getAbsolutePath());
    }

    @Test
    public void testConcurrentAppend() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testConcurrentAppend");
        IOTools.deleteDir(dir.getAbsolutePath());

        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);

        // Use a small index file size so that the test frequently generates new index files
        VanillaIndexCache cache = new VanillaIndexCache(dir.getAbsolutePath(), 5, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);
        final int numberOfTasks = 2;
        final int countPerTask = 1000;

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

        cache.close();
        IOTools.deleteDir(dir.getAbsolutePath());
    }

    private Set<Long> readAllIndexValues(final VanillaIndexCache cache, final int cycle) throws IOException {
        final Set<Long> indexValues = new TreeSet<Long>();
        for (int i = 0; i <= cache.lastIndexFile(cycle); i++) {
            final VanillaFile vanillaFile = cache.indexFor(cycle, i, false);
            indexValues.addAll(readAllIndexValues(vanillaFile));
            vanillaFile.decrementUsage();
        }
        return indexValues;
    }

    private Set<Long> readAllIndexValues(final VanillaFile vanillaFile) {
        final Set<Long> indexValues = new TreeSet<Long>();
        final NativeBytes bytes = vanillaFile.bytes();
        bytes.position(0);
        while (bytes.remaining() >= 8) {
            indexValues.add(bytes.readLong());
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
                    cache.append(cycle, counter, false);
                    counter++;
                }
                return null;
            }
        };
    }

}
