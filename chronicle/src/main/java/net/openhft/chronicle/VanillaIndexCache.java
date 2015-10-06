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

import net.openhft.lang.io.FileLifecycleListener;
import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.io.VanillaMappedCache;
import net.openhft.lang.model.constraints.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VanillaIndexCache implements Closeable {
    public static final String FILE_NAME_PREFIX = "index-";

    private final String basePath;
    private final File baseFile;
    private final IndexKey key = new IndexKey();
    private final int blockBits;
    private final VanillaDateCache dateCache;
    private final VanillaMappedCache<IndexKey> cache;
    private final Map<IndexKey, File> cyclePathMap;
    private final FileLifecycleListener fileLifecycleListener;

    VanillaIndexCache(
            @NotNull ChronicleQueueBuilder.VanillaChronicleQueueBuilder builder,
            @NotNull VanillaDateCache dateCache,
            int blocksBits, FileLifecycleListener fileLifecycleListener) {
        this.fileLifecycleListener = fileLifecycleListener;
        this.baseFile = builder.path();
        this.basePath = this.baseFile.getAbsolutePath();

        this.blockBits = blocksBits;
        this.dateCache = dateCache;

        this.cache = new VanillaMappedCache<>(
            builder.indexCacheCapacity(),
            true,
            builder.cleanupOnClose()
        );

        this.cyclePathMap = new HashMap<>();
    }

    public static long append(final VanillaMappedBytes bytes, final long indexValue, final boolean synchronous) {
        // Position can be changed by another thread, so take a snapshot each loop
        // so that buffer overflows are not generated when advancing to the next
        // position. As a result, the position could step backwards when this method
        // is called concurrently, but the compareAndSwapLong call ensures that
        // data is never overwritten.

        if (bytes != null) {
            boolean endOfFile = false;
            long position = bytes.position();
            while (!endOfFile) {
                endOfFile = (bytes.limit() - position) < 8;
                if (!endOfFile) {
                    if (bytes.compareAndSwapLong(position, 0L, indexValue)) {
                        bytes.lazyPosition(position + 8);
                        if (synchronous) {
                            bytes.force();
                        }

                        return position;
                    }
                }
                position += 8;
            }
        }

        return -1;
    }

    public static long countIndices(final VanillaMappedBytes buffer) {
        long indices = 0;
        for (long offset = 0; offset < buffer.capacity(); offset += 8) {
            if (buffer.readLong(offset) != 0) {
                indices++;

            } else {
                break;
            }
        }

        return indices;
    }

    public synchronized File cyclePathFor(int cycle) {
        key.cycle = cycle;
        key.indexCount = 0;

        File path = this.cyclePathMap.get(key);
        if(path == null) {
            this.cyclePathMap.put(
                key.clone(),
                path = new File(baseFile, dateCache.formatFor(cycle))
            );
        }

        return path;
    }

    public synchronized VanillaMappedBytes indexFor(int cycle, int indexCount, boolean forAppend) throws IOException {
        key.cycle = cycle;
        key.indexCount = indexCount;

        VanillaMappedBytes vmb = this.cache.get(key);
        if(vmb == null) {
            long start = System.nanoTime();
            String name = FILE_NAME_PREFIX + indexCount;
            vmb = this.cache.put(
                key.clone(),
                VanillaChronicleUtils.mkFiles(
                    basePath,
                    dateCache.formatFor(cycle),
                        name,
                    forAppend),
                1L << blockBits,
                indexCount);
            fileLifecycleListener.onFileGrowth(new File(name), System.nanoTime() - start);
        }

        vmb.reserve();

        return vmb;
    }

    @Override
    public synchronized void close() {
        this.cache.close();
    }

    public VanillaMappedBytes append(
            int cycle, long indexValue, boolean synchronous, int lastIndex, long[] position) throws IOException {

        for (int indexCount = lastIndex; indexCount < 10000; indexCount++) {
            VanillaMappedBytes vmb = indexFor(cycle, indexCount, true);
            long position0 = append(vmb, indexValue, synchronous);
            if (position0 >= 0) {
                position[0] = position0;
                return vmb;
            }

            vmb.release();
        }

        throw new AssertionError(
            "Unable to write index" + indexValue + "on cycle " + cycle + "(" + dateCache.formatFor(cycle) + ")"
        );
    }

    int lastIndexFile() {
        int lastCycle = (int)lastCycle();
        return lastIndexFile(lastCycle);
    }

    int lastIndexFile(int cycle) {
        return lastIndexFile(cycle, 0);
    }

    int lastIndexFile(int cycle, int defaultCycle) {
        int maxIndex = -1;

        final File cyclePath = cyclePathFor(cycle);
        final String[] files = cyclePath.list();
        if (files != null) {
            for (int i=files.length - 1; i>=0; i--) {
                if (files[i].startsWith(FILE_NAME_PREFIX)) {
                    int index = Integer.parseInt(files[i].substring(FILE_NAME_PREFIX.length()));
                    if (maxIndex < index) {
                        maxIndex = index;
                    }
                }
            }
        }

        return maxIndex != -1 ? maxIndex : defaultCycle;
    }

    public long firstCycle() {
        final List<File> files = VanillaChronicleUtils.findLeafDirectories(baseFile);
        if (files.isEmpty()) {
            return -1;
        }

        long firstDate = Long.MAX_VALUE;
        for (int i=files.size() - 1; i >= 0; i--) {
            try {
                String name = files.get(i).getAbsolutePath().substring(basePath.length() + 1);
                long date = dateCache.parseCount(name);
                if (firstDate > date) {
                    firstDate = date;
                }
            } catch (ParseException ignored) {
                // ignored
            }
        }

        return firstDate;
    }

    public long lastCycle() {
        final List<File> files = VanillaChronicleUtils.findLeafDirectories(baseFile);
        if (files.isEmpty()) {
            return -1;
        }

        long firstDate = Long.MIN_VALUE;
        for (int i=files.size() - 1; i >= 0; i--) {
            try {
                String name = files.get(i).getAbsolutePath().substring(basePath.length() + 1);
                long date = dateCache.parseCount(name);
                if (firstDate < date) {
                    firstDate = date;
                }
            } catch (ParseException ignored) {
                // ignored
            }
        }

        return firstDate;
    }

    public void checkCounts(int min, int max) {
        this.cache.checkCounts(min,max);
    }

    static class IndexKey implements Cloneable {
        int cycle;
        int indexCount;

        @Override
        public int hashCode() {
            return cycle * 10191 ^ indexCount;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof IndexKey)) {
                return false;
            }

            if(obj == this) {
                return true;
            }

            IndexKey key = (IndexKey) obj;
            return indexCount == key.indexCount && cycle == key.cycle;
        }

        @Override
        protected IndexKey clone() {
            try {
                return (IndexKey) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public String toString() {
            return "IndexKey ["
                + "cycle="       + cycle      + ","
                + "indexCount="  + indexCount + "]";
        }
    }
}
