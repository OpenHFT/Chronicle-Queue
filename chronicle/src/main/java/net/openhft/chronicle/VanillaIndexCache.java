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

import net.openhft.lang.io.NativeBytes;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedHashMap;
import java.util.Map;

public class VanillaIndexCache implements Closeable {
    private static final int MAX_SIZE = 32;
    private static final String INDEX = "index-";

    private final String basePath;
    private final File baseFile;
    private final IndexKey key = new IndexKey();
    private final int blockBits;
    private final DateCache dateCache;
    private final Map<IndexKey, VanillaFile> indexKeyVanillaFileMap = new LinkedHashMap<IndexKey, VanillaFile>(MAX_SIZE, 1.0f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<IndexKey, VanillaFile> eldest) {
            boolean removed = size() >= MAX_SIZE;
            if (removed) {
                VanillaFile file = eldest.getValue();
                file.decrementUsage();
                file.close();
            }
            return removed;
        }
    };

    public VanillaIndexCache(String basePath, int blockBits, DateCache dateCache) {
        this.basePath = basePath;
        baseFile = new File(basePath);
        this.blockBits = blockBits;
        this.dateCache = dateCache;
    }

    public synchronized VanillaFile indexFor(int cycle, int indexCount, boolean forAppend) throws IOException {
        key.cycle = cycle;
        key.indexCount = indexCount << blockBits;
        VanillaFile vanillaFile = indexKeyVanillaFileMap.get(key);
        if (vanillaFile == null) {
            String cycleStr = dateCache.formatFor(cycle);
            indexKeyVanillaFileMap.put(key.clone(), vanillaFile = new VanillaFile(basePath, cycleStr, INDEX + indexCount, indexCount, 1L << blockBits, forAppend));
        }
        vanillaFile.incrementUsage();
//        new Throwable("IndexFor " + vanillaFile + " as " + vanillaFile.usage()).printStackTrace();
        return vanillaFile;
    }

    @Override
    public synchronized void close() {
        for (VanillaFile vanillaFile : indexKeyVanillaFileMap.values()) {
            vanillaFile.close();
        }
        indexKeyVanillaFileMap.clear();
    }

    int lastIndexFile(int cycle) {
        return lastIndexFile(cycle, 0);
    }

    int lastIndexFile(int cycle, int defaultCycle) {
        int maxIndex = -1;
        File cyclePath = new File(baseFile, dateCache.formatFor(cycle));
        File[] files = cyclePath.listFiles();
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                if (name.startsWith(INDEX)) {
                    int index = Integer.parseInt(name.substring(INDEX.length()));
                    if (maxIndex < index)
                        maxIndex = index;
                }
            }
        }

        return maxIndex != -1 ? maxIndex : defaultCycle;
    }

    public VanillaFile append(int cycle, long indexValue, boolean synchronous) throws IOException {
        for (int indexCount = lastIndexFile(cycle, 0); indexCount < 10000; indexCount++) {
            VanillaFile file = indexFor(cycle, indexCount, true);
            if (append(file, indexValue, synchronous))
                return file;
            file.decrementUsage();
        }
        throw new AssertionError();
    }

    public static boolean append(final VanillaFile vanillaFile, final long indexValue, final boolean synchronous) {

        // Position can be changed by another thread, so take a snapshot each loop so that
        // buffer overflows are not generated when advancing to the next position.
        // As a result, the position could step backwards when this method is called concurrently,
        // but the compareAndSwapLong call ensures that data is never overwritten.

        final NativeBytes bytes = vanillaFile.bytes();
        boolean endOfFile = false;
        while (!endOfFile) {
            final long position = bytes.position();
            endOfFile = (bytes.limit() - position) < 8;
            if (!endOfFile) {
                if (bytes.compareAndSwapLong(position, 0L, indexValue)) {
                    if (synchronous)
                        vanillaFile.force();
                    return true;
                }
                bytes.position(position + 8);
            }
        }
        return false;
    }

    public static long countIndices(final VanillaFile vanillaFile) {
        final NativeBytes bytes = vanillaFile.bytes();
        long indices = 0;
        for (long offset = 0;(bytes.limit() - bytes.address() + offset) < 8;offset += 8) {
            if(bytes.readLong(offset) != 0) {
                indices++;
            } else {
                break;
            }
        }

        return indices;
    }

    public long firstCycle() {
        File[] files = baseFile.listFiles();
        if (files == null)
            return -1;

        long firstDate = Long.MAX_VALUE;
        for (File file : files) {
            try {
                long date = dateCache.parseCount(file.getName());
                if (firstDate > date)
                    firstDate = date;

            } catch (ParseException ignored) {
                // ignored
            }
        }

        return firstDate;
    }

    public long lastCycle() {
        File[] files = baseFile.listFiles();
        if (files == null)
            return -1;

        long firstDate = Long.MIN_VALUE;
        for (File file : files) {
            try {
                long date = dateCache.parseCount(file.getName());
                if (firstDate < date)
                    firstDate = date;

            } catch (ParseException ignored) {
                // ignored
            }
        }

        return firstDate;
    }

    public synchronized void checkCounts(int min, int max) {
        for (VanillaFile file : indexKeyVanillaFileMap.values()) {
            if (file.usage() < min || file.usage() > max)
                throw new IllegalStateException(file.file() + " has a count of " + file.usage());
        }
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
            if (!(obj instanceof IndexKey)) return false;
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
    }
}
