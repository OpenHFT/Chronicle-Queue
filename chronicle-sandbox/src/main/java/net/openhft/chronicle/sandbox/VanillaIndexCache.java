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

import net.openhft.lang.io.NativeBytes;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class VanillaIndexCache implements Closeable {
    private static final int MAX_SIZE = 128;
    public static final String INDEX = "index-";

    private final String basePath;
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
        this.blockBits = blockBits;
        this.dateCache = dateCache;
    }

    public synchronized VanillaFile indexFor(int cycle, int indexCount) throws IOException {
        key.cycle = cycle;
        key.indexCount = indexCount << blockBits;
        VanillaFile vanillaFile = indexKeyVanillaFileMap.get(key);
        if (vanillaFile == null) {
            String cycleStr = dateCache.formatFor(cycle);
            indexKeyVanillaFileMap.put(key.clone(), vanillaFile = new VanillaFile(basePath, cycleStr, INDEX + indexCount, indexCount, 1L << blockBits));
        }
        vanillaFile.incrementUsage();
        return vanillaFile;
    }

    @Override
    public synchronized void close() throws IOException {
        for (VanillaFile vanillaFile : indexKeyVanillaFileMap.values()) {
            vanillaFile.decrementUsage();
        }
        indexKeyVanillaFileMap.clear();
    }

    public int lastIndexFile(int cycle) {
        int maxIndex = 0;
        File[] files = new File(dateCache.formatFor(cycle)).listFiles();
        if (files != null)
            for (File file : files) {
                String name = file.getName();
                if (name.startsWith(INDEX)) {
                    int index = Integer.parseInt(name.substring(INDEX.length()));
                    if (maxIndex < index)
                        maxIndex = index;
                }
            }
        return maxIndex;
    }

    public void append(int cycle, int threadId, long dataOffset) throws IOException {
        long index = ((long) threadId << 48) + dataOffset;
        for (int indexCount = lastIndexFile(cycle); indexCount < 10000; indexCount++) {
            VanillaFile file = indexFor(cycle, indexCount);
            NativeBytes bytes = file.bytes();
            while (bytes.remaining() >= 8) {
                if (bytes.compareAndSwapLong(bytes.position(), 0L, index))
                    return;
                bytes.position(bytes.position() + 8);
            }
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
