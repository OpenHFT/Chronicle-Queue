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

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class VanillaDataCache implements Closeable {
    private static final int MAX_SIZE = 128;

    private final String basePath;
    private final DataKey key = new DataKey();
    private final int blockBits;
    private final DateCache dateCache;
    private final Map<DataKey, VanillaFile> dataKeyVanillaFileMap = new LinkedHashMap<DataKey, VanillaFile>(MAX_SIZE, 1.0f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<DataKey, VanillaFile> eldest) {
            return size() >= MAX_SIZE;
        }
    };

    public VanillaDataCache(String basePath, int blockBits, DateCache dateCache) {
        this.basePath = basePath;
        this.blockBits = blockBits;
        this.dateCache = dateCache;
    }

    public VanillaFile dataFor(int cycle, int threadId, int dataCount) throws IOException {
        key.cycle = cycle;
        key.threadId = threadId;
        key.dataCount = dataCount;
        VanillaFile vanillaFile = dataKeyVanillaFileMap.get(key);
        if (vanillaFile == null) {
            String cycleStr = dateCache.formatFor(cycle);
            dataKeyVanillaFileMap.put(key.clone(), vanillaFile = new VanillaFile(basePath, cycleStr, "data-" + threadId + "-" + dataCount, 1L << blockBits));
        }
        return vanillaFile;
    }

    @Override
    public synchronized void close() throws IOException {
        for (VanillaFile vanillaFile : dataKeyVanillaFileMap.values()) {
            vanillaFile.close();
        }
        dataKeyVanillaFileMap.clear();
    }

    static class DataKey implements Cloneable {
        int cycle;
        int threadId;
        int dataCount;

        @Override
        public int hashCode() {
            return threadId * 10191 + cycle * 17 + dataCount;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof DataKey)) return false;
            DataKey key = (DataKey) obj;
            return dataCount == key.dataCount && threadId == key.threadId && cycle == key.cycle;
        }

        @Override
        protected DataKey clone() {
            try {
                return (DataKey) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }
    }
}
