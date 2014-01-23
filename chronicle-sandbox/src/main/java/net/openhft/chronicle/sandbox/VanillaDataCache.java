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

public class VanillaDataCache implements Closeable {
    private static final int MAX_SIZE = 32;

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

    public synchronized VanillaFile dataFor(int cycle, int threadId, int dataCount, boolean forWrite) throws IOException {
        key.cycle = cycle;
        key.threadId = threadId;
        key.dataCount = dataCount;
        VanillaFile vanillaFile = dataKeyVanillaFileMap.get(key);
        if (vanillaFile == null) {
            String cycleStr = dateCache.formatFor(cycle);
            dataKeyVanillaFileMap.put(key.clone(), vanillaFile = new VanillaFile(basePath, cycleStr, "data-" + threadId + "-" + dataCount, dataCount, 1L << blockBits, forWrite));
            findEndOfData(vanillaFile);
        }
        vanillaFile.incrementUsage();
        return vanillaFile;
    }

    private void findEndOfData(VanillaFile vanillaFile) {
        NativeBytes bytes = vanillaFile.bytes();
        for (int i = 0, max = 1 << blockBits; i < max; i += 4) {
            int len = bytes.readInt(bytes.position());
            if (len == 0) {
                return;
            }
            int len2 = nextWordAlignment(~len);
            if (len2 < 0) {
                throw new IllegalStateException("Corrupted length in " + vanillaFile.file() + " " + Integer.toHexString(len));
            }
            bytes.position(bytes.position() + len2 + 4);
        }
        throw new AssertionError();
    }

    public int nextWordAlignment(int len) {
        return (len + 3) & ~3;
    }

    @Override
    public synchronized void close() {
        for (VanillaFile vanillaFile : dataKeyVanillaFileMap.values()) {
            vanillaFile.close();
        }
        dataKeyVanillaFileMap.clear();
    }

    int lastCycle = -1;
    int lastCount = -1;

    public VanillaFile dataForLast(int cycle, int threadId) throws IOException {
        String cycleStr = dateCache.formatFor(cycle);
        String dataPrefix = basePath + "/" + cycleStr + "/data-" + threadId + "-";
        if (lastCycle != cycle) {
            int maxCount = 0;
            File[] files = new File(dataPrefix).listFiles();
            if (files != null)
                for (File file : files) {
                    if (file.getName().startsWith(dataPrefix)) {
                        int count = Integer.parseInt(file.getName().substring(dataPrefix.length()));
                        if (maxCount < count)
                            maxCount = count;
                    }
                }
            lastCycle = cycle;
            lastCount = maxCount;
        }

        return dataFor(cycle, threadId, lastCount, true);
    }

    public void incrementLastCount() {
        lastCount++;
    }

    public synchronized void checkCounts(int min, int max) {
        for (VanillaFile file : dataKeyVanillaFileMap.values()) {
            if (file.usage() < min || file.usage() > max)
                throw new IllegalStateException(file.file() + " has a count of " + file.usage());
        }
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
