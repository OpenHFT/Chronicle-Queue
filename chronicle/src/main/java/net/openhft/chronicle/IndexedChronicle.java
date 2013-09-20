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

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * @author peter.lawrey
 */
public class IndexedChronicle implements Chronicle {
    @NotNull
    final MappedFileCache indexFileCache;
    @NotNull
    final MappedFileCache dataFileCache;
    @NotNull
    final ChronicleConfig config;
    private final String basePath;
    // todo consider making volatile to help detect bugs in calling code.
    private long lastWrittenIndex = -1;

    public IndexedChronicle(String basePath) throws FileNotFoundException {
        this(basePath, ChronicleConfig.DEFAULT);
    }

    public IndexedChronicle(String basePath, @NotNull ChronicleConfig config) throws FileNotFoundException {
        this.basePath = basePath;
        this.config = config.clone();
        File parentFile = new File(basePath).getParentFile();
        if (parentFile != null)
            parentFile.mkdirs();
        this.indexFileCache = new PrefetchingMappedFileCache(basePath + ".index", config.indexBlockSize());
        this.dataFileCache = new PrefetchingMappedFileCache(basePath + ".data", config.dataBlockSize());

        findTheLastIndex();
    }

    @Override
    public ChronicleConfig config() {
        return config;
    }

    public long findTheLastIndex() {
        return lastWrittenIndex = findTheLastIndex0();
    }

    private long findTheLastIndex0() {
        long size = indexFileCache.size();
        if (size <= 0) {
            return -1;
        }
        int indexBlockSize = config.indexBlockSize();
        for (long block = size / indexBlockSize; block >= 0; block--) {
            MappedByteBuffer mbb = indexFileCache.acquireBuffer(block, false);
            if (block > 0 && mbb.getLong(0) == 0) {
                continue;
            }
            int cacheLineSize = config.cacheLineSize();
            for (int pos = 0; pos < indexBlockSize; pos += cacheLineSize) {
                if (mbb.getLong(pos + cacheLineSize) == 0) {
                    // last cache line.
                    int pos2 = 8;
                    for (pos2 = 8; pos2 < cacheLineSize - 4; pos += 4) {
                        if (mbb.getInt(pos + pos2) == 0)
                            break;
                    }
                    return (block * indexBlockSize + pos) / cacheLineSize * (cacheLineSize / 4 - 2) + pos / 4 - 1;
                }
            }
            return (block + 1) * indexBlockSize / cacheLineSize * (cacheLineSize / 4 - 2);
        }
        return -1;
    }

    @Override
    public long size() {
        return lastWrittenIndex + 1;
    }

    @Override
    public String name() {
        return basePath;
    }

    @Override
    public void close() throws IOException {
        this.indexFileCache.close();
        this.dataFileCache.close();
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new NativeExcerpt(this);
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new NativeExcerptTailer(this);
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new NativeExcerptAppender(this);
    }

    @Override
    public long lastWrittenIndex() {
        return lastWrittenIndex;
    }

    void incrSize() {
        lastWrittenIndex++;
    }
}
