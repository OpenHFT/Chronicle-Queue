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

import net.openhft.lang.io.MappedFile;
import net.openhft.lang.model.constraints.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

/**
 * @author peter.lawrey
 */
public class IndexedChronicle implements Chronicle {
    @NotNull
    final MappedFile indexFileCache;
    @NotNull
    final MappedFile dataFileCache;
    @NotNull
    final ChronicleConfig config;
    private final String basePath;
    // todo consider making volatile to help detect bugs in calling code.
    private long lastWrittenIndex = -1;
    private volatile boolean closed = false;

    public IndexedChronicle(@NotNull String basePath) throws FileNotFoundException {
        this(basePath, ChronicleConfig.DEFAULT);
    }

    public IndexedChronicle(@NotNull String basePath, @NotNull ChronicleConfig config) throws FileNotFoundException {
        this.basePath = basePath;
        this.config = config.clone();
        File parentFile = new File(basePath).getParentFile();
        if (parentFile != null)
            parentFile.mkdirs();
        this.indexFileCache = new MappedFile(basePath + ".index", config.indexBlockSize());
        this.dataFileCache = new MappedFile(basePath + ".data", config.dataBlockSize());

        findTheLastIndex();
    }

    public void checkNotClosed() {
        if (closed) throw new IllegalStateException(basePath + " is closed");
    }

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
        for (long block = size / indexBlockSize - 1; block >= 0; block--) {
            MappedByteBuffer mbb = null;
            try {
                mbb = indexFileCache.acquire(block).buffer();
            } catch (IOException e) {
                continue;
            }
            mbb.order(ByteOrder.nativeOrder());
            if (block > 0 && mbb.getLong(0) == 0) {
                continue;
            }
            int cacheLineSize = config.cacheLineSize();
            for (int pos = 0; pos < indexBlockSize; pos += cacheLineSize) {
                // if the next line is blank
                if (pos + cacheLineSize >= indexBlockSize || mbb.getLong(pos + cacheLineSize) == 0) {
                    // last cache line.
                    int pos2 = 8;
                    for (pos2 = 8; pos2 < cacheLineSize; pos2 += 4) {
                        if (mbb.getInt(pos + pos2) == 0)
                            break;
                    }
                    return (block * indexBlockSize + pos) / cacheLineSize * (cacheLineSize / 4 - 2) + pos2 / 4 - 3;
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
        closed = true;
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
