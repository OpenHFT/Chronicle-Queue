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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class IndexedChronicle implements Chronicle {
    private final String basePath;
    final MappedFileCache indexFileCache;
    final MappedFileCache dataFileCache;
    final ChronicleConfig config;
    private long size = 0;

    public IndexedChronicle(String basePath) throws FileNotFoundException {
        this(basePath, ChronicleConfig.DEFAULT);
    }

    public IndexedChronicle(String basePath, ChronicleConfig config) throws FileNotFoundException {
        this.basePath = basePath;
        this.config = config.clone();
        File parentFile = new File(basePath).getParentFile();
        if (parentFile != null)
            parentFile.mkdirs();
        this.indexFileCache = new PrefetchingMappedFileCache(basePath + ".index", config.indexBlockSize());
        this.dataFileCache = new PrefetchingMappedFileCache(basePath + ".data", config.dataBlockSize());
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

    @Override
    public Excerpt createExcerpt() throws IOException {
        return new NativeExcerpt(this);
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new NativeExcerptTailer(this);
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new NativeExcerptAppender(this);
    }

    @Override
    public long size() {
        return size;
    }

    void incrSize() {
        size++;
    }
}
