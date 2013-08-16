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

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class RollingChronicle implements Chronicle {
    static final int LINE_SIZE = 64;
    static final int DATA_BLOCK_SIZE = 128 * 1024 * 1024;
    static final int INDEX_BLOCK_SIZE = DATA_BLOCK_SIZE / 4;
    protected final MappedFileCache fileCache;
    private final RollingNativeExcerptAppender appender;

    public RollingChronicle(String dirPath, ChronicleConfig config) throws IOException {
        this(getMappedFileCache(dirPath, config), config);
    }

    public RollingChronicle(MappedFileCache mappedFileCache, ChronicleConfig config) throws IOException {
        fileCache = mappedFileCache;
        ChronicleConfig config1 = config.clone();
        appender = new RollingNativeExcerptAppender(this);
    }

    private static MappedFileCache getMappedFileCache(String dirPath, ChronicleConfig config) throws IOException {
        return config.minimiseFootprint()
                ? new LightMappedFileCache(dirPath, config)
                : new VanillaMappedFileCache(dirPath, config);
    }

    @Override
    public ExcerptReader createReader() {
        fileCache.randomAccess(true);
        return new RollingNativeExcerptReader(this);
    }

    @Override
    public ExcerptTailer createTailer() {
        return new RollingNativeExcerptTailer(this);
    }

    @Override
    public ExcerptAppender createAppender() {
        return appender;
    }

    @Override
    public long size() {
        return appender.size();
    }

    @Override
    public void close() throws IOException {
        fileCache.close();
    }
}
