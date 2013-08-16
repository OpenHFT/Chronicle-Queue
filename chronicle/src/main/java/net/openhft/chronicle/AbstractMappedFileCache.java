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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author peter.lawrey
 */
public abstract class AbstractMappedFileCache implements MappedFileCache {
    private static final float VERSION = 2.0f;
    private static final int HEADER_VERSION = 0;
    private static final int HEADER_INDEX_EXCERPTS = HEADER_VERSION + 4;
    private static final int HEADER_INDEX_COUNT = HEADER_INDEX_EXCERPTS + 4;
    private static final int HEADER_SIZE = 128;
    protected final File dir;
    @SuppressWarnings("FieldCanBeLocal")
    private final FileChannel masterFileChannel;
    private final MappedByteBuffer masterBuffer;
    private final ChronicleConfig config;

    public AbstractMappedFileCache(String dirPath, ChronicleConfig config) throws IOException {
        this.config = config;
        dir = new File(dirPath);
        if (!dir.isDirectory() && !dir.mkdirs())
            throw new FileNotFoundException("Unable to create directory " + dir);
        File masterFile = new File(dir, "master");
        long size = Math.max(masterFile.length(), config.indexFileCapacity() * 4);
        masterFileChannel = new RandomAccessFile(masterFile, "rw").getChannel();
        masterBuffer = masterFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        masterBuffer.order(config.byteOrder());
    }

    public void writeHeader() {
        masterBuffer.putFloat(HEADER_VERSION, VERSION);
        if (indexFileExcerpts() == 0)
            masterBuffer.putInt(HEADER_INDEX_EXCERPTS, config.indexFileExcerpts());
    }

    public float version() {
        return masterBuffer.getFloat(HEADER_VERSION);
    }

    public int indexFileExcerpts() {
        return masterBuffer.getInt(HEADER_INDEX_EXCERPTS);
    }

    public int lastIndexFileNumber() {
        return masterBuffer.getInt(HEADER_INDEX_COUNT);
    }

    public int incrLastIndexFileNumber() {
        int lastIndexFileNumber = lastIndexFileNumber() + 1;
        masterBuffer.putInt(HEADER_INDEX_COUNT, lastIndexFileNumber);
        return lastIndexFileNumber;
    }

    @Override
    public long findLast() throws IOException {
        MappedByteBuffer byteBuffer = acquireMappedBufferForIndex(lastIndexFileNumber());
        // short cut
        if (byteBuffer.getInt(8) == 0)
            return lastIndexFileNumber() * indexFileExcerpts();
        // find the line first.
        @SuppressWarnings("UnnecessaryLocalVariable")
        int lines = (indexFileExcerpts() + 13) / 14 * 16; // 14 entries in each line of 16.
        int lo = 1, hi = lines;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            long offset = byteBuffer.getLong(mid * 64);
            if (offset == 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }

        for (int i = 0; i < 14; i++) {
            int end = byteBuffer.getInt(lo * 64 + i * 4);
            if (end == 0)
                return lo * 14 + i;
        }
        return (lo + 1) * 14;
    }

    public abstract MappedByteBuffer acquireMappedBufferForIndex(int indexNumber) throws IOException;
}
