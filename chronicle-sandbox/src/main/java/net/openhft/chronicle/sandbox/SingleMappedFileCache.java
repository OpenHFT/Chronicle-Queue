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

import net.openhft.chronicle.MapUtils;
import net.openhft.chronicle.MappedFileCache;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: peter.lawrey Date: 17/08/13 Time: 14:58
 */
public class SingleMappedFileCache implements MappedFileCache {
    private static final AtomicLong totalWait = new AtomicLong();
    private final FileChannel fileChannel;
    private final int blockSize;
    private long lastIndex = Long.MIN_VALUE;
    @Nullable
    private
    MappedByteBuffer lastMBB = null;

    public SingleMappedFileCache(String basePath, int blockSize) throws FileNotFoundException {
        this(new File(basePath), blockSize);
    }

    public SingleMappedFileCache(File basePath, int blockSize) throws FileNotFoundException {
        this.blockSize = blockSize;
        fileChannel = new RandomAccessFile(basePath, "rw").getChannel();
    }

    @Override
    public void excerptUsed() {
        // ignored.
    }

    @NotNull
    @Override
    public MappedByteBuffer acquireBuffer(long index, boolean prefetch) {
        if (index == lastIndex)
            return lastMBB;
        long start = System.nanoTime();
        MappedByteBuffer mappedByteBuffer;
        try {
            mappedByteBuffer = MapUtils.getMap(fileChannel, index * blockSize, blockSize);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        lastIndex = index;
        lastMBB = mappedByteBuffer;

        long time = (System.nanoTime() - start);
        if (index > 0)
            totalWait.addAndGet(time);
//            System.out.println("Took " + time + " us to obtain a data chunk");
        return mappedByteBuffer;
    }

    @Override
    public long size() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void close() {
        try {
            fileChannel.close();
        } catch (IOException ignored) {
        }
    }
}
