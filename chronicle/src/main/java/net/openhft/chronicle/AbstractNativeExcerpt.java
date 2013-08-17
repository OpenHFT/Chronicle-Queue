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
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * @author peter.lawrey
 */
public abstract class AbstractNativeExcerpt extends NativeBytes implements Excerpt {
    protected final IndexedChronicle chronicle;
    final int cacheLineMask;
    final int dataBlockSize;
    final int indexBlockSize;
    @SuppressWarnings("FieldCanBeLocal")
    MappedByteBuffer indexBuffer;
    @SuppressWarnings("FieldCanBeLocal")
    MappedByteBuffer dataBuffer;
    long index = -1;

    // relatively static
    // the start of the index block, as an address
    long indexStartAddr;
    // which index does this refer to?
    long indexStartOffset;
    // the offset in data referred to the start of the line
    long indexBaseForLine;

    // the start of the data block, as an address
    long dataStartAddr;
    // which offset does this refer to.
    long dataStartOffset;

    // the position currently writing to in the index.
    long indexPositionAddr;

    // the start of this entry
    // iherited - long startAddr;
    // iherited - long positionAddr;
    // iherited - long limitAddr;


    public AbstractNativeExcerpt(IndexedChronicle chronicle) throws IOException {
        super(0, 0, 0);
        this.chronicle = chronicle;
        cacheLineMask = (chronicle.config.cacheLineSize() - 1);
        dataBlockSize = chronicle.config.dataBlockSize();
        indexBlockSize = chronicle.config.indexBlockSize();

        loadIndexBuffer();
        loadDataBuffer();

        finished = true;
    }

    @Override
    public long index() {
        return index;
    }

    @Override
    public Excerpt toEnd() {
        index = chronicle().size() - 1;
        return this;
    }

    public void roll() {
        // nothing to do
    }

    @Override
    public long size() {
        return chronicle.size();
    }

    @Override
    public Chronicle chronicle() {
        return chronicle;
    }

    void loadNextIndexBuffer() {
        indexStartOffset += indexBlockSize;
        try {
            loadIndexBuffer();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    void loadNextDataBuffer() {
        dataStartOffset += dataBlockSize;
        try {
            loadDataBuffer();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    void loadDataBuffer() throws IOException {
        long start = System.nanoTime();
        dataBuffer = MapUtils.getMap(chronicle.dataFile, dataStartOffset, dataBlockSize);
        dataStartAddr = startAddr = positionAddr = limitAddr = ((DirectBuffer) dataBuffer).address();
        long time = System.nanoTime() - start;
        if (time > 50e3)
            System.out.println("Took " + time / 1000 + " us to obtain a data chunk");
    }

    void loadIndexBuffer() throws IOException {
        long start = System.nanoTime();
        indexBuffer = MapUtils.getMap(chronicle.indexFile, indexStartOffset, indexBlockSize);
        indexStartAddr = indexPositionAddr = ((DirectBuffer) indexBuffer).address();
        long time = System.nanoTime() - start;
        if (time > 50e3)
            System.out.println("Took " + time / 1000 + " us to obtain an index chunk");
    }

}
