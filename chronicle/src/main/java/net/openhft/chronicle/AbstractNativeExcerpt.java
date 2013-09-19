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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * @author peter.lawrey
 */
public abstract class AbstractNativeExcerpt extends NativeBytes implements ExcerptCommon {
    @NotNull
    protected final IndexedChronicle chronicle;
    final int cacheLineMask;
    final int dataBlockSize;
    final int indexBlockSize;
    final int indexEntriesPerLine;
    final int indexEntriesPerBlock;
    private final int cacheLineSize;
    @Nullable
    @SuppressWarnings("FieldCanBeLocal")
    MappedByteBuffer indexBuffer;
    @Nullable
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
    boolean padding = true;

    // the start of this entry
    // inherited - long startAddr;
    // inherited - long positionAddr;
    // inherited - long limitAddr;


    public AbstractNativeExcerpt(@NotNull IndexedChronicle chronicle) throws IOException {
        super(0, 0, 0);
        this.chronicle = chronicle;
        cacheLineSize = chronicle.config.cacheLineSize();
        cacheLineMask = (cacheLineSize - 1);
        dataBlockSize = chronicle.config.dataBlockSize();
        indexBlockSize = chronicle.config.indexBlockSize();
        indexEntriesPerLine = (cacheLineSize - 8) / 4;
        indexEntriesPerBlock = indexBlockSize * indexEntriesPerLine / cacheLineSize;
        loadIndexBuffer();
        loadDataBuffer();

        finished = true;
    }

    @Override
    public long index() {
        return index;
    }

    @NotNull
    @Override
    public ExcerptCommon toEnd() {
        index = chronicle().size();
        index(index);
        return this;
    }

    @Override
    public boolean index(long l) {
        if (l < 0) {
            padding = true;
            return false;
        }
        long indexLookup = l / indexEntriesPerBlock;
        indexBuffer = chronicle.indexFileCache.acquireBuffer(indexLookup, true);
        indexStartAddr = ((DirectBuffer) indexBuffer).address();

        long indexLookupMod = l % indexEntriesPerBlock;
        int indexLineEntry = (int) (indexLookupMod % indexEntriesPerLine);
        int indexLineStart = (int) (indexLookupMod / indexEntriesPerLine * cacheLineSize);
        int inLine = (indexLineEntry << 2) + 8;

        int dataOffsetEnd = UNSAFE.getInt(indexStartAddr + indexLineStart + inLine);

        indexBaseForLine = UNSAFE.getLong(indexStartAddr + indexLineStart);
        long dataOffsetStart = inLine == 0
                ? indexBaseForLine
                : (indexBaseForLine + Math.abs(UNSAFE.getInt(indexStartAddr + indexLineStart + inLine - 4)));

        long dataLookup = dataOffsetStart / dataBlockSize;
        long dataLookupMod = dataOffsetStart % dataBlockSize;
        MappedByteBuffer dataMBB = chronicle.dataFileCache.acquireBuffer(dataLookup, true);
        long dataAddr = ((DirectBuffer) dataMBB).address();

        startAddr = positionAddr = dataAddr + dataLookupMod;
        index = l;
        if (dataOffsetEnd > 0) {
            limitAddr = dataAddr + (indexBaseForLine + dataOffsetEnd - dataLookup * dataBlockSize);
            padding = false;
            return true;
        } else if (dataOffsetEnd == 0) {
            indexPositionAddr = indexStartAddr + indexLineStart + inLine;
            padding = false;
            return false;
        } else /*if (dataOffsetEnd < 0)*/ {
            padding = true;
            return false;
        }
    }

    @Override
    public boolean wasPadding() {
        return padding;
    }

    @Override
    public long lastWrittenIndex() {
        return chronicle.lastWrittenIndex();
    }

    @Override
    public long size() {
        return chronicle.size();
    }

    @NotNull
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
        dataBuffer = chronicle.dataFileCache.acquireBuffer(dataStartOffset / dataBlockSize, true);
        dataStartAddr = startAddr = positionAddr = limitAddr = ((DirectBuffer) dataBuffer).address();
    }

    void loadIndexBuffer() throws IOException {
        indexBuffer = chronicle.indexFileCache.acquireBuffer(indexStartOffset / indexBlockSize, true);
        indexStartAddr = indexPositionAddr = ((DirectBuffer) indexBuffer).address();
    }

}
