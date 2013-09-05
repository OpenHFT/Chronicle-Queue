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

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * @author peter.lawrey
 */
public class NativeExcerpt extends AbstractNativeExcerpt implements Excerpt {
    private boolean padding = true;

    public NativeExcerpt(IndexedChronicle chronicle) throws IOException {
        super(chronicle);
    }

    public void startExcerpt(long capacity) {
        // if the capacity is to large, roll the previous entry, and there was one
        if (positionAddr + capacity > dataStartAddr + dataBlockSize) {
            // check we are the start of a block.
            checkNewIndexLine();

            writePaddedEntry();

            loadNextDataBuffer();
        }

        // check we are the start of a block.
        checkNewIndexLine();

        // update the soft limitAddr
        startAddr = positionAddr;
        limitAddr = positionAddr + capacity;
        finished = false;
    }

    private void writePaddedEntry() {
        int size = (int) (dataBlockSize + dataStartOffset - indexBaseForLine);
        assert size >= 0;
        if (size == 0)
            return;
        checkNewIndexLine();
        UNSAFE.putInt(indexPositionAddr, -size);
        indexPositionAddr += 4;
    }

    @Override
    public void finish() {
        super.finish();

        if (chronicle.config.synchronousMode()) {
            dataBuffer.force();
            indexBuffer.force();
        }
    }

    @Override
    public Excerpt toEnd() {
        super.toEnd();
        return this;
    }

    void checkNewIndexLine() {
        switch ((int) (indexPositionAddr & cacheLineMask)) {
            case 0:
                newIndexLine();
                break;
            case 4:
                throw new AssertionError();
        }
    }

    void newIndexLine() {
        // check we have a valid index
        if (indexPositionAddr >= indexStartAddr + indexBlockSize) {
            loadNextIndexBuffer();
        }
        // sets the base address
        indexBaseForLine = positionAddr - dataStartAddr + dataStartOffset;

        assert indexBaseForLine >= 0 && indexBaseForLine < 1L << 48 :
                "dataPositionAtStartOfLine out of bounds, was " + indexBaseForLine;

        UNSAFE.putLong(indexPositionAddr, indexBaseForLine);
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "=== " + dataPositionAtStartOfLine);
        indexPositionAddr += 8;
    }

    @Override
    public boolean index(long l) {
        long lineNo = l / 14;
        int inLine = (int) (l % 14);
        long lineOffset = lineNo << 4;
        long indexLookup = lineOffset / (indexBlockSize / 4);
        long indexLookupMod = lineOffset % (indexBlockSize / 4);
        indexBuffer = chronicle.indexFileCache.acquireBuffer(indexLookup);
        indexStartAddr = ((DirectBuffer) indexBuffer).address();
        indexPositionAddr = indexStartAddr + (indexLookupMod << 2);
        int dataOffsetEnd = UNSAFE.getInt(indexPositionAddr + 8 + (inLine << 2));
        if (dataOffsetEnd <= 0) {
            padding = dataOffsetEnd < 0;
            return false;
        }
        indexBaseForLine = UNSAFE.getLong(indexPositionAddr);
        int startOffset = UNSAFE.getInt(indexPositionAddr + 4 + (inLine << 2));
        long dataOffsetStart = inLine == 0 ? indexBaseForLine : (indexBaseForLine + Math.abs(startOffset));
        long dataLookup = dataOffsetStart / dataBlockSize;
        long dataLookupMod = dataOffsetStart % dataBlockSize;
        MappedByteBuffer dataMBB = chronicle.dataFileCache.acquireBuffer(dataLookup);
        startAddr = positionAddr = ((DirectBuffer) dataMBB).address() + dataLookupMod;
        limitAddr = ((DirectBuffer) dataMBB).address() + (indexBaseForLine + dataOffsetEnd - dataLookup * dataBlockSize);
        padding = false;
        return true;
    }

    @Override
    public Excerpt toStart() {
        this.index(0);
        return this;
    }

    @Override
    public boolean nextIndex() {
        if (index(index() + 1)) {
            index++;
            return true;
        }
        if (wasPadding()) {
            index++;
            return index(index() + 1);
        }
        return false;
    }

    @Override
    public boolean wasPadding() {
        return padding;
    }
}
