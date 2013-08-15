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
public class NativeExcerptAppender extends NativeBytes implements ExcerptAppender {
    private final IndexedChronicle chronicle;
    private final int cacheLineMask;
    private final int dataBlockSize, indexBlockSize;
    @SuppressWarnings("FieldCanBeLocal")
    private MappedByteBuffer indexBuffer, dataBuffer;
    private long index = -1;
    // relatively static
    private long indexStart;
    private long indexLimitAddr;
    private long bufferAddr = 0;
    private long dataStart;
    private long dataLimitAddr;
    // changed per line
    private long dataPositionAtStartOfLine = 0;
    // changed per entry.
    private long indexPositionAddr;

    public NativeExcerptAppender(IndexedChronicle chronicle) throws IOException {
        super(0, 0, 0);
        this.chronicle = chronicle;
        cacheLineMask = (chronicle.config.cacheLineSize() - 1);
        dataBlockSize = chronicle.config.dataBlockSize();
        indexBlockSize = chronicle.config.indexBlockSize();


        indexStart = 0;
        loadIndexBuffer();

        dataStart = 0;
        loadDataBuffer();
        limitAddr = startAddr;

        finished = true;
    }

    @Override
    public long index() {
        return index;
    }

    @Override
    public ExcerptAppender toEnd() {
        index = chronicle().size() - 1;
        return this;
    }

    @Override
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

    public void startExcerpt(long capacity) {
        // check we are the start of a block.
        checkNewIndexLine();

        // if the capacity is to large, roll the previous entry, and there was one
        if (positionAddr + capacity > dataLimitAddr) {
            windToNextDataBuffer();
        }

        // update the soft limitAddr
        startAddr = positionAddr;
        limitAddr = positionAddr + capacity;
        finished = false;
    }

    private void checkNewIndexLine() {
        if ((indexPositionAddr & cacheLineMask) == 0) {
            newIndexLine();
        }
    }

    private void windToNextDataBuffer() {
        try {
            if (dataLimitAddr != 0)
                padPreviousEntry();
            loadNextDataBuffer();
            checkNewIndexLine();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void padPreviousEntry() {
        positionAddr = dataLimitAddr;
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "= 0xFFFFFFFF");
        UNSAFE.putOrderedInt(null, indexPositionAddr, 0xFFFFFFFF);
        indexPositionAddr += 4;
        index++;
    }

    private void loadNextDataBuffer() throws IOException {
        dataStart += dataBlockSize;
        loadDataBuffer();
    }

    private void loadDataBuffer() throws IOException {
        dataBuffer = MapUtils.getMap(chronicle.dataFile, dataStart, dataBlockSize);
        bufferAddr = startAddr = positionAddr = limitAddr = ((DirectBuffer) dataBuffer).address();
        dataLimitAddr = startAddr + dataBlockSize;
    }

    private long dataPosition() {
        return positionAddr - bufferAddr + dataStart;
    }

    @Override
    public void finish() {
        super.finish();

        // push out the entry is available.  This is what the reader polls.
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "= " + (int) (dataPosition() - dataPositionAtStartOfLine));
        int relativeOffset = (int) (dataPosition() - dataPositionAtStartOfLine);
        assert relativeOffset > 0;
        UNSAFE.putOrderedInt(null, indexPositionAddr, relativeOffset);
        indexPositionAddr += 4;
        index++;
        chronicle.incrSize();

        if (chronicle.config.synchronousMode()) {
            dataBuffer.force();
            indexBuffer.force();
        }
    }

    private void newIndexLine() {
        // check we have a valid index
        if (indexPositionAddr >= indexLimitAddr) {
            try {
                // roll index memory mapping.

                indexStart += indexBlockSize;
                loadIndexBuffer();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        // sets the base address
        dataPositionAtStartOfLine = dataPosition();
        assert dataPositionAtStartOfLine >= 0 && dataPositionAtStartOfLine < 1L << 48 :
                "dataPositionAtStartOfLine out of bounds, was " + dataPositionAtStartOfLine;
        assert dataPositionAtStartOfLine >= dataStart;
        assert dataPositionAtStartOfLine <= dataStart + dataBlockSize;
//        System.out.println("w "+dataPositionAtStartOfLine);
        UNSAFE.putLong(indexPositionAddr, dataPositionAtStartOfLine);
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "=== " + dataPositionAtStartOfLine);

        indexPositionAddr += 8;
    }

    private void loadIndexBuffer() throws IOException {
        indexBuffer = MapUtils.getMap(chronicle.indexFile, indexStart, indexBlockSize);
        long indexStartAddr = indexPositionAddr = ((DirectBuffer) indexBuffer).address();
        indexLimitAddr = indexStartAddr + indexBlockSize;
    }

}
