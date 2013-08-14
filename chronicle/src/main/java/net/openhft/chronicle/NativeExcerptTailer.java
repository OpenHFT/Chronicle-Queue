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
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

/**
 * @author peter.lawrey
 */
public class NativeExcerptTailer extends NativeBytes implements ExcerptTailer, ExcerptReader {
    private static final Logger LOGGER = Logger.getLogger(NativeExcerptTailer.class.getName());
    private final IndexedChronicle chronicle;
    private final int cacheLineMask;
    private final int dataBlockSize, indexBlockSize;
    @SuppressWarnings("FieldCanBeLocal")
    private MappedByteBuffer indexBuffer, dataBuffer;
    private long index = -1;
    // relatively static
    private long indexStart;
    private long indexLimitAddr;
    private long bufferAddr = 0, dataStart;
    // changed per line
    private long dataPositionAtStartOfLine = 0;
    // changed per entry.
    private long indexPositionAddr;
    private long dataLimitAddr;

    public NativeExcerptTailer(IndexedChronicle chronicle) throws IOException {
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
    public ExcerptReader toEnd() {
        index = chronicle().size() - 1;
        return this;
    }

    @Override
    public boolean index(long l) {
        return false;
    }

    @Override
    public ExcerptReader toStart() {
        index = -1;
        return this;
    }

    @Override
    public Chronicle chronicle() {
        return chronicle;
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

    private void checkNewIndexLine1() {
        assert indexPositionAddr != 0;
        if ((indexPositionAddr & cacheLineMask) == 0) {
            newIndexLine();
        }
    }

    public boolean nextIndex() {
        // update the soft limitAddr
        assert indexPositionAddr != 0;

        if ((indexPositionAddr & cacheLineMask) == 0) {
            indexPositionAddr += 8;
        }
        long offset = UNSAFE.getInt(null, indexPositionAddr) & 0xFFFFFFFFL;
        if (offset == 0)
            offset = UNSAFE.getIntVolatile(null, indexPositionAddr) & 0xFFFFFFFFL;
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + " was " + offset);
        if (offset == 0) {
            return false;
        }
        checkNewIndexLine2();

        index++;
        return nextIndex0(offset);
    }

    private boolean nextIndex0(long offset) {
        try {
            checkNewIndexLine2();
            if (offset == 0xFFFFFFFFL) {
                indexPositionAddr += 4;
                loadNextDataBuffer();
                checkNewIndexLine1();
                checkNewIndexLine2();
                return false;
            }
            if (dataPositionAtStartOfLine + offset > dataStart + dataBlockSize)
                loadNextDataBuffer();
            assert limitAddr != 0;
            long pstartAddr = startAddr;
            startAddr = limitAddr;
            limitAddr = (dataPositionAtStartOfLine + offset - dataStart) + bufferAddr;
//            System.out.println(Long.toHexString(startAddr) + " to " + Long.toHexString(limitAddr));
            assert limitAddr <= dataLimitAddr;
            indexPositionAddr += 4;
            assert limitAddr > startAddr : "index=" + index() + " % " + index() % 14
                    + " pstartAddr: " + (pstartAddr - bufferAddr)
                    + " startAddr: " + (startAddr - bufferAddr)
                    + " limitAddr: " + (limitAddr - bufferAddr)
                    + " dataPositionAtStartOfLine: " + dataPositionAtStartOfLine
                    + " offset: " + offset
                    + " dataStart: " + dataStart
                    ;
            return true;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void finish() {
        super.finish();
        // check we are the start of a block.
        checkNewIndexLine1();
        positionAddr = limitAddr;
    }

    private void checkNewIndexLine2() {
        if ((indexPositionAddr & cacheLineMask) == 8) {
            dataPositionAtStartOfLine = UNSAFE.getLongVolatile(null, indexPositionAddr - 8);

            assert dataPositionAtStartOfLine >= 0 && dataPositionAtStartOfLine <= 1L << 48 :
                    "Corrupt index: " + dataPositionAtStartOfLine;
            // System.out.println(Long.toHexString(indexPositionAddr - 8 - indexStartAddr + indexStart) + " WAS " + dataPositionAtStartOfLine);
        }
    }

    private void newIndexLine() {
        // check we have a valid index
        assert indexPositionAddr != 0;
        assert indexLimitAddr != 0;
        if (indexPositionAddr >= indexLimitAddr) {
            newIndexLine2();
        }
    }

    private void newIndexLine2() {
        try {
            // roll index memory mapping.

            indexStart += indexBlockSize;
            loadIndexBuffer();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void loadIndexBuffer() throws IOException {
        indexBuffer = chronicle.indexFile.map(FileChannel.MapMode.READ_WRITE, indexStart, indexBlockSize);
        long indexStartAddr = indexPositionAddr = ((DirectBuffer) indexBuffer).address();
        indexLimitAddr = indexStartAddr + indexBlockSize;
    }

    @Override
    public long size() {
        return chronicle.size();
    }


}
