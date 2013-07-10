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

/**
 * @author peter.lawrey
 */
public class NativeExcerptTailer extends NativeBytes implements ExcerptTailer, ExcerptReader {
    private final IndexedChronicle chronicle;
    @SuppressWarnings("FieldCanBeLocal")
    private MappedByteBuffer indexBuffer, dataBuffer;
    private long index = -1;

    public NativeExcerptTailer(IndexedChronicle chronicle) {
        super(0, 0, 0);
        this.chronicle = chronicle;
        newIndexLine();
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

    // relatively static
    private long indexStart = -IndexedChronicle.INDEX_BLOCK_SIZE;
    private long indexLimitAddr;
    private long dataStart = -IndexedChronicle.DATA_BLOCK_SIZE;
    // changed per line
    private long dataPositionAtStartOfLine;
    // changed per entry.
    private long indexPositionAddr;

    private void loadNextDataBuffer() throws IOException {
        dataStart += IndexedChronicle.DATA_BLOCK_SIZE;
        dataBuffer = getMap(chronicle.dataFile, dataStart, IndexedChronicle.DATA_BLOCK_SIZE);
        startAddr = positionAddr = ((DirectBuffer) dataBuffer).address();
    }

    private MappedByteBuffer getMap(FileChannel fileChannel, long start, int size) throws IOException {
        for (int i = 1; ; i++) {
            try {
                long startTime = System.nanoTime();
                MappedByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_WRITE, start, size);
                if (i > 1)
                    System.out.println("i=" + i);
                long time = System.nanoTime() - startTime;
                System.out.printf("Took %,d us to map %,d MB%n", time / 1000, size / 1024 / 1024);
                return map;
            } catch (IOException e) {
                if (e.getMessage() == null || !e.getMessage().endsWith("user-mapped section open")) {
                    throw e;
                }
                if (i < 10)
                    Thread.yield();
                else
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
            }
        }
    }

    public boolean nextIndex() {
        // update the soft limitAddr
        long offset = UNSAFE.getIntVolatile(null, indexPositionAddr) & 0xFFFFFFFFL;
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + " was " + offset);
        if (offset == 0) {
            return false;
        }

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
            if (dataPositionAtStartOfLine + offset > dataStart + IndexedChronicle.DATA_BLOCK_SIZE)
                loadNextDataBuffer();
            limitAddr = (dataPositionAtStartOfLine + offset - dataStart) + startAddr;
            indexPositionAddr += 4;
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

    private void checkNewIndexLine1() {
        if ((indexPositionAddr & (IndexedChronicle.LINE_SIZE - 1)) == 0) {
            newIndexLine();
        }
    }

    private void checkNewIndexLine2() {
        if ((indexPositionAddr & (IndexedChronicle.LINE_SIZE - 1)) == 8) {
            dataPositionAtStartOfLine = UNSAFE.getLongVolatile(null, indexPositionAddr - 8);
            if (dataPositionAtStartOfLine < 0 || dataPositionAtStartOfLine > 1L << 48)
                throw new AssertionError("Corrupt index: " + dataPositionAtStartOfLine);
            // System.out.println(Long.toHexString(indexPositionAddr - 8 - indexStartAddr + indexStart) + " WAS " + dataPositionAtStartOfLine);
        }
    }

    private void newIndexLine() {
        // check we have a valid index
        if (indexPositionAddr >= indexLimitAddr) {
            try {
                // roll index memory mapping.

                indexStart += IndexedChronicle.INDEX_BLOCK_SIZE;
                indexBuffer = getMap(chronicle.indexFile, indexStart, IndexedChronicle.INDEX_BLOCK_SIZE);
                long indexStartAddr = indexPositionAddr = ((DirectBuffer) indexBuffer).address();
                indexLimitAddr = indexStartAddr + IndexedChronicle.INDEX_BLOCK_SIZE;
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        indexPositionAddr += 8;
    }

    @Override
    public long size() {
        return chronicle.size();
    }
}
