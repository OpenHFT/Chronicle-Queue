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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * @author peter.lawrey
 */
public class NativeExcerptAppender extends AbstractNativeExcerpt implements ExcerptAppender {
    public NativeExcerptAppender(@NotNull IndexedChronicle chronicle) throws IOException {
        super(chronicle);
        toEnd();
    }

    public void startExcerpt(long capacity) {
        // in case there is more than one appender :P
        if (index != size()) {
            toEnd();
        }
        if (capacity >= chronicle.config.dataBlockSize())
            throw new IllegalArgumentException("Capacity too large " + capacity + " >= " + chronicle.config.dataBlockSize());
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

    @Override
    public void addPaddedEntry() {
        // in case there is more than one appender :P
        if (index != lastWrittenIndex()) {
            toEnd();
        }

        // check we are the start of a block.
        checkNewIndexLine();

        writePaddedEntry();

        loadNextDataBuffer();

        // check we are the start of a block.
        checkNewIndexLine();

        finished = true;
    }

    private void writePaddedEntry() {
        int size = (int) (dataBlockSize + dataStartOffset - indexBaseForLine);
        assert size >= 0;
        if (size == 0)
            return;
        checkNewIndexLine();
        UNSAFE.putInt(indexPositionAddr, -size);
        indexPositionAddr += 4;
        index++;
        chronicle.incrSize();
    }

    @Override
    public void finish() {
        super.finish();
        if (index != chronicle.size())
            throw new ConcurrentModificationException("Chronicle appended by more than one Appender at the same time, index=" + index + ", size=" + chronicle().size());

        // push out the entry is available.  This is what the reader polls.
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "= " + (int) (dataPosition() - dataPositionAtStartOfLine));
        long offsetInBlock = positionAddr - dataStartAddr;
        assert offsetInBlock >= 0 && offsetInBlock <= dataBlockSize;
        int relativeOffset = (int) (dataStartOffset + offsetInBlock - indexBaseForLine);
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

    @NotNull
    @Override
    public ExcerptAppender toEnd() {
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
}
