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
    private boolean nextSynchronous;

    public NativeExcerptAppender(@NotNull IndexedChronicle chronicle) throws IOException {
        super(chronicle);
        toEnd();
    }

    @Override
    public void startExcerpt() {
        startExcerpt(chronicle.config().messageCapacity());
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

            try {
                loadNextDataBuffer();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        // check we are the start of a block.
        checkNewIndexLine();

        // update the soft limitAddr
        startAddr = positionAddr;
        limitAddr = positionAddr + capacity;
        finished = false;
        nextSynchronous = chronicle.config.synchronousMode();
    }

    public void nextSynchronous(boolean nextSynchronous) {
        this.nextSynchronous = nextSynchronous;
    }

    public boolean nextSynchronous() {
        return nextSynchronous;
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

        try {
            loadNextDataBuffer();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        // check we are the start of a block.
        checkNewIndexLine();

        finished = true;
    }

    private void writePaddedEntry() {
        int size = (int) (dataBlockSize + dataStartOffset - indexBaseForLine);
        assert size >= 0;
        if (size == 0)
            return;
        appendIndexPaddingEntry(size);
        indexPositionAddr += 4;
        index++;
        chronicle.incrSize();
    }

    private void appendIndexPaddingEntry(int size) {
        assert index < this.indexEntriesPerLine || UNSAFE.getLong(indexPositionAddr & ~cacheLineMask) != 0 : "index: " + index + ", no start of line set.";
        UNSAFE.putInt(indexPositionAddr, -size);
    }

    @Override
    public void finish() {
        super.finish();
        if (index != chronicle.size())
            throw new ConcurrentModificationException("Chronicle appended by more than one Appender at the same time, index=" + index + ", size="
                    + chronicle().size());

        // push out the entry is available. This is what the reader polls.
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "= " + (int) (dataPosition() - dataPositionAtStartOfLine));
        long offsetInBlock = positionAddr - dataStartAddr;
        assert offsetInBlock >= 0 && offsetInBlock <= dataBlockSize;
        int relativeOffset = (int) (dataStartOffset + offsetInBlock - indexBaseForLine);
        assert relativeOffset > 0;
        writeIndexEntry(relativeOffset);
        indexPositionAddr += 4;
        index++;
        chronicle.incrSize();

        if ((indexPositionAddr & cacheLineMask) == 0 && indexPositionAddr - indexStartAddr < indexBlockSize) {
            indexBaseForLine += relativeOffset;
            appendStartOfLine();
        }

        if (nextSynchronous) {
            assert dataBuffer != null;
            dataBuffer.force();
            assert indexBuffer != null;
            indexBuffer.force();
        }
    }

    private void writeIndexEntry(int relativeOffset) {
        UNSAFE.putOrderedInt(null, indexPositionAddr, relativeOffset);
    }

    @NotNull
    @Override
    public ExcerptAppender toEnd() {
        index = chronicle().size();
        try {
            indexForAppender(index);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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
            try {
                loadNextIndexBuffer();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        // sets the base address
        indexBaseForLine = positionAddr - dataStartAddr + dataStartOffset;

        assert (index == 0 || indexBaseForLine > 0) && indexBaseForLine < 1L << 48 : "dataPositionAtStartOfLine out of bounds, was " + indexBaseForLine;

        appendStartOfLine();

    }

    private void appendStartOfLine() {
        UNSAFE.putLong(indexPositionAddr, indexBaseForLine);
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "=== " + dataPositionAtStartOfLine);
        indexPositionAddr += 8;
    }
}
