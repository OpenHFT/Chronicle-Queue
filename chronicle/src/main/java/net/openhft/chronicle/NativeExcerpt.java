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

/**
 * @author peter.lawrey
 */
public class NativeExcerpt extends AbstractNativeExcerpt implements Excerpt {
    private boolean padding = true;

    public NativeExcerpt(@NotNull IndexedChronicle chronicle) throws IOException {
        super(chronicle);
    }

    public void startExcerpt(long capacity) {
        chronicle.checkNotClosed();
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
    }

    private void writePaddedEntry() {
        int size = (int) (dataBlockSize + dataStartOffset - indexBaseForLine);
        assert size >= 0;
        if (size == 0)
            return;
        checkNewIndexLine();
        writePaddingIndexEntry(size);
        indexPositionAddr += 4;
    }

    private void writePaddingIndexEntry(int size) {
        UNSAFE.putInt(indexPositionAddr, -size);
    }

    @Override
    public boolean index(long l) {
        chronicle.checkNotClosed();
        try {
            return indexForRead(l);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void finish() {
        super.finish();

        if (chronicle.config.synchronousMode()) {
            if (dataBuffer != null) {
                dataBuffer.force();
            }
            if (indexBuffer != null) {
                indexBuffer.force();
            }
        }
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

        assert indexBaseForLine >= 0 && indexBaseForLine < 1L << 48 : "dataPositionAtStartOfLine out of bounds, was " + indexBaseForLine;

        appendToIndex();
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "=== " + dataPositionAtStartOfLine);
        indexPositionAddr += 8;
    }

    private void appendToIndex() {
        UNSAFE.putLong(indexPositionAddr, indexBaseForLine);
    }

    @NotNull
    @Override
    public Excerpt toStart() {
        index = -1;
        return this;
    }

    @NotNull
    @Override
    public Excerpt toEnd() {
        index = chronicle().size();
        try {
            indexForRead(index);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    @Override
    public boolean nextIndex() {
        chronicle.checkNotClosed();
        try {
            long index2 = index;
            if (indexForRead(index() + 1)) {
                return true;
            } else {
                // rewind on a failure
                index = index2;
            }
            if (wasPadding()) {
                index++;
                return indexForRead(index() + 1);
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }
}
