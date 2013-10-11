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
        writePaddingIndexEntry(size);
        indexPositionAddr += 4;
    }

    private void writePaddingIndexEntry(int size) {
        UNSAFE.putInt(indexPositionAddr, -size);
    }

    @Override
    public boolean index(long l) {
        return indexForRead(l);
    }

    @Override
    public void finish() {
        super.finish();

        if (chronicle.config.synchronousMode()) {
            dataBuffer.force();
            indexBuffer.force();
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
            loadNextIndexBuffer();
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
        indexForRead(index);
        return this;
    }

    @Override
    public boolean nextIndex() {
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
    }

    @Override
    public long findExact(ExcerptComparator comparator) {
        long lo = 0, hi = lastWrittenIndex();
        while (lo <= hi) {
            long mid = (hi + lo) >>> 1;
            if (!index(mid)) {
                if (mid > lo)
                    index(--mid);
                else
                    break;
            }
            int cmp = comparator.compare(this);
            finish();
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
                return mid; // key found
        }
        return ~lo; // -(lo + 1)
    }

    @Override
    public void findRange(long[] startEnd, ExcerptComparator comparator) {
        // lower search range
        long lo1 = 0, hi1 = lastWrittenIndex();
        // upper search range
        long lo2 = 0, hi2 = hi1;
        boolean both = true;
        // search for the low values.
        while (lo1 <= hi1) {
            long mid = (hi1 + lo1) >>> 1;
            if (!index(mid)) {
                if (mid > lo1)
                    index(--mid);
                else
                    break;
            }
            int cmp = comparator.compare(this);
            finish();

            if (cmp < 0) {
                lo1 = mid + 1;
                if (both)
                    lo2 = lo1;
            } else if (cmp > 0) {
                hi1 = mid - 1;
                if (both)
                    hi2 = hi1;
            } else {
                hi1 = mid - 1;
                if (both)
                    lo2 = mid + 1;
                both = false;
            }
        }
        // search for the high values.
        while (lo2 <= hi2) {
            long mid = (hi2 + lo2) >>> 1;
            if (!index(mid)) {
                if (mid > lo2)
                    index(--mid);
                else
                    break;
            }
            int cmp = comparator.compare(this);
            finish();

            if (cmp <= 0) {
                lo2 = mid + 1;
            } else {
                hi2 = mid - 1;
            }
        }
        startEnd[0] = lo1; // inclusive
        startEnd[1] = lo2; // exclusive
    }
}
