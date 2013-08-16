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

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class NativeExcerptTailer extends AbstractNativeExcerpt implements ExcerptTailer, ExcerptReader {

    public static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;

    public NativeExcerptTailer(IndexedChronicle chronicle) throws IOException {
        super(chronicle);
    }

    @Override
    public ExcerptReader toEnd() {
        super.toEnd();
        return this;
    }

    @Override
    public ExcerptReader toStart() {
        index = -1;
        return this;
    }

    @Override
    public boolean index(long l) {
        if (l != size() - 1)
            throw new UnsupportedOperationException();
        return true;
    }

    public boolean nextIndex() {
        checkNextLine();
        long offset = UNSAFE.getInt(null, indexPositionAddr) & UNSIGNED_INT_MASK;
        if (offset == 0)
            offset = UNSAFE.getIntVolatile(null, indexPositionAddr) & UNSIGNED_INT_MASK;
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + " was " + offset);
        if (offset == 0) {
            return false;
        }

        index++;
        return nextIndex0(offset);
    }

    private void checkNextLine() {
        switch ((int) (indexPositionAddr & cacheLineMask)) {
            case 0:
                newIndexLine();
                // skip the base until we have the offset.
                indexPositionAddr += 8;
                break;
            case 4:
                throw new AssertionError();
        }
    }

    private void newIndexLine() {
        if (indexPositionAddr >= indexStartAddr + indexBlockSize) {
            loadNextIndexBuffer();
        }
    }

    private boolean nextIndex0(long offset) {
        boolean present = true;
        if (offset < 0) {
            present = false;
            offset = -offset;
        }
        checkNewIndexLine2();

        startAddr = limitAddr;
        setLmitAddr(offset);
        assert limitAddr > startAddr;
        return present;
    }

    private void setLmitAddr(long offset) {
        long offsetInThisBuffer = indexBaseForLine + offset - dataStartOffset;
        assert offsetInThisBuffer >= 0 && offsetInThisBuffer <= dataBlockSize : "offsetInThisBuffer: " + offsetInThisBuffer;
        limitAddr = dataStartAddr + offsetInThisBuffer;
    }

    void checkNewIndexLine2() {
        if ((indexPositionAddr & cacheLineMask) == 8) {
            indexBaseForLine = UNSAFE.getLongVolatile(null, indexPositionAddr - 8);
            setLmitAddr(0);
        }
    }
}
