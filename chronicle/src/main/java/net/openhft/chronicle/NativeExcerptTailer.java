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
public class NativeExcerptTailer extends AbstractNativeExcerpt implements ExcerptTailer {

    public static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;

    public NativeExcerptTailer(@NotNull IndexedChronicle chronicle) throws IOException {
        super(chronicle);
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        super.toEnd();
        return this;
    }

    @Override
    public boolean index(long l) {
        return super.index(l);
    }

    public boolean nextIndex() {
        checkNextLine();
        long offset = UNSAFE.getInt(null, indexPositionAddr);
        if (offset == 0)
            offset = UNSAFE.getIntVolatile(null, indexPositionAddr);
        // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + " was " + offset);
        if (offset == 0) {
            return false;
        }
        index++;
        return nextIndex0(offset) || nextIndex1();
    }

    private boolean nextIndex1() {
        long offset;
        checkNextLine();
        offset = UNSAFE.getInt(null, indexPositionAddr);
        if (offset == 0)
            offset = UNSAFE.getIntVolatile(null, indexPositionAddr);
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
        padding = (offset < 0);
        if (padding) {
            present = false;
            offset = -offset;
        }

        checkNewIndexLine2();
        checkNewDataBlock();
        startAddr = positionAddr = limitAddr;
        setLmitAddr(offset);
        assert limitAddr > startAddr || (!present && limitAddr == startAddr);
        indexPositionAddr += 4;
        return present;
    }

    private void checkNewDataBlock() {
        if (limitAddr >= dataStartAddr + dataBlockSize)
            loadNextDataBuffer();
    }

    private void setLmitAddr(long offset) {
        long offsetInThisBuffer = indexBaseForLine + offset - dataStartOffset;
        assert offsetInThisBuffer >= 0 && offsetInThisBuffer <= dataBlockSize : "index: " + index + ", offsetInThisBuffer: " + offsetInThisBuffer;
        limitAddr = dataStartAddr + offsetInThisBuffer;
    }

    void checkNewIndexLine2() {
        if ((indexPositionAddr & cacheLineMask) == 8) {
            indexBaseForLine = UNSAFE.getLongVolatile(null, indexPositionAddr - 8);
            assert index <= 0 || indexBaseForLine > 0 : "index: " + index + " indexBaseForLine: " + indexBaseForLine;
            setLmitAddr(0);
        }
    }
}
