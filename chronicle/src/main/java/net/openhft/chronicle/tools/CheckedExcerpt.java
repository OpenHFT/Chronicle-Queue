/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.tools;

import net.openhft.chronicle.ExcerptCommon;
import net.openhft.lang.model.constraints.NotNull;

public class CheckedExcerpt extends WrappedExcerpt {

    public CheckedExcerpt(final @NotNull ExcerptCommon common) {
        super(common);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Override
    public void write(int i) {
        checkSpaceLeft(1);
        super.write(i);
    }

    @Override
    public void writeByte(int i) {
        checkSpaceLeft(1);
        super.writeByte(i);
    }

    @Override
    public void writeByte(long offset, int i) {
        checkSpaceLeft(offset, 1);
        super.writeByte(offset, i);
    }

    @Override
    public void writeShort(int i) {
        checkSpaceLeft(2);
        super.writeShort(i);
    }

    @Override
    public void writeShort(long offset, int i) {
        checkSpaceLeft(offset, 2);
        super.writeShort(offset, i);
    }

    @Override
    public void writeChar(int i) {
        checkSpaceLeft(1);
        super.writeChar(i);
    }

    @Override
    public void writeChar(long offset, int i) {
        checkSpaceLeft(offset, 1);
        super.writeChar(offset, i);
    }

    @Override
    public void writeInt(int i) {
        checkSpaceLeft(4);
        super.writeInt(i);
    }

    @Override
    public void writeInt(long offset, int i) {
        checkSpaceLeft(offset, 4);
        super.writeInt(offset, i);
    }

    @Override
    public void writeOrderedInt(int i) {
        checkSpaceLeft(4);
        super.writeOrderedInt(i);
    }

    @Override
    public void writeOrderedInt(long offset, int i) {
        checkSpaceLeft(offset, 4);
        super.writeOrderedInt(offset, i);
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expected, int x) {
        checkSpaceLeft(offset, 4);
        return super.compareAndSwapInt(offset, expected, x) ;
    }

    @Override
    public void writeLong(long l) {
        checkSpaceLeft(8);
        super.writeLong(l);
    }

    @Override
    public void writeLong(long offset, long l) {
        checkSpaceLeft(offset, 8);
        super.writeLong(offset, l);
    }

    @Override
    public void writeOrderedLong(long l) {
        checkSpaceLeft(8);
        super.writeOrderedLong(l);
    }

    @Override
    public void writeOrderedLong(long offset, long l) {
        checkSpaceLeft(offset, 8);
        super.writeOrderedLong(offset, l);
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expected, long x) {
        checkSpaceLeft(offset, 8);
        return super.compareAndSwapLong(offset, expected, x);
    }

    @Override
    public void writeFloat(float v) {
        checkSpaceLeft(4);
        super.writeFloat(v);
    }

    @Override
    public void writeFloat(long offset, float v) {
        checkSpaceLeft(4);
        super.writeFloat(offset, v);
    }

    @Override
    public void writeDouble(double v) {
        checkSpaceLeft(8);
        super.writeDouble(v);
    }

    @Override
    public void writeDouble(long offset, double v) {
        checkSpaceLeft(offset, 8);
        super.writeDouble(offset, v);
    }

    @Override
    public void writeObject(Object object, int start, int end) {
        checkSpaceLeft(end - start);
        super.writeObject(object, start, end);
    }

    // *************************************************************************
    //
    // *************************************************************************

    private void checkSpaceLeft(long offset, long requiredSize) {
        checkSpaceLeft0(requiredSize, limit() - offset);
    }

    private void checkSpaceLeft(long requiredSize) {
        checkSpaceLeft0(requiredSize, remaining());
    }

    private void checkSpaceLeft0(long requiredSize, long availableSize) {
        if (requiredSize > availableSize) {
            throw new IllegalStateException(
                    "Not enough space left, required is " + requiredSize + " remaining is " + availableSize
            );
        }
    }
}
