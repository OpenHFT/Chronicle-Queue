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

package net.openhft.lang.io;

import net.openhft.lang.io.serialization.ObjectSerializer;
import org.jetbrains.annotations.NotNull;
import sun.misc.Unsafe;

import java.io.EOFException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;


public class MappedNativeBytes extends AbstractBytes {
    /**
     * *** Access the Unsafe class *****
     */
    @NotNull
    @SuppressWarnings("ALL")
    public final ChronicleUnsafe chronicleUnsafe;

    static final int BYTES_OFFSET;
    static final int CHARS_OFFSET;

    static {
        try {
            @SuppressWarnings("ALL")
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Unsafe unsafe = (Unsafe) theUnsafe.get(null);
            BYTES_OFFSET = unsafe.arrayBaseOffset(byte[].class);
            CHARS_OFFSET = unsafe.arrayBaseOffset(char[].class);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }


    protected long positionAddr;
    protected long limitAddr;
    protected long capacityAddr;

    public MappedNativeBytes(ChronicleUnsafe chronicleUnsafe) {
        super();

        this.chronicleUnsafe = chronicleUnsafe;

        this.positionAddr = 0;
        this.limitAddr =
                this.capacityAddr = Long.MAX_VALUE;
        positionChecks(positionAddr);
    }

    public MappedNativeBytes(ObjectSerializer objectSerializer, long sliceStart, long sliceEnd, AtomicInteger refCount) {
        throw new UnsupportedOperationException();
    }


    @Override
    public MappedNativeBytes slice() {
        return new MappedNativeBytes(objectSerializer(), positionAddr, limitAddr, refCount);
    }

    @Override
    public MappedNativeBytes slice(long offset, long length) {
        long sliceStart = positionAddr + offset;
        assert   sliceStart < capacityAddr;
        long sliceEnd = sliceStart + length;
        assert sliceEnd > sliceStart && sliceEnd <= capacityAddr;
        return new MappedNativeBytes(objectSerializer(), sliceStart, sliceEnd, refCount);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        long subStart = positionAddr + start;
        if (subStart < positionAddr || subStart > limitAddr)
            throw new IndexOutOfBoundsException();
        long subEnd = positionAddr + end;
        if (subEnd < subStart || subEnd > limitAddr)
            throw new IndexOutOfBoundsException();
        if (start == end)
            return "";
        return new MappedNativeBytes(objectSerializer(), subStart, subEnd, refCount);
    }

    @Override
    public MappedNativeBytes bytes() {
        return new MappedNativeBytes(objectSerializer(), 0L, capacityAddr, refCount);
    }

    @Override
    public MappedNativeBytes bytes(long offset, long length) {
        long sliceStart = offset;
        assert sliceStart < capacityAddr;
        long sliceEnd = sliceStart + length;
        assert sliceEnd > sliceStart && sliceEnd <= capacityAddr;
        return new MappedNativeBytes(objectSerializer(), sliceStart, sliceEnd, refCount);
    }

    @Override
    public long address() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bytes zeroOut() {
        clear();

        throw new UnsupportedOperationException("todo");
        // chronicleUnsafe.setMemory( 0L, capacity(), (byte) 0);
        //return this;
    }

    @Override
    public Bytes zeroOut(long start, long end) {
        if (start < 0 || end > limit())
            throw new IllegalArgumentException("start: " + start + ", end: " + end);
        if (start >= end)
            return this;
        chronicleUnsafe.setMemory(start, end - start, (byte) 0);
        return this;
    }

    @Override
    public Bytes zeroOut(long start, long end, boolean ifNotZero) {
        return ifNotZero ? zeroOutDirty(start, end) : zeroOut(start, end);
    }

    private Bytes zeroOutDirty(long start, long end) {
        if (start < 0 || end > limit())
            throw new IllegalArgumentException("start: " + start + ", end: " + end);
        if (start >= end)
            return this;
        // get unaligned leading bytes
        while (start < end && (start & 7) != 0) {
            byte b = chronicleUnsafe.getByte(start);
            if (b != 0)
                chronicleUnsafe.putByte(start, (byte) 0);
            start++;
        }
        // check 64-bit aligned access
        while (start < end - 7) {
            long l = chronicleUnsafe.getLong(start);
            if (l != 0)
                chronicleUnsafe.putLong(start, 0L);
            start++;
        }
        // check unaligned tail
        while (start < end) {
            byte b = chronicleUnsafe.getByte(start);
            if (b != 0)
                chronicleUnsafe.putByte(start, (byte) 0);
            start++;
        }
        return this;
    }

    @Override
    public int read(@NotNull byte[] bytes, int off, int len) {
        if (len < 0 || off < 0 || off + len > bytes.length)
            throw new IllegalArgumentException();
        long left = remaining();
        if (left <= 0) return -1;
        int len2 = (int) Math.min(len, left);
        chronicleUnsafe.copyMemory(null, positionAddr, bytes, BYTES_OFFSET + off, len2);
        addPosition(len2);
        return len2;
    }

    @Override
    public byte readByte() {
        byte aByte = chronicleUnsafe.getByte(positionAddr);
        addPosition(1);
        return aByte;
    }

    @Override
    public byte readByte(long offset) {
        return chronicleUnsafe.getByte(offset);
    }

    @Override
    public void readFully(@NotNull byte[] b, int off, int len) {
        checkArrayOffs(b.length, off, len);
        long left = remaining();
        if (left < len)
            throw new IllegalStateException(new EOFException());
        chronicleUnsafe.copyMemory(null, positionAddr, b, BYTES_OFFSET + off, len);
        addPosition(len);
    }

    @Override
    public void readFully(long offset, byte[] bytes, int off, int len) {
        checkArrayOffs(bytes.length, off, len);
        chronicleUnsafe.copyMemory(null, offset, bytes, BYTES_OFFSET + off, len);
    }

    @Override
    public void readFully(@NotNull char[] data, int off, int len) {
        checkArrayOffs(data.length, off, len);
        long bytesOff = off * 2L;
        long bytesLen = len * 2L;
        long left = remaining();
        if (left < bytesLen)
            throw new IllegalStateException(new EOFException());
        chronicleUnsafe.copyMemory(null, positionAddr, data, BYTES_OFFSET + bytesOff, bytesLen);
        addPosition(bytesLen);
    }

    @Override
    public short readShort() {
        short s = chronicleUnsafe.getShort(positionAddr);
        addPosition(2);
        return s;
    }

    @Override
    public short readShort(long offset) {
        return chronicleUnsafe.getShort(offset);
    }

    @Override
    public char readChar() {
        char ch = chronicleUnsafe.getChar(positionAddr);
        addPosition(2);
        return ch;
    }

    @Override
    public char readChar(long offset) {
        return chronicleUnsafe.getChar(offset);
    }

    @Override
    public int readInt() {
        int i = chronicleUnsafe.getInt(positionAddr);
        addPosition(4);
        return i;
    }

    @Override
    public int readInt(long offset) {
        return chronicleUnsafe.getInt(offset);
    }

    @Override
    public int readVolatileInt() {
        int i = chronicleUnsafe.getIntVolatile(null, positionAddr);
        addPosition(4);
        return i;
    }

    @Override
    public int readVolatileInt(long offset) {
        return chronicleUnsafe.getIntVolatile(null, offset);
    }

    @Override
    public long readLong() {
        long l = chronicleUnsafe.getLong(positionAddr);
        addPosition(8);
        return l;
    }

    @Override
    public long readLong(long offset) {
        return chronicleUnsafe.getLong(offset);
    }

    @Override
    public long readVolatileLong() {
        long l = chronicleUnsafe.getLongVolatile(null, positionAddr);
        addPosition(8);
        return l;
    }

    @Override
    public long readVolatileLong(long offset) {
        return chronicleUnsafe.getLongVolatile(null, offset);
    }

    @Override
    public float readFloat() {
        float f = chronicleUnsafe.getFloat(positionAddr);
        addPosition(4);
        return f;
    }

    @Override
    public float readFloat(long offset) {
        return chronicleUnsafe.getFloat(offset);
    }

    @Override
    public double readDouble() {
        double d = chronicleUnsafe.getDouble(positionAddr);
        addPosition(8);
        return d;
    }

    @Override
    public double readDouble(long offset) {
        return chronicleUnsafe.getDouble(offset);
    }

    @Override
    public void write(int b) {

        chronicleUnsafe.putByte(positionAddr, (byte) b);
        incrementPositionAddr(1);

    }

    @Override
    public void writeByte(long offset, int b) {
        offsetChecks(offset, 1L);
        chronicleUnsafe.putByte(offset, (byte) b);
    }

    @Override
    public void write(long offset, @NotNull byte[] bytes) {
        if (offset < 0 || offset + bytes.length > capacity())
            throw new IllegalArgumentException();
        chronicleUnsafe.copyMemory(bytes, BYTES_OFFSET, null, offset, bytes.length);
        addPosition(bytes.length);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        if (off < 0 || off + len > bytes.length || len > remaining())
            throw new IllegalArgumentException();
        chronicleUnsafe.copyMemory(bytes, BYTES_OFFSET + off, null, positionAddr, len);
        addPosition(len);
    }

    @Override
    public void write(long offset, byte[] bytes, int off, int len) {
        if (offset < 0 || off + len > bytes.length || offset + len > capacity())
            throw new IllegalArgumentException();
        chronicleUnsafe.copyMemory(bytes, BYTES_OFFSET + off, null, offset, len);
    }

    @Override
    public void writeShort(int v) {
        positionChecks(positionAddr + 2L);
        chronicleUnsafe.putShort(positionAddr, (short) v);
        positionAddr += 2L;
    }


    private long incrementPositionAddr(long value) {
        positionAddr(positionAddr() + value);
        return positionAddr();
    }

    @Override
    public void writeShort(long offset, int v) {
        offsetChecks(offset, 2L);
        chronicleUnsafe.putShort(offset, (short) v);
    }

    @Override
    public void writeChar(int v) {
        positionChecks(positionAddr + 2L);
        chronicleUnsafe.putChar(positionAddr, (char) v);
        positionAddr += 2L;
    }

    void addPosition(long delta) {
        positionAddr(positionAddr() + delta);
    }

    @Override
    public void writeChar(long offset, int v) {
        offsetChecks(offset, 2L);
        chronicleUnsafe.putChar(offset, (char) v);
    }

    @Override
    public void writeInt(int v) {
        positionChecks(positionAddr + 4L);
        chronicleUnsafe.putInt(positionAddr, v);
        positionAddr += 4L;
    }

    @Override
    public void writeInt(long offset, int v) {
        offsetChecks(offset, 4L);
        chronicleUnsafe.putInt(offset, v);
    }

    @Override
    public void writeOrderedInt(int v) {
        positionChecks(positionAddr + 4L);
        chronicleUnsafe.putOrderedInt(null, positionAddr, v);
        positionAddr += 4L;
    }

    @Override
    public void writeOrderedInt(long offset, int v) {
        offsetChecks(offset, 4L);
        chronicleUnsafe.putOrderedInt(null, offset, v);
    }

    @Override
    public boolean compareAndSwapInt(long offset, int expected, int x) {
        offsetChecks(offset, 4L);
        return chronicleUnsafe.compareAndSwapInt(null, offset, expected, x);
    }

    @Override
    public void writeLong(long v) {
        positionChecks(positionAddr + 8L);
        chronicleUnsafe.putLong(positionAddr, v);
        positionAddr += 8L;
    }

    @Override
    public void writeLong(long offset, long v) {
        offsetChecks(offset, 8L);
        chronicleUnsafe.putLong(offset, v);
    }

    @Override
    public void writeOrderedLong(long v) {
        positionChecks(positionAddr + 8L);
        chronicleUnsafe.putOrderedLong(null, positionAddr, v);
        positionAddr += 8L;
    }

    @Override
    public void writeOrderedLong(long offset, long v) {
        offsetChecks(offset, 8L);
        chronicleUnsafe.putOrderedLong(null, offset, v);
    }

    @Override
    public boolean compareAndSwapLong(long offset, long expected, long x) {
        offsetChecks(offset, 8L);
        return chronicleUnsafe.compareAndSwapLong(null, offset, expected, x);
    }

    @Override
    public void writeFloat(float v) {
        positionChecks(positionAddr + 4L);
        chronicleUnsafe.putFloat(positionAddr, v);
        positionAddr += 4L;
    }

    @Override
    public void writeFloat(long offset, float v) {
        offsetChecks(offset, 4L);
        chronicleUnsafe.putFloat(offset, v);
    }

    @Override
    public void writeDouble(double v) {
        positionChecks(positionAddr + 8L);
        chronicleUnsafe.putDouble(positionAddr, v);
        positionAddr += 8L;
    }

    @Override
    public void writeDouble(long offset, double v) {
        offsetChecks(offset, 8L);
        chronicleUnsafe.putDouble(offset, v);
    }

    @Override
    public void readObject(Object object, int start, int end) {
        int len = end - start;
        if (positionAddr + len >= limitAddr)
            throw new IndexOutOfBoundsException("Length out of bounds len: " + len);

        for (; len >= 8; len -= 8) {
            chronicleUnsafe.putLong(object, (long) start, chronicleUnsafe.getLong(positionAddr));
            incrementPositionAddr(8L);
            start += 8;
        }
        for (; len > 0; len--) {
            chronicleUnsafe.putByte(object, (long) start, chronicleUnsafe.getByte(positionAddr));
            incrementPositionAddr(1L);
            start++;
        }
    }

    @Override
    public void writeObject(Object object, int start, int end) {
        int len = end - start;

        for (; len >= 8; len -= 8) {
            positionChecks(positionAddr + 8L);
            chronicleUnsafe.putLong(positionAddr, chronicleUnsafe.getLong(object, (long) start));
            positionAddr += 8;
            start += 8;
        }
        for (; len > 0; len--) {
            positionChecks(positionAddr + 1L);
            chronicleUnsafe.putByte(positionAddr, chronicleUnsafe.getByte(object, (long) start));
            positionAddr++;
            start++;
        }
    }

    @Override
    public boolean compare(long offset, RandomDataInput input, long inputOffset, long len) {
        if (offset < 0 || inputOffset < 0 || len < 0)
            throw new IndexOutOfBoundsException();
        if (offset + len < 0 || offset + len > capacity() || inputOffset + len < 0 ||
                inputOffset + len > input.capacity()) {
            return false;
        }
        long i = 0L;
        for (; i < len - 7L; i += 8L) {
            if (chronicleUnsafe.getLong(offset + i) != input.readLong(inputOffset + i))
                return false;
        }
        if (i < len - 3L) {
            if (chronicleUnsafe.getInt(offset + i) != input.readInt(inputOffset + i))
                return false;
            i += 4L;
        }
        if (i < len - 1L) {
            if (chronicleUnsafe.getChar(offset + i) != input.readChar(inputOffset + i))
                return false;
            i += 2L;
        }
        if (i < len) {
            if (chronicleUnsafe.getByte(offset + i) != input.readByte(inputOffset + i))
                return false;
        }
        return true;
    }

    @Override
    public long position() {
        return (positionAddr);
    }

    @Override
    public MappedNativeBytes position(long position) {
        if (position < 0 || position > limit())
            throw new IndexOutOfBoundsException("position: " + position + " limit: " + limit());


        positionAddr(position);
        return this;
    }

    /**
     * Change the position acknowleging there is no thread safety assumptions. Best effort setting
     * is fine. *
     *
     * @param position to set if we can.
     * @return this
     */
    public MappedNativeBytes lazyPosition(long position) {
        if (position < 0 || position > limit())
            throw new IndexOutOfBoundsException("position: " + position + " limit: " + limit());

        // assume we don't need to no check thread safety.

        positionAddr(position);
        return this;
    }

    @Override
    public void write(RandomDataInput bytes, long position, long length) {
        if (length > remaining())
            throw new IllegalArgumentException("Attempt to write " + length + " bytes with " + remaining() + " remaining");
        if (bytes instanceof MappedNativeBytes) {
            chronicleUnsafe.copyMemory(position, positionAddr, length);
            skip(length);
        } else {
            super.write(bytes, position, length);
        }
    }

    @Override
    public long capacity() {
        return (capacityAddr);
    }

    @Override
    public long remaining() {
        return (limitAddr - positionAddr);
    }

    @Override
    public long limit() {
        return (limitAddr);
    }

    @Override
    public MappedNativeBytes limit(long limit) {
        if (limit < 0 || limit > capacity()) {
            throw new IllegalArgumentException("limit: " + limit + " capacity: " + capacity());
        }

        limitAddr = limit;
        return this;
    }

    @NotNull
    @Override
    public ByteOrder byteOrder() {
        return ByteOrder.nativeOrder();
    }

    @Override
    public void checkEndOfBuffer() throws IndexOutOfBoundsException {
        if (position() > limit()) {
            throw new IndexOutOfBoundsException(
                    "position is beyond the end of the buffer " + position() + " > " + limit());
        }
    }

    public long startAddr() {
        throw new UnsupportedOperationException();
    }

    long capacityAddr() {
        return capacityAddr;
    }

    @Override
    protected void cleanup() {
        // TODO nothing to do.
    }

    @Override
    public Bytes load() {
        int pageSize = chronicleUnsafe.pageSize();
        for (long addr = 0L; addr < capacityAddr; addr += pageSize)
            chronicleUnsafe.getByte(addr);
        return this;
    }

    public void alignPositionAddr(int powerOf2) {
        long value = (positionAddr + powerOf2 - 1) & ~(powerOf2 - 1);
        positionAddr(value);
    }


    public void positionAddr(long positionAddr) {
        positionChecks(positionAddr);
        this.positionAddr = positionAddr;
    }

    void positionChecks(long positionAddr) {
        assert actualPositionChecks(positionAddr);
    }

    boolean actualPositionChecks(long positionAddr) {
        if (positionAddr < 0L)
            throw new IndexOutOfBoundsException("position before the start by " + (0L - positionAddr) + " bytes");
        if (positionAddr > limitAddr)
            throw new IndexOutOfBoundsException("position after the limit by " + (positionAddr - limitAddr) + " bytes");

        return true;
    }

    void offsetChecks(long offset, long len) {
        assert actualOffsetChecks(offset, len);
    }

    boolean actualOffsetChecks(long offset, long len) {
        if (offset < 0L || offset + len > capacity())
            throw new IndexOutOfBoundsException("offset out of bounds: " + offset + ", len: " +
                    len + ", capacity: " + capacity());
        return true;
    }

    public long positionAddr() {
        return positionAddr;
    }

    @Override
    public ByteBuffer sliceAsByteBuffer(ByteBuffer toReuse) {
        return sliceAsByteBuffer(toReuse, null);
    }

    protected ByteBuffer sliceAsByteBuffer(ByteBuffer toReuse, Object att) {
        return ByteBufferReuse.INSTANCE.reuse(positionAddr, (int) remaining(), att, toReuse);
    }
}
