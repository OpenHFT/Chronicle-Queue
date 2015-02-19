package net.openhft.lang.io;

import net.openhft.chronicle.queue.impl.Indexer;
import org.apache.mina.core.RuntimeIoException;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Created by robaustin on 18/02/15.
 */
public class ChronicleUnsafe {

    private final MappedFile mappedFile;
    private volatile MappedMemory[] mappedMemory = new MappedMemory[1];

    public static final Unsafe UNSAFE;

    static {
        try {
            @SuppressWarnings("ALL")
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);

        } catch (Exception e) {
            throw new AssertionError(e);
        }

    }

    private long chunkSize;
    private long offset;
    private final long mask;
    private long last = -1;

    public ChronicleUnsafe(MappedFile mappedFile, long shift) {
        this.mappedFile = mappedFile;
        this.chunkSize = mappedFile.blockSize();

        mask = ~((1L << shift) - 1L);

       /* System.out.println(Indexer.IndexOffset.toBinaryString(i));
        System.out.println(Indexer.IndexOffset.toScale());
        System.out.println("");*/
    }

    public long toAddress(long address) {

        long last = mask & address;

        if ((last ^ this.last) == 0) {
            return address + offset;
        }

        int chunk = (int) ((address / chunkSize));
        long remainder = address - (((long) chunk) * chunkSize);

        // resize the chunks if required
        if (chunk >= mappedMemory.length) {
            MappedMemory[] newMm = new MappedMemory[chunk + 1];
            System.arraycopy(mappedMemory, 0, newMm, 0, mappedMemory.length);
            mappedMemory = newMm;
        }

        if (mappedMemory[chunk] == null) {
            try {
                mappedMemory[chunk] = mappedFile.acquire(chunk);
            } catch (IOException e) {
                throw new RuntimeIoException(e);
            }
        }

        long address1 = mappedMemory[chunk].bytes().address();
        long result = address1 + remainder;
        this.offset = result - address;
        long start = address - remainder;
        this.start(start);
        this.end(start + chunkSize);
        this.last = last;
        return result;
    }

    private void end(long address) {
        this.end = address;
    }

    private void start(long address) {
        this.start = address;
    }


    long start;

    long start() {
        return this.start;
    }

    long end;

    long end() {
        return this.end;
    }

    public long toRemainingInChunk(long address) {

        int chunk = (int) ((address / chunkSize));
        long remainder = address - (((long) chunk) * chunkSize);

        return mappedMemory[chunk].bytes().capacity() - remainder;

    }

    public int arrayBaseOffset(Class<?> aClass) {
        return UNSAFE.arrayBaseOffset(aClass);
    }

    public int pageSize() {
        throw new UnsupportedOperationException("todo");
    }

    public long allocateMemory(int aVoid) {
        throw new UnsupportedOperationException("todo");
    }

    public long getLong(byte[] bytes, long address) {
        return UNSAFE.getLong(bytes, toAddress(address));
    }

    public long getLong(Object object, long address) {
        return UNSAFE.getLong(object, toAddress(address));
    }

    public void setMemory(long startAddress, long len, byte defaultValue) {
        long remaining = len;
        while (remaining > 0) {
            long address = toAddress(startAddress);
            long remainingInChunk = toRemainingInChunk(startAddress);
            if (remainingInChunk > remaining)
                remainingInChunk = remaining;
            UNSAFE.setMemory(address, remainingInChunk, defaultValue);
            startAddress += remainingInChunk;
            remaining -= remainingInChunk;
        }
    }

    public byte getByte(long address) {
        return UNSAFE.getByte(toAddress(address));
    }

    public void putByte(long address, byte value) {
        UNSAFE.putByte(toAddress(address), value);
    }

    public void putLong(long address, long value) {
        UNSAFE.putLong(toAddress(address), value);
    }

    public long getLong(long address) {
        return UNSAFE.getLong(toAddress(address));
    }

    public void copyMemory(Object o, long positionAddr, Object bytes, long i, long len2) {
        throw new UnsupportedOperationException("todo");
    }

    public short getShort(long address) {
        return UNSAFE.getShort(toAddress(address));
    }

    public char getChar(long address) {
        return UNSAFE.getChar(toAddress(address));
    }

    public int getInt(long address) {
        return UNSAFE.getInt(toAddress(address));
    }

    public int getIntVolatile(Object o, long address) {
        return UNSAFE.getIntVolatile(o, toAddress(address));
    }

    public long getLongVolatile(Object o, long address) {
        return UNSAFE.getLongVolatile(o, toAddress(address));
    }

    public float getFloat(long address) {
        return UNSAFE.getFloat(toAddress(address));
    }

    public double getDouble(long address) {
        return UNSAFE.getDouble(toAddress(address));
    }

    public void putShort(long address, short v) {
        UNSAFE.putShort(toAddress(address), v);
    }

    public void putChar(long address, char v) {
        UNSAFE.putChar(toAddress(address), v);
    }

    public void putInt(long address, int v) {
        UNSAFE.putInt(toAddress(address), v);
    }

    public void putOrderedInt(Object o, long address, int v) {
        UNSAFE.putOrderedInt(0, toAddress(address), v);
    }

    public boolean compareAndSwapInt(Object o, long address, int expected, int v) {
        return UNSAFE.compareAndSwapInt(o, toAddress(address), expected, v);
    }

    public void putOrderedLong(Object o, long address, long v) {
        UNSAFE.putOrderedLong(o, toAddress(address), v);
    }

    public boolean compareAndSwapLong(Object o, long address, long expected, long v) {
        return UNSAFE.compareAndSwapLong(o, toAddress(address), expected, v);
    }

    public void putFloat(long address, float v) {
        UNSAFE.putFloat(toAddress(address), v);
    }

    public void putDouble(long address, double v) {
        UNSAFE.putDouble(toAddress(address), v);
    }

    public void putLong(Object o, long address, long aLong) {
        UNSAFE.putLong(o, toAddress(address), aLong);
    }

    public void putByte(Object o, long address, byte aByte) {
        UNSAFE.putByte(o, toAddress(address), aByte);
    }

    public byte getByte(Object o, long address) {
        return UNSAFE.getByte(o, toAddress(address));
    }

    public void copyMemory(long l, long positionAddr, long length) {
        throw new UnsupportedOperationException("todo");


    }
}
