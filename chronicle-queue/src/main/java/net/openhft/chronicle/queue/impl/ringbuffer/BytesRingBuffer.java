/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.ref.LongReference;
import net.openhft.chronicle.bytes.ref.UncheckedLongReference;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteOrder;

/**
 * Multi writer single Reader, zero GC, ring buffer, which takes bytes
 *
 * @author Rob Austin.
 */
public class BytesRingBuffer {

    private static final int SIZE = 8;
    private static final int LOCKED = -1;
    private static final int FLAG = 1;
    private final long capacity, mask;
    @NotNull
    private final RingBuffer bytes;
    @NotNull
    private final Header header;

    public BytesRingBuffer(@NotNull final BytesStore byteStore) {
        capacity = byteStore.realCapacity() - Header.HEADER_SIZE;
        assert Maths.isPowerOf2(capacity);
        mask = capacity - 1;

        if (byteStore.writeRemaining() <= Header.HEADER_SIZE) {
            throw new IllegalStateException("The byteStore is too small, the minimum recommended " +
                    "size = (max-size-of-element x number-of-elements) + 24");
        }

        this.header = new Header(byteStore, capacity);
        this.bytes = new RingBuffer(byteStore, 0, capacity);
        this.header.setWriteUpTo(capacity);
        byteStore.writeLong(0, 0);
    }

    private static void checkSize(@NotNull Bytes using, long elementSize) {
        if (using.writeRemaining() < elementSize)
            throw new IllegalStateException("requires size=" + elementSize +
                    " bytes, but only " + using.readRemaining() + " remaining.");
    }

    public static long sizeFor(long cacacity) {
        return Maths.nextPower2(cacacity, OS.pageSize()) + Header.HEADER_SIZE;
    }

    public void clear() {
        header.clear(capacity);
    }

    /**
     * each time the ring is read, this logs the number of bytes in the write buffer, calling this
     * method resets these statistics,
     *
     * @return Long.MAX_VALUE if no read calls were made since the last time this method was called.
     */
    public long minNumberOfWriteBytesRemaining() {
        return header.getAndClearMinRemainingBytesCount();
    }

    /**
     * @return the total capacity in bytes
     */
    public long capacity() {
        return capacity;
    }

    /**
     * Inserts the specified element at the tail of this queue if it is possible to do so
     * immediately without exceeding the queue's capacity,
     *
     * @param bytes0 the {@code bytes0} that you wish to add to the ring buffer
     * @return returning {@code true} upon success and {@code false} if this queue is full.
     */
    public boolean offer(@NotNull Bytes bytes0) throws InterruptedException {

        try {

            for (; ; ) {

                long writeLocation = this.writeLocation();

                assert writeLocation >= 0;

                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                // if reading is occurring the remain capacity will only get larger, as have locked
                long remainingForWrite = remainingForWrite(writeLocation, header.getWriteUpTo());
                header.sampleRemainingForWrite(remainingForWrite);

                if (remainingForWrite < bytes0.readRemaining() + SIZE + FLAG) {
                    remainingForWrite = remainingForWrite(writeLocation, header.getWriteUpToVolatile());
                    header.sampleRemainingForWrite(remainingForWrite);
                    if (remainingForWrite < bytes0.readRemaining() + SIZE + FLAG)
                        return false;
                }

                // writeBytes the size
                long len = bytes0.readLimit();
                long messageLen = SIZE + FLAG + len;
                long offset = writeLocation;

                // we want to ensure that only one thread ever gets in here at a time
                if (!header.compareAndSetWriteLocation(writeLocation, LOCKED))
                    continue;

                // we have to set the busy fag before the writeBytes location for the reader
                // this is why we have the compareAndSet above to ensure that only one thread
                // gets in here
                final long flagLoc = offset;
                offset = this.bytes.writeByte(offset, States.BUSY.ordinal());

                if (!header.compareAndSetWriteLocation(-1, writeLocation + messageLen))
                    continue;

                // writeBytes a size
                offset += this.bytes.write(offset, len);

                // writeBytes the data
                this.bytes.write(offset, bytes0);
                this.bytes.writeByte(flagLoc, States.READY.ordinal());

                header.incrementWriteCount();
                return true;
            }

        } catch (IllegalStateException e) {
            // when the ring buffer is full
            return false;
        }
    }

    /**
     * @return spin loops to get a valid writeBytes location
     */
    private long writeLocation() {
        long writeLocation;
        for (; ; ) {
            if ((writeLocation = header.writeLocation()) != LOCKED)
                return writeLocation;
        }
    }

    private long remainingForWrite(long offset, final long writeUpTo) {
        return (writeUpTo - 1) - offset;
    }

    /**
     * Retrieves and removes the head of this queue, or returns {@code null} if this queue is
     * empty.
     *
     * @return false if this queue is empty, or a populated buffer if the element was retried
     * @throws IllegalStateException is the {@code using} buffer is not large enough
     */
    public boolean read(@NotNull Bytes using) throws
            InterruptedException,
            IllegalStateException {

        long writeLoc = header.writeLocation();
        long offset = header.getReadLocation();
        long readLocation = offset;//= this.readLocation.get();

        if (readLocation >= writeLoc) {
            writeLoc = header.writeLocationVolatile();

            if (readLocation >= writeLoc)
                return false;

        }

        assert readLocation <= writeLoc : "reader has go ahead of the writer";

        long flag = offset;

        final byte state = bytes.readVolatileByte(flag);
        offset += 1;

        // the element is currently being written to, so let wait for the writeBytes to finish
        if (state == States.BUSY.ordinal())
            return false;

        assert state == States.READY.ordinal() : " we are reading a message that we " +
                "shouldn't,  state=" + state;

        final long elementSize = bytes.readLong(offset);
        offset += 8;

        final long next = offset + elementSize;

        long start = System.nanoTime();
        // checks that the 'using' bytes is large enough
        checkSize(using, elementSize);

        bytes.read(using, offset, elementSize);
        long time = System.nanoTime() - start;
        header.copyTime(time);
        bytes.writeOrderedLong(flag, States.USED.ordinal());

        header.setWriteUpTo(next + bytes.capacity());
        header.setReadLocation(next);

        // note, not thread safe so fast but not reliable.
        header.incrementReadCount();
        return true;
    }

    public long numberOfReadsSinceLastCall() {
        return header.getAndClearReadCount();
    }

    public long numberOfWritesSinceLastCall() {
        return header.getAndClearWriteCount();
    }

    public long maxCopyTimeSinceLastCall() {
        return header.getAndClearMaxCopyTime();
    }

    private enum States {BUSY, READY, USED}


    /**
     * used to store the locations around the ring buffer or reading and writing
     */
    private static class Header {

        public static final int CACHE_LINE_SIZE = 64;
        public static final int HEADER_SIZE = CACHE_LINE_SIZE * 2;

        private final BytesStore bytesStore;
        // these fields are written using put ordered long ( so don't have to be volatile )
        private final LongReference readLocationOffsetRef;
        private final LongReference writeUpToRef;
        private final LongReference readCount;
        private final LongReference maxCopyTime;

        private final LongReference writeLocation;
        private final LongReference writeCount;
        private final LongReference minRemainingBytesCount;

        /**
         * @param bytesStore the bytes for the header
         * @param start      the location where the header is to be written
         */
        Header(@NotNull BytesStore bytesStore, long start) {
            this.bytesStore = bytesStore;

            // written by reading thread
            readLocationOffsetRef = UncheckedLongReference.create(this.bytesStore, start, Long.BYTES);

            // written by reading thread
            writeUpToRef = UncheckedLongReference.create(this.bytesStore, start + Long.BYTES, Long.BYTES);

            // written by the reading thread
            readCount = UncheckedLongReference.create(this.bytesStore, start + Long.BYTES * 2, Long.BYTES);

            // written by the reading thread
            maxCopyTime = UncheckedLongReference.create(this.bytesStore, start + Long.BYTES * 3, Long.BYTES);

            // written by writing thread.
            writeLocation = UncheckedLongReference.create(this.bytesStore, start + CACHE_LINE_SIZE, Long.BYTES);

            // written by writing thread.
            writeCount = UncheckedLongReference.create(this.bytesStore, start + CACHE_LINE_SIZE + Long.BYTES, Long.BYTES);

            // written by writing thread.
            minRemainingBytesCount = UncheckedLongReference.create(this.bytesStore, start + CACHE_LINE_SIZE + Long.BYTES * 2, Long.BYTES);
        }

        private static long getAndClear(LongReference readCount) {
            for (; ; ) {
                long value = readCount.getVolatileValue();
                if (readCount.compareAndSwapValue(value, 0L))
                    return value;
            }
        }

        private boolean compareAndSetWriteLocation(long expectedValue, long newValue) {
            return writeLocation.compareAndSwapValue(expectedValue, newValue);
        }

        long writeLocation() {
            return writeLocation.getValue();
        }

        long writeLocationVolatile() {
            return writeLocation.getVolatileValue();
        }

        /**
         * @return the point at which you should not writeBytes any additional bits
         */
        long getWriteUpTo() {
            return writeUpToRef.getValue();
        }

        /**
         * sets the point at which you should not writeBytes any additional bits
         */
        private void setWriteUpTo(long value) {
            writeUpToRef.setOrderedValue(value);
        }

        long getWriteUpToVolatile() {
            return writeUpToRef.getVolatileValue();
        }

        void incrementReadCount() {
            readCount.addAtomicValue(1);
        }

        long getAndClearReadCount() {
            return getAndClear(readCount);
        }

        void incrementWriteCount() {
            writeCount.addAtomicValue(1);
        }

        long getAndClearWriteCount() {
            return getAndClear(writeCount);
        }

        private long getReadLocation() {
            return readLocationOffsetRef.getValue();
        }

        private void setReadLocation(long value) {
            readLocationOffsetRef.setOrderedValue(value);
        }

        @Override
        public String toString() {
            return "Header{" +
                    "writeLocation=" + writeLocation() +
                    ", writeUpTo=" + getWriteUpTo() +
                    ", readLocation=" + getReadLocation() + "}";
        }

        public synchronized void clear(long size) {
            writeLocation.setOrderedValue(0);
            setReadLocation(0);
            setWriteUpTo(size);
        }

        public void copyTime(long time) {
            for (; ; ) {
                long copyTime = maxCopyTime.getVolatileValue();
                if (copyTime >= time)
                    return;
                if (maxCopyTime.compareAndSwapValue(copyTime, time))
                    return;
            }
        }

        public long getAndClearMaxCopyTime() {
            return getAndClear(maxCopyTime);
        }

        public void sampleRemainingForWrite(long remainingForWrite) {
            for (; ; ) {
                long value = minRemainingBytesCount.getVolatileValue();
                if (value <= remainingForWrite)
                    return;
                if (minRemainingBytesCount.compareAndSwapValue(value, remainingForWrite))
                    return;
            }
        }

        public long getAndClearMinRemainingBytesCount() {
            for (; ; ) {
                long value = minRemainingBytesCount.getVolatileValue();
                if (minRemainingBytesCount.compareAndSwapValue(value, Long.MAX_VALUE))
                    return value;
            }
        }
    }

    /**
     * This is a Bytes ( like ) implementation where the backing buffer is a ring buffer In the
     * future we could extend this class to implement Bytes.
     */
    private class RingBuffer {

        // if we want multi readers then we could later replace this with a thread-local
        final Bytes bytes;
        private final boolean isBytesBigEndian;
        private final long capacity;
        private final BytesStore byteStore;

        public RingBuffer(BytesStore byteStore, int start, long end) {
            this.byteStore = byteStore;
            this.capacity = end - start;
            bytes = Bytes.allocateDirect(capacity);
            isBytesBigEndian = byteStore.byteOrder() == ByteOrder.BIG_ENDIAN;
        }

        private long write(long offset, @NotNull Bytes bytes0) {
            long result = offset + bytes0.readRemaining();
            offset &= mask;

            long len = bytes0.readRemaining();
            long endOffSet = nextOffset(offset, len);

            if (endOffSet >= offset) {
                this.byteStore.write(offset, bytes0, 0, len);
                return result;
            }

            this.byteStore.write(offset, bytes0, 0, len - endOffSet);
            this.byteStore.write(0, bytes0, len - endOffSet, endOffSet);
            return result;
        }

        long capacity() {
            return capacity;
        }

        long nextOffset(long offset, long increment) {

            long result = offset + increment;
            if (result < capacity())
                return result;

            return result & mask;
        }

        private long write(long offset, long value) {

            offset &= mask;

            if (nextOffset(offset, 8) > offset)
                byteStore.writeLong(offset, value);
            else if (isBytesBigEndian)
                putLongB(offset, value);
            else
                putLongL(offset, value);

            return 8;
        }

        private long writeOrderedLong(long offset, long value) {

            offset &= mask;

            if (nextOffset(offset, 8) > offset)
                byteStore.writeOrderedLong(offset, value);
            else if (isBytesBigEndian)
                putLongB(offset, value);
            else
                putLongL(offset, value);

            return 8;
        }

        public long writeByte(long l, int i) {
            byteStore.writeByte(l & mask, i);
            return l + 1;
        }

        private long read(@NotNull Bytes bytes, long offset, long len) {

            offset &= mask;
            long endOffSet = nextOffset(offset, len);

            // can the data be read straight out of the buffer or is it split around the ring, in
            //other words is that start of the data at the end of the ring and the remaining
            //data at the start of the ring.
            if (endOffSet >= offset) {
                bytes.write(byteStore, offset, len);
                return endOffSet;
            }

            final long firstChunkLen = capacity() - offset;
            bytes.write(byteStore, offset, firstChunkLen);
            bytes.write(byteStore, 0, len - firstChunkLen);
            return endOffSet;
        }

        long read(@NotNull ReadBytesMarshallable readBytesMarshallable,
                  long offset,
                  long len) {

            bytes.clear();

            offset &= mask;
            long endOffSet = nextOffset(offset, len);

            if (endOffSet >= offset) {
                bytes.write(byteStore, offset, len);
                bytes.writeLimit(offset + len);
                readBytesMarshallable.readMarshallable(bytes);
                return endOffSet;
            }

            final long firstChunkLen = capacity() - offset;
            final long l = bytes.writeLimit();

            bytes.writeLimit(bytes.writePosition() + firstChunkLen);
            bytes.write(byteStore, offset, firstChunkLen);
            bytes.writeLimit(l);
            bytes.write(byteStore, 0, len - firstChunkLen);

            readBytesMarshallable.readMarshallable(bytes);
            return endOffSet;
        }

        long readLong(long offset) {

            offset &= mask;
            if (nextOffset(offset, 8) > offset)
                return byteStore.readLong(offset);

            return isBytesBigEndian ? makeLong(byteStore.readByte(offset),
                    byteStore.readByte(offset = nextOffset(offset)),
                    byteStore.readByte(offset = nextOffset(offset)),
                    byteStore.readByte(offset = nextOffset(offset)),
                    byteStore.readByte(offset = nextOffset(offset)),
                    byteStore.readByte(offset = nextOffset(offset)),
                    byteStore.readByte(offset = nextOffset(offset)),
                    byteStore.readByte(nextOffset(offset)))

                    : makeLong(byteStore.readByte(nextOffset(offset, 7L)),
                    byteStore.readByte(nextOffset(offset, 6L)),
                    byteStore.readByte(nextOffset(offset, 5L)),
                    byteStore.readByte(nextOffset(offset, 4L)),
                    byteStore.readByte(nextOffset(offset, 3L)),
                    byteStore.readByte(nextOffset(offset, 2L)),
                    byteStore.readByte(nextOffset(offset)),
                    byteStore.readByte(offset));
        }

        private long makeLong(byte b7, byte b6, byte b5, byte b4,
                              byte b3, byte b2, byte b1, byte b0) {
            return ((((long) b7) << 56) |
                    (((long) b6 & 0xff) << 48) |
                    (((long) b5 & 0xff) << 40) |
                    (((long) b4 & 0xff) << 32) |
                    (((long) b3 & 0xff) << 24) |
                    (((long) b2 & 0xff) << 16) |
                    (((long) b1 & 0xff) << 8) |
                    (((long) b0 & 0xff)));
        }

        long nextOffset(long offset) {
            return nextOffset(offset, 1);
        }

        public byte readByte(long l) {
            return byteStore.readByte(l & mask);
        }

        public byte readVolatileByte(long l) {
            return byteStore.readVolatileByte(l & mask);
        }

        void putLongB(long offset, long value) {
            byteStore.writeByte(offset, (byte) (value >> 56));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 48));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 40));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 32));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 24));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 16));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 8));
            byteStore.writeByte(nextOffset(offset), (byte) (value));
        }

        void putLongL(long offset, long value) {
            byteStore.writeByte(offset, (byte) (value));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 8));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 16));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 24));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 32));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 40));
            byteStore.writeByte(offset = nextOffset(offset), (byte) (value >> 48));
            byteStore.writeByte(nextOffset(offset), (byte) (value >> 56));
        }
    }

}