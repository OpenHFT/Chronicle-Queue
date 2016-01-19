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
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.wire.BinaryLongReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private final long capacity;

    private long minRemainingForWriteSinceLastPoll = Integer.MAX_VALUE;

    @NotNull
    private final RingBuffer bytes;
    @NotNull
    private final Header header;

    public BytesRingBuffer(@NotNull final BytesStore byteStore) {
        capacity = byteStore.writeLimit() - Header.size();

        if (byteStore.writeRemaining() <= Header.size()) {
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

    public void clear() {
        header.clear(capacity);
    }

    /**
     * each time the ring is read, this logs the number of bytes in the write buffer, calling this
     * method resets these statistics,
     *
     * @return -1 if no read calls were made since the last time this method was called.
     */
    public long minNumberOfWriteBytesRemainingSinceLastCall() {
        long result = minRemainingForWriteSinceLastPoll;
        minRemainingForWriteSinceLastPoll = Integer.MAX_VALUE;
        return result == Integer.MAX_VALUE ? -1 : result;
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
                final long remainingForWrite = remainingForWrite(writeLocation);

                this.minRemainingForWriteSinceLastPoll = Math.min
                        (minRemainingForWriteSinceLastPoll, remainingForWrite);

                if (remainingForWrite < bytes0.readRemaining() + SIZE + FLAG)
                    return false;

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
            if ((writeLocation = header.getWriteLocation()) != LOCKED)
                return writeLocation;
        }
    }

    private long remainingForWrite(long offset) {
        final long writeUpTo = header.getWriteUpTo();

        return (writeUpTo - 1) - offset;
    }

    /**
     * @param bytesProvider provides a bytes to read into
     * @return the Bytes written to
     * @throws InterruptedException
     * @throws IllegalStateException if the Bytes provided by the BytesProvider are not large
     *                               enough
     */
    @NotNull
    public Bytes take(@NotNull BytesProvider bytesProvider) throws
            InterruptedException,
            IllegalStateException {
        Bytes poll;
        do {
            poll = poll(bytesProvider);
        } while (poll == null);
        return poll;
    }

    /**
     * they similar to net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer#take(net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer.BytesProvider)
     *
     * @param readBytesMarshallable used to read the bytes
     * @return the number of bytes read, if ZERO the read was blocked so you should call this method
     * again if you want to read some data.
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    public long read(@NotNull ReadBytesMarshallable readBytesMarshallable) throws
            InterruptedException {

        long writeLoc = writeLocation();
        long offset = header.getReadLocation();
        long readLocation = offset;

        if (readLocation >= writeLoc)
            return 0;

        assert readLocation <= writeLoc : "reader has go ahead of the writer";

        long flag = offset;

        final byte state = bytes.readByte(flag);
        offset += 1;

        // the element is currently being written to, so let wait for the writeBytes to finish
        if (state == States.BUSY.ordinal()) return 0;

        assert state == States.READY.ordinal() : " we are reading a message that we " +
                "shouldn't,  state=" + state + ", flag-location=" + flag + ", remainingForWrite=" +
                remainingForWrite(header.getWriteLocation());

        final long elementSize = bytes.readLong(offset);
        offset += 8;

        final long next = offset + elementSize;

        bytes.read(readBytesMarshallable, offset, elementSize);
        bytes.write(flag, States.USED.ordinal());

        header.setWriteUpTo(next + bytes.capacity());
        header.setReadLocation(next);

        return 8;
    }

    /**
     * Retrieves and removes the head of this queue, or returns {@code null} if this queue is
     * empty.
     *
     * @return {@code null} if this queue is empty, or a populated buffer if the element was retried
     * @throws IllegalStateException is the {@code using} buffer is not large enough
     */
    @Nullable
    public Bytes poll(@NotNull BytesProvider bytesProvider) throws
            InterruptedException,
            IllegalStateException {

        long writeLoc = writeLocation();
        long offset = header.getReadLocation();
        long readLocation = offset;//= this.readLocation.get();

        if (readLocation >= writeLoc)
            return null;

        assert readLocation <= writeLoc : "reader has go ahead of the writer";

        long flag = offset;

        final byte state = bytes.readVolatileByte(flag);
        offset += 1;

        // the element is currently being written to, so let wait for the writeBytes to finish
        if (state == States.BUSY.ordinal()) return null;

        assert state == States.READY.ordinal() : " we are reading a message that we " +
                "shouldn't,  state=" + state;

        final long elementSize = bytes.readLong(offset);
        offset += 8;

        final long next = offset + elementSize;
        final Bytes using = bytesProvider.provide(elementSize);

        // checks that the 'using' bytes is large enough
        checkSize(using, elementSize);

        bytes.read(using, offset, elementSize);
        bytes.writeOrderedLong(flag, States.USED.ordinal());

        header.setWriteUpTo(next + bytes.capacity());
        header.setReadLocation(next);

        return using;
    }

    private enum States {BUSY, READY, USED}

    public interface BytesProvider {

        /**
         * sets up a buffer to back the ring buffer, the data wil be read into this buffer the size
         * of the buffer must be as big as {@code maxSize}
         *
         * @param maxSize the number of bytes required
         * @return a buffer of at least {@code maxSize} bytes remaining
         */
        @NotNull
        Bytes provide(long maxSize);
    }

    /**
     * used to store the locations around the ring buffer or reading and writing
     */
    private static class Header {

        public static final int PAGE_ALIGN_SIZE = 64;
        private final BytesStore bytesStore;
        // these fields are written using put ordered long ( so don't have to be volatile )
        private BinaryLongReference readLocationOffsetRef;
        private BinaryLongReference writeUpToRef;
        private BinaryLongReference writeLocation;

        /**
         * @param bytesStore the bytes for the header
         * @param start      the location where the header is to be written
         */
        private Header(@NotNull BytesStore bytesStore, long start) {
            this.bytesStore = bytesStore;

            readLocationOffsetRef = new BinaryLongReference();
            readLocationOffsetRef.bytesStore(this.bytesStore, start, 8);

            writeUpToRef = new BinaryLongReference();
            writeUpToRef.bytesStore(this.bytesStore, start += PAGE_ALIGN_SIZE, 8);

            writeLocation = new BinaryLongReference();
            writeLocation.bytesStore(this.bytesStore, start += PAGE_ALIGN_SIZE, 8);

            OS.pageAlign(PAGE_ALIGN_SIZE);
        }

        public static int size() {
            return PAGE_ALIGN_SIZE * 3;
        }


        private boolean compareAndSetWriteLocation(long expectedValue, long newValue) {
            return writeLocation.compareAndSwapValue(expectedValue, newValue);
        }

        private long getWriteLocation() {
            return writeLocation.getValue();
        }

        /**
         * @return the point at which you should not writeBytes any additional bits
         */
        private long getWriteUpTo() {
            return writeUpToRef.getValue();
        }

        /**
         * sets the point at which you should not writeBytes any additional bits
         */
        private void setWriteUpTo(long value) {
            writeUpToRef.setOrderedValue(value);
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
                    "writeLocation=" + getWriteLocation() +
                    ", writeUpTo=" + getWriteUpTo() +
                    ", readLocation=" + getReadLocation() + "}";
        }

        public synchronized void clear(long size) {

            writeLocation.setOrderedValue(0);
            setReadLocation(0);
            setWriteUpTo(size);
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
            offset %= capacity();

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

            return result % capacity();
        }

        private long write(long offset, long value) {

            offset %= capacity();

            if (nextOffset(offset, 8) > offset)
                byteStore.writeLong(offset, value);
            else if (isBytesBigEndian)
                putLongB(offset, value);
            else
                putLongL(offset, value);

            return 8;
        }

        private long writeOrderedLong(long offset, long value) {

            offset %= capacity();

            if (nextOffset(offset, 8) > offset)
                byteStore.writeOrderedLong(offset, value);
            else if (isBytesBigEndian)
                putLongB(offset, value);
            else
                putLongL(offset, value);

            return 8;
        }

        public long writeByte(long l, int i) {
            byteStore.writeByte(l % capacity(), i);
            return l + 1;
        }

        private long read(@NotNull Bytes bytes, long offset, long len) {

            offset %= capacity();
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

            offset %= capacity();
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

            offset %= capacity();
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
            return byteStore.readByte(l % capacity);
        }

        public byte readVolatileByte(long l) {
            return byteStore.readVolatileByte(l % capacity);
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