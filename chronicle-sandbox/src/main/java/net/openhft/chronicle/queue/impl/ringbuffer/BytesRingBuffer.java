/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
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

    @NotNull
    private final RingBuffer bytes;
    @NotNull
    private final Header header;

    /**
     * @param buffer the bytes that you wish to use for the ring buffer
     */
    public BytesRingBuffer(@NotNull final Bytes<?> buffer) {
        this.header = new Header(buffer);
        this.bytes = new RingBuffer(buffer);
        header.setWriteUpTo(bytes.capacity());
    }

    private static void checkSize(@NotNull Bytes<?> using, long elementSize) {
        if (using.remaining() < elementSize)
            throw new IllegalStateException("requires size=" + elementSize +
                    " bytes, but only " + using.remaining() + " remaining.");
    }

    /**
     * Inserts the specified element at the tail of this queue if it is possible to do so
     * immediately without exceeding the queue's capacity,
     *
     * @param bytes the {@code bytes} that you wish to add to the ring buffer
     * @return returning {@code true} upon success and {@code false} if this queue is full.
     */
    public boolean offer(@NotNull Bytes<?> bytes) throws InterruptedException {

        try {

            for (; ; ) {
                long writeLocation = this.writeLocation();

                assert writeLocation >= 0;

                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                // if reading is occurring the remain capacity will only get larger, as have locked
                if (remainingForWrite(writeLocation) < bytes.remaining() + SIZE + FLAG)
                    return false;

                // write the size
                long len = bytes.remaining();

                long messageLen = SIZE + FLAG + len;

                long offset = writeLocation;

                // we want to ensure that only one thread ever gets in here at a time
                if (!header.compareAndSetWriteLocation(writeLocation, LOCKED))
                    continue;

                // we have to set the busy fag before the write location for the reader
                // this is why we have the compareAndSet above to ensure that only one thread
                // gets in here
                long flagLoc = offset;
                offset = this.bytes.writeByte(offset, States.BUSY.ordinal());

                if (!header.compareAndSetWriteLocation(-1, writeLocation + messageLen))
                    continue;

                // write a size
                offset = this.bytes.write(offset, len);

                // write the data
                this.bytes.write(offset, bytes);
                this.bytes.writeByte(flagLoc, States.READY.ordinal());

                return true;
            }
        } catch (IllegalStateException e) {
            // when the ring buffer is full
            return false;
        }
    }

    /**
     * @return spin loops to get a valid write location
     */
    private long writeLocation() {
        long writeLocation;
        for (; ; ) {
            if ((writeLocation = header.getWriteLocation()) != LOCKED)
                return writeLocation;
        }
    }

    private long remainingForWrite(long offset) {
        return (header.getWriteUpTo() - 1) - offset;
    }

    @NotNull
    public Bytes<?> take(@NotNull BytesProvider bytesProvider) throws
            InterruptedException,
            IllegalStateException {
        Bytes<?> poll;
        do {
            poll = poll(bytesProvider);
        } while (poll == null);
        return poll;
    }

    /**
     * Retrieves and removes the head of this queue, or returns {@code null} if this queue is
     * empty.
     *
     * @return {@code null} if this queue is empty, or a populated buffer if the element was retried
     * @throws IllegalStateException is the {@code using} buffer is not large enough
     */
    @Nullable
    public Bytes<?> poll(@NotNull BytesProvider bytesProvider) throws
            InterruptedException,
            IllegalStateException {

        long writeLoc = writeLocation();

        long offset = header.getReadLocation();
        long readLocation = offset;//= this.readLocation.get();

        if (readLocation >= writeLoc) {
            return null;
        }

        assert readLocation <= writeLoc : "reader has go ahead of the writer";

        long flag = offset;

        final byte state = bytes.readByte(flag);
        offset += 1;

        // the element is currently being written to, so let wait for the write to finish
        if (state == States.BUSY.ordinal()) return null;

        assert state == States.READY.ordinal() : " we are reading a message that we " +
                "shouldn't,  state=" + state;

        final long elementSize = bytes.readLong(offset);
        offset += 8;

        final long next = offset + elementSize;

        final Bytes<?> using = bytesProvider.provide(elementSize);

        // checks that the 'using' bytes is large enough
        checkSize(using, elementSize);

        using.limit(using.position() + elementSize);

        bytes.read(using, offset);
        bytes.write(flag, States.USED.ordinal());

        header.setWriteUpTo(next + bytes.capacity());
        header.setReadLocation(next);

        using.position(using.position());
        return using;
    }

    private enum States {BUSY, READY, USED}

    public interface BytesProvider {

        /**
         * sets up a buffer to back the ring buffer, the data will be read into this buffer the size of the buffer must
         * be as big as {@code maxSize}
         *
         * @param maxSize the number of bytes required
         * @return a buffer of at least {@code maxSize} bytes remaining
         */
        @NotNull
        Bytes<?> provide(long maxSize);
    }

    /**
     * used to store the locations around the ring buffer or reading and writing
     */
    private class Header {

        private final long writeLocationOffset;
        private final long writeUpToOffset;
        private final long readLocationOffset;
        private final Bytes<?> buffer;

        /**
         * @param buffer the bytes for the header
         */
        private Header(@NotNull Bytes<?> buffer) {
            if (buffer.remaining() < 24) {
                final String message = "buffer too small, buffer size=" + buffer.remaining();
                throw new IllegalStateException(message);
            }

            readLocationOffset = buffer.position();
            buffer.skip(8);

            writeLocationOffset = buffer.position();
            buffer.skip(8);

            writeUpToOffset = buffer.position();
            buffer.skip(8);

            this.buffer = buffer.bytes();
        }

       // final AtomicLong writeLocationAtomic = new AtomicLong();
       // final AtomicLong readLocationAtomic = new AtomicLong();
       // final AtomicLong writeUpToOffsetAtomic = new AtomicLong();

        boolean compareAndSetWriteLocation(long expectedValue, long newValue) {
            //return writeLocationAtomic.compareAndSet(expectedValue, newValue);
            // todo replace the above with this :   
            return buffer.compareAndSwapLong(writeLocationOffset, expectedValue, newValue);
        }

        long getWriteLocation() {
            //return writeLocationAtomic.get();
            // todo replace the above with this :   
            return buffer.readVolatileLong(writeLocationOffset);
        }

        /**
         * @return the point at which you should not write any additional bits
         */
        long getWriteUpTo() {
            //return writeUpToOffsetAtomic.get();
            // todo replace the above with this :  
            return buffer.readVolatileLong(writeUpToOffset);
        }

        /**
         * sets the point at which you should not write any additional bits
         */
        void setWriteUpTo(long value) {
            //writeUpToOffsetAtomic.set(value);
            // todo replace the above with this : 
            buffer.writeOrderedLong(writeUpToOffset, value);
        }

        long getReadLocation() {
            //return readLocationAtomic.get();
            // todo replace the above with this :    
            return buffer.readVolatileLong(readLocationOffset);
        }

        void setReadLocation(long value) {
            //readLocationAtomic.set(value);
            // todo replace the above with this : 
            buffer.writeOrderedLong(readLocationOffset, value);
        }
    }

    /**
     * This is a Bytes ( like ) implementation where the backing buffer is a ring buffer. In the
     * future we could extend this class to implement Bytes.
     */
    private class RingBuffer {

        @NotNull
        final Bytes<?> buffer;
        final boolean isBytesBigEndian;

        public RingBuffer(@NotNull Bytes<?> buffer) {
            this.buffer = buffer.bytes();
            isBytesBigEndian = buffer.byteOrder() == ByteOrder.BIG_ENDIAN;
        }

        private long write(long offset, @NotNull Bytes<?> bytes) {
            long result = offset + bytes.remaining();
            offset %= capacity();

            long len = bytes.remaining();
            long endOffSet = nextOffset(offset, len);

            if (endOffSet >= offset) {
                this.buffer.write(offset, bytes);
                return result;
            }

            long limit = bytes.limit();

            bytes.limit(capacity() - offset);
            this.buffer.write(offset, bytes);

            bytes.position(bytes.limit());
            bytes.limit(limit);

            this.buffer.write(0, bytes);
            return result;
        }

        long capacity() {
            return buffer.capacity();
        }

        long nextOffset(long offset, long increment) {
            long result = offset + increment;
            if (result < capacity())
                return result;

            return result % capacity();
        }

        private long write(long offset, long value) {
            long result = offset + 8;
            offset %= capacity();

            if (nextOffset(offset, 8) > offset)
                buffer.writeLong(offset, value);
            else if (isBytesBigEndian)
                putLongB(offset, value);
            else
                putLongL(offset, value);

            return result;
        }

        public long writeByte(long l, int i) {
            buffer.writeByte(l % capacity(), i);
            return l + 1;
        }

        private long read(@NotNull Bytes<?> bytes, long offset) {
            offset %= capacity();
            long endOffSet = nextOffset(offset, bytes.remaining());

            if (endOffSet >= offset) {
                bytes.write(buffer, offset, bytes.remaining());
                bytes.flip();
                return endOffSet;
            }

            bytes.write(buffer, offset, capacity() - offset);
            bytes.write(buffer, 0, bytes.remaining());
            bytes.flip();

            return endOffSet;
        }

        long readLong(long offset) {
            offset %= capacity();
            if (nextOffset(offset, 8) > offset)
                return buffer.readLong(offset);

            return isBytesBigEndian ? makeLong(buffer.readByte(offset),
                    buffer.readByte(offset = nextOffset(offset)),
                    buffer.readByte(offset = nextOffset(offset)),
                    buffer.readByte(offset = nextOffset(offset)),
                    buffer.readByte(offset = nextOffset(offset)),
                    buffer.readByte(offset = nextOffset(offset)),
                    buffer.readByte(offset = nextOffset(offset)),
                    buffer.readByte(nextOffset(offset)))

                    : makeLong(buffer.readByte(nextOffset(offset, 7L)),
                    buffer.readByte(nextOffset(offset, 6L)),
                    buffer.readByte(nextOffset(offset, 5L)),
                    buffer.readByte(nextOffset(offset, 4L)),
                    buffer.readByte(nextOffset(offset, 3L)),
                    buffer.readByte(nextOffset(offset, 2L)),
                    buffer.readByte(nextOffset(offset)),
                    buffer.readByte(offset));
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
            return buffer.readByte(l % capacity());
        }

        void putLongB(long offset, long value) {
            buffer.writeByte(offset, (byte) (value >> 56));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 48));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 40));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 32));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 24));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 16));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 8));
            buffer.writeByte(nextOffset(offset), (byte) (value));
        }

        void putLongL(long offset, long value) {
            buffer.writeByte(offset, (byte) (value));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 8));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 16));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 24));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 32));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 40));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 48));
            buffer.writeByte(nextOffset(offset), (byte) (value >> 56));
        }
    }
}
