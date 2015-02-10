package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Multi writer single Reader, zero GC, ring buffer
 *
 * @author Rob Austin.
 */
public class BytesQueue {

    private static final int SIZE = 8;
    private static final int LOCKED = -1;
    private static final int FLAG = 1;

    private final BytesRingBuffer bytes;
    private final Header header;

    /**
     * @param buffer the bytes that you wish to use for the ring buffer
     */
    public BytesQueue(@NotNull final Bytes buffer) {
        this.header = new Header(buffer);
        this.bytes = new BytesRingBuffer(buffer);
        header.setWriteUpTo(bytes.capacity());
    }


    private enum States {BUSY, READY, USED}

    /**
     * Inserts the specified element at the tail of this queue if it is possible to do so
     * immediately without exceeding the queue's capacity,
     *
     * @param bytes the {@code bytes} that you wish to add to the ring buffer
     * @return returning {@code true} upon success and {@code false} if this queue is full.
     */
    public boolean offer(@NotNull Bytes bytes) throws InterruptedException {

        try {

            for (; ; ) {

                long writeLocation = this.writeLocation();

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
     * Retrieves and removes the head of this queue, or returns {@code null} if this queue is
     * empty.
     *
     * @return {@code null} if this queue is empty, or a populated buffer if the element was retried
     * @throws IllegalStateException is the {@code using} buffer is not large enough
     */
    @Nullable
    public Bytes poll(@NotNull Bytes using) throws InterruptedException, IllegalStateException {

        long writeLoc = writeLocation();

        long offset = header.getReadLocation();
        long readLocation = offset;//= this.readLocation.get();

        if (readLocation >= writeLoc) {
            return null;
        }

        assert readLocation <= writeLoc : "reader has go ahead of the writer";

        long flag = offset;

        byte state = bytes.readByte(flag);
        offset += 1;

        // the element is currently being written to, so let wait for the write to finish
        if (state == States.BUSY.ordinal()) return null;

        assert state == States.READY.ordinal() : " we are reading a message that we " +
                "shouldn't,  state=" + state;

        long elementSize = bytes.readLong(offset);
        offset += 8;

        long next = offset + elementSize;

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


    private static void checkSize(@NotNull Bytes using, long elementSize) {
        if (using.remaining() < elementSize)
            throw new IllegalStateException("requires size=" + elementSize +
                    " bytes, but only " + using.remaining() + " remaining.");
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


    /**
     * used to store the locations around the ring buffer or reading and writing
     */
    private class Header {

        private final long writeLocationOffset;
        private final long writeUpToOffset;
        private final long readLocationOffset;
        private final Bytes buffer;

        /**
         * @param buffer the bytes for the header
         */
        private Header(@NotNull Bytes buffer) {

            if (buffer.remaining() < 24) {
                final String message = "buffer too small, buffer size=" + buffer.remaining();
                throw new IllegalStateException(message);
            }

            long start = buffer.position();
            readLocationOffset = buffer.position();
            buffer.skip(8);


            writeLocationOffset = buffer.position();
            buffer.skip(8);

            writeUpToOffset = buffer.position();
            buffer.skip(8);

            this.buffer = buffer.bytes(start, buffer.position());

        }

        // has to be synchronized because the compareAndSwapLong is not writeOrdered
        private synchronized boolean compareAndSetWriteLocation(long expectedValue, long newValue) {
            return buffer.compareAndSwapLong(writeLocationOffset, expectedValue, newValue);
        }

        // has to be synchronized because the compareAndSwapLong is not writeOrdered
        private synchronized long getWriteLocation() {
            return buffer.readVolatileLong(writeLocationOffset);
        }

        /**
         * sets the point at which you should not write any additional bits
         */
        private void setWriteUpTo(long value) {
            buffer.writeOrderedLong(writeUpToOffset, value);
        }

        /**
         * @return the point at which you should not write any additional bits
         */
        private long getWriteUpTo() {
            return buffer.readVolatileLong(writeUpToOffset);
        }

        private void setReadLocation(long value) {
            buffer.writeOrderedLong(readLocationOffset, value);
        }

        private long getReadLocation() {
            return buffer.readVolatileLong(readLocationOffset);
        }
    }

    /**
     * This is a Bytes ( like ) implementation where the backing buffer is a ring buffer In the
     * future we could extend this class to implement Bytes.
     */
    private class BytesRingBuffer {

        @NotNull
        final Bytes buffer;
        final boolean isBytesBigEndian;

        public BytesRingBuffer(@NotNull Bytes buffer) {
            this.buffer = buffer.bytes(buffer.position(), buffer.remaining());
            isBytesBigEndian = isBytesBigEndian();
        }


        private long write(long offset, @NotNull Bytes bytes) {

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


        private long read(@NotNull Bytes bytes, long offset) {
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

        public byte readByte(long l) {
            return buffer.readByte(l % capacity());
        }

        long nextOffset(long offset, long increment) {

            long result = offset + increment;
            if (result < capacity())
                return result;

            return result % capacity();
        }

        long nextOffset(long offset) {
            return nextOffset(offset, 1);
        }

        long capacity() {
            return buffer.capacity();
        }

        boolean isBytesBigEndian() {
            try {
                putLongB(0, 1);
                return buffer.flip().readLong() == 1;
            } finally {
                buffer.clear();
            }
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
