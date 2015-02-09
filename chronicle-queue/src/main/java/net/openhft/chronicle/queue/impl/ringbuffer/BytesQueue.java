package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rob Austin.
 */
public class BytesQueue {

    private static final int SIZE_OF_SIZE = 8;

    @NotNull
    private final VanillaByteQueue queue;

    public BytesQueue(@NotNull final Bytes buffer) {
        queue = new VanillaByteQueue(buffer);
    }

    /**
     * Inserts the specified element at the tail of this queue if it is possible to do so
     * immediately without exceeding the queue's capacity, returning {@code true} upon success and
     * {@code false} if this queue is full.
     */
    public boolean offer(@NotNull Bytes bytes) throws InterruptedException {
        long writeLocation = Integer.MAX_VALUE;

        try {

            for (; ; ) {

                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                writeLocation = queue.writeLocation.get();

                if (writeLocation == -1)
                    continue;

                // if reading is occurring the remain capacity will only get larger, as have locked
                if (queue.remainingCapacity() < bytes.remaining() + SIZE_OF_SIZE)
                    return false;

                if (!queue.writeLocation.compareAndSet(writeLocation, -1))
                    continue;

                queue.readupto.set(writeLocation);
                queue.write(writeLocation, bytes.remaining());

                long offset = queue.nextOffset(writeLocation, 8);

                offset = queue.write(bytes, offset);

                writeLocation = offset;
                return true;

            }
        } catch (IllegalStateException e) {
            // when the queue is full
            return false;
        } finally {
            if (writeLocation == Integer.MIN_VALUE)
                throw new IllegalStateException();

            assert writeLocation != -1;
            // writeLocation =  queue.nextWrite(writeLocation);
            queue.readupto.set(writeLocation);
            queue.writeLocation.set(writeLocation);
        }
    }


    /**
     * Retrieves and removes the head of this queue, or returns {@code null} if this queue is
     * empty.
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     * @throws IllegalStateException is the {@code using} buffer is not large enough
     */
    @Nullable
    public Bytes poll(@NotNull Bytes using) throws InterruptedException, IllegalStateException {

        for (; ; ) {

            long offset = queue.readLocation.get();
            assert queue.writeLocation.get() != -1;

            if (offset == -1)
                continue;

            if (!queue.readLocation.compareAndSet(offset, -1))
                continue;

            long elementSize = queue.readLong(offset);

            // checks that the 'using' bytes is large enough
            checkSize(using, elementSize);

            offset = queue.nextOffset(offset, 8);
            assert offset < queue.capacity();

            using.limit(using.position() + elementSize);

            offset = queue.read(using, offset);
            queue.writeupto.set(offset);
            queue.readLocation.set(offset);

            return using;
        }

    }



    private static void checkSize(@NotNull Bytes using, long elementSize) {
        if (using.remaining() < elementSize)
            throw new IllegalStateException("requires size=" + elementSize + " " +
                    "bytes, but " +
                    "only " + using.remaining() + " remaining.");
    }


    private static class VanillaByteQueue {


        final AtomicLong readLocation = new AtomicLong();
        final AtomicLong writeLocation = new AtomicLong();

        final AtomicLong readupto = new AtomicLong();
        final AtomicLong writeupto = new AtomicLong();

        private boolean isBytesBigEndian;

        @NotNull
        private final Bytes buffer;


        public VanillaByteQueue(@NotNull Bytes buffer) {
            this.buffer = buffer;
            isBytesBigEndian = isBytesBigEndian();
        }

        private long write(@NotNull Bytes bytes, long offset) {

            long endOffSet = nextOffset(offset, bytes.remaining());

            if (endOffSet >= offset) {
                this.buffer.write(offset, bytes);
                return endOffSet;
            }

            long limit = bytes.limit();

            bytes.limit(capacity() - offset);
            this.buffer.write(offset, bytes);

            bytes.position(bytes.limit());
            bytes.limit(limit);

            this.buffer.write(1, bytes);
            return endOffSet;

        }

        private long read(@NotNull Bytes bytes, long offset) {

            long endOffSet = nextOffset(offset, bytes.remaining());

            if (endOffSet >= offset) {
                bytes.write(buffer, offset, bytes.remaining());
                bytes.flip();
                return endOffSet;
            }

            bytes.write(buffer, offset, capacity() - offset);
            bytes.write(buffer, 1, bytes.remaining());
            bytes.flip();

            return endOffSet;

        }


        boolean isBytesBigEndian() {
            try {
                putLongB(0, 1);
                return buffer.flip().readLong() == 1;
            } finally {
                buffer.clear();
            }
        }


        private void write(long offset, long value) {

            if (nextOffset(offset, 8) > offset)
                buffer.writeLong(offset, value);
            else if (isBytesBigEndian)
                putLongB(offset, value);
            else
                putLongL(offset, value);
        }


        static private long makeLong(byte b7, byte b6, byte b5, byte b4,
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

        private long readLong(long offset) {

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


        private void putLongB(long offset, long value) {
            buffer.writeByte(offset, (byte) (value >> 56));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 48));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 40));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 32));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 24));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 16));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 8));
            buffer.writeByte(nextOffset(offset), (byte) (value));

        }

        private void putLongL(long offset, long value) {
            buffer.writeByte(offset, (byte) (value));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 8));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 16));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 24));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 32));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 40));
            buffer.writeByte(offset = nextOffset(offset), (byte) (value >> 48));
            buffer.writeByte(nextOffset(offset), (byte) (value >> 56));

        }

        private byte read(long offset) {
            return buffer.readByte(offset);
        }

        private long capacity() {
            return buffer.capacity();
        }

        /**
         * This method is not thread safe it therefore only provides and approximation of the size,
         * the size will be corrected if nothing was added or removed from the queue at the time it
         * was called
         *
         * @return an approximation of the size
         */
        long size() {
            long writeUpTo = writeupto.get();
            long readUpTo = readupto.get();

            if (readUpTo < writeUpTo)
                readUpTo += capacity();

            return readUpTo - writeUpTo;
        }


        /**
         * This method does not lock, it therefore only provides and approximation of isEmpty(), it
         * will be correct, if nothing was added or removed from the queue at the time it was
         * called.
         *
         * @return an approximation of isEmpty()
         */
        boolean isEmpty() {
            return size() == 0;
        }


        long blockForReadSpace(long readOffset) {

            // in the for loop below, we are blocked reading unit another item is written, this is because we are empty ( aka size()=0)
            // inside the for loop, getting the 'writeLocation', this will serve as our read memory barrier.
            while (readupto.get() == readOffset)
                return -1;

            return nextOffset(readOffset);
        }


        long nextOffset(long offset) {
            return nextOffset(offset, 1);
        }

        long nextOffset(long offset, long increment) {

            long result = offset + increment;
            if (result < capacity())
                return result;

            return result - (capacity() - 1);

        }


        /**
         * Returns the number of additional elements that this queue can ideally (in the absence of
         * memory or resource constraints) accept without blocking, or <tt>Integer.MAX_VALUE</tt> if
         * there is no intrinsic limit. <p> <p>Note that you <em>cannot</em> always tell if an
         * attempt to insert an element will succeed by inspecting <tt>remainingCapacity</tt>
         * because it may be the case that another thread is about to insert or remove an element.
         *
         * @return the remaining capacity
         */
        long remainingCapacity() {
            return (capacity() - 1) - size();
        }

    }
}
