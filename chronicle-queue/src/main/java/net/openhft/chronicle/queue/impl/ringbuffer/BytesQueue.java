package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rob Austin.
 */
public class BytesQueue {

    private static final Logger LOG = LoggerFactory.getLogger(BytesQueue.class.getName());

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
     *
     * @InterruptedException if interrupted
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
                if (queue.remainingForWrite(writeLocation) < bytes.remaining() + SIZE_OF_SIZE)
                    return false;

                if (!queue.writeLocation.compareAndSet(writeLocation, -1))
                    continue;


                queue.readupto.set(writeLocation);

                System.out.println("(write) element size location =" + writeLocation + "size =" + bytes.remaining());
                // write the size
                queue.write(writeLocation, bytes.remaining());

                long offset = writeLocation + 8;
                writeLocation = offset + bytes.remaining();
                queue.write(bytes, offset);


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
            queue.readupto.set(writeLocation - 1);
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
            if (offset == -1)
                continue;

            if (queue.remainingForRead(offset) == 0)
                return null;

            if (!queue.readLocation.compareAndSet(offset, -1))
                continue;


            System.out.println("(read) element size location =" + offset);
            long elementSize = queue.readLong(offset);


            long nextElement = offset + 8 + elementSize;
            queue.readLocation.set(nextElement);

            // checks that the 'using' bytes is large enough
            checkSize(using, elementSize);


            using.limit(using.position() + elementSize);

            queue.read(using, offset + 8);
            queue.writeupto.set((nextElement - 1) + queue.capacity());


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
        final AtomicLong writeupto;

        private boolean isBytesBigEndian;

        @NotNull
        private final Bytes buffer;


        public VanillaByteQueue(@NotNull Bytes buffer) {
            this.buffer = buffer;
            isBytesBigEndian = isBytesBigEndian();
            writeupto = new AtomicLong(capacity());
        }

        private long write(@NotNull Bytes bytes, long offset) {

            offset %= capacity();
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

            this.buffer.write(0, bytes);
            return endOffSet;

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


        boolean isBytesBigEndian() {
            try {
                putLongB(0, 1);
                return buffer.flip().readLong() == 1;
            } finally {
                buffer.clear();
            }
        }


        private void write(long offset, long value) {

            offset %= capacity();

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


        long nextOffset(long offset) {
            return nextOffset(offset, 1);
        }

        long nextOffset(long offset, long increment) {

            long result = offset + increment;
            if (result < capacity())
                return result;

            return result % capacity();

        }


        long remainingForRead(long offset) {
            //
            return readupto.get() - offset;

        }


        long remainingForWrite(long offset) {
            return writeupto.get() - offset;

        }


    }
}
