package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.IByteBufferBytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rob Austin.
 */
public class BytesQueue {

    private static final int SIZE_OF_SIZE = 8;

    private final VanillaByteQueue queue;

    public BytesQueue(int size) {
        queue = new VanillaByteQueue(size);
    }

    /**
     * Inserts the specified element at the tail of this queue if it is possible to do so
     * immediately without exceeding the queue's capacity, returning {@code true} upon success and
     * {@code false} if this queue is full.
     */
    public boolean offer(@NotNull Bytes bytes) throws InterruptedException {
        long writeLocation = Integer.MAX_VALUE;

        try {

            IByteBufferBytes size = ByteBufferBytes.wrap(ByteBuffer.wrap(new byte[8]));

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

                size.clear().writeLong(bytes.remaining());
                size.clear();

                long nextWriteLocation = writeLocation;

                for (; size.remaining() > 0; nextWriteLocation =
                        queue.nextWrite(nextWriteLocation)) {
                    assert nextWriteLocation != -1;
                    queue.write(nextWriteLocation, size.readByte());
                }

                for (; bytes.remaining() > 0; nextWriteLocation =
                        queue.nextWrite(nextWriteLocation)) {
                    assert nextWriteLocation != -1;
                    queue.write(nextWriteLocation, bytes.readByte());
                }

                writeLocation = nextWriteLocation;
                return true;
            }
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
    public Bytes poll(Bytes using) throws InterruptedException, IllegalStateException {

        for (; ; ) {

            long readLocation = queue.readLocation.get();
            assert queue.writeLocation.get() != -1;
            if (readLocation == -1)
                continue;

            if (!queue.readLocation.compareAndSet(readLocation, -1))
                continue;


            long elementSize = sizeOfNextElement(using, readLocation);
            if (elementSize == -1) {
                using.position(0);
                using.limit(0);
                return null;
            }

            // checks that the 'using' bytes is large enough
            checkSize(using, elementSize);

            readLocation = queue.nextlocation(readLocation, 8);
            assert readLocation < queue.capacity();
            for (int i = 0; i < elementSize; readLocation = queue.blockForReadSpace(readLocation), i++) {

                if (readLocation == -1) {
                    using.position(0);
                    using.limit(0);
                    return null;
                }

                byte b = queue.read(readLocation);

                using.write(b);
            }

            queue.writeupto.set(readLocation);
            queue.readLocation.set(readLocation);

            return using.flip();
        }

    }

    private static void checkSize(Bytes using, long elementSize) {
        if (using.remaining() < elementSize)
            throw new IllegalStateException("requires size=" + elementSize + " " +
                    "bytes, but " +
                    "only " + using.remaining() + " remaining.");
    }


    /**
     * reads the size of the next element by does not move the position on
     *
     * @param using
     * @param readLocation
     * @return -1 if the size can not be read
     * @throws InterruptedException
     */
    public long sizeOfNextElement(final Bytes using, long readLocation) throws
            InterruptedException {

        long position = using.position();
        long limit = using.limit();

        // rather than creating a new bytes we can the bytes passed in to read the size into
        Bytes size = (using.remaining() < 8) ? ByteBufferBytes.wrap(ByteBuffer.wrap(new
                byte[8])) : using;

        size.limit(position + 8);


        // block until we can read the size of the element, currently there not enough data in
        // the queue to even  read the size of the element
        if (queue.size() < 8) {
            return -1;
        }

        for (; size.remaining() > 0; readLocation = queue.nextlocation(readLocation)) {
            size.write(queue.read(readLocation));
        }

        size.position(position);

        long result = size.readLong();

        using.position(position);
        using.limit(limit);

        return result;
    }

    private class VanillaByteQueue {

        final AtomicLong readLocation = new AtomicLong();
        final AtomicLong writeLocation = new AtomicLong();

        final AtomicLong readupto = new AtomicLong();
        final AtomicLong writeupto = new AtomicLong();

        private final Bytes bytes;

        /**
         * @param size Creates an BlockingQueue with the given (fixed) capacity
         */
        public VanillaByteQueue(int size) {
            bytes = ByteBufferBytes.wrap(ByteBuffer.allocate(size + 1));
        }

        private void write(long offset, byte value) {
            bytes.writeByte(offset, value);
        }

        private byte read(long offset) {
            return bytes.readByte(offset);
        }

        private long capacity() {
            return bytes.capacity();
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

        void clear() {
            readLocation.set(writeLocation.get());
            readupto.set(writeLocation.get());
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

        /**
         * @param writeLocation the current write location
         * @return the next write location
         */
        long nextWrite(long writeLocation) throws IllegalStateException {

            if (remainingCapacity() == 0)
                throw new IllegalStateException("queue is full");

            // sets the nextWriteLocation my moving it on by 1, this may cause it it wrap back to the start.
            final long nextWriteLocation = nextlocation(writeLocation);

            return nextWriteLocation;
        }


        long blockForReadSpace(long readLocation) {
            final long nextReadLocation = nextlocation(readLocation);


            // in the for loop below, we are blocked reading unit another item is written, this is because we are empty ( aka size()=0)
            // inside the for loop, getting the 'writeLocation', this will serve as our read memory barrier.
            while (readupto.get() == readLocation)
                return -1;


            return nextReadLocation;
        }


        long nextlocation(long location) {
            return nextlocation(location, 1);
        }

        long nextlocation(long location, int increment) {

            long result = location + increment;
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
