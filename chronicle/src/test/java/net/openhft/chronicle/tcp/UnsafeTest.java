package net.openhft.chronicle.tcp;

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.DirectByteBufferBytes;
import org.junit.Test;

import java.nio.ByteBuffer;

public class UnsafeTest {

    private final StringBuilder sb = new StringBuilder(128);

    @Test
    public void copyMemory() {
        final int capacity = 2 * 1024;// * 1024;
        final DirectByteBufferBytes source = new DirectByteBufferBytes(ByteBuffer.allocateDirect(capacity));
        for (int i = 0; i < capacity; i += 8) {
            source.writeLong(System.nanoTime());
        }


        final DirectByteBufferBytes destination = new DirectByteBufferBytes(ByteBuffer.allocateDirect(capacity));

        final int runs = 1_000_000;

        final long start = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            destination.clear().write(source.flip());
        }
        final long end = System.nanoTime();

        System.out.printf("Took an avg of %.2f us to copy direct memory.%n", (end - start) / runs / 1e3 );
    }

    @Test
    public void writeMemory() {
        final int capacity = 2 * 1024;// * 1024;
        final ByteBufferBytes source = (ByteBufferBytes) ByteBufferBytes.wrap(ByteBuffer.allocate(capacity));
        for (int i = 0; i < capacity; i += 8) {
            source.writeLong(System.nanoTime());
        }


        final DirectByteBufferBytes destination = new DirectByteBufferBytes(ByteBuffer.allocateDirect(capacity));

        final int runs = 1_000_000;

        final long start = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            destination.clear().write(source.flip());
        }
        final long end = System.nanoTime();

        System.out.printf("Took an avg of %.2f us to write to direct memory.%n", (end - start) / runs / 1e3 );
    }

    @Test
    public void test() {
        int result = (int) System.nanoTime();
        for (int i = 0; i < 1000000000; i++) {
            result |= assertSize(i);
        }
        System.out.println(result);
    }

    private int assertSize(int receivedSize) {
        if (receivedSize > 128 << 20 || receivedSize < 0) {
//            throw new TcpHandlingException("size was " + receivedSize);
        }
        return receivedSize;
    }

}
