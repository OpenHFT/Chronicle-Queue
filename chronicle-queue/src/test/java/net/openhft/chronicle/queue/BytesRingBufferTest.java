package net.openhft.chronicle.queue;


import net.openhft.chronicle.queue.impl.ringbuffer.BytesQueue;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class BytesRingBufferTest {

    Bytes output;
    Bytes input = ByteBufferBytes.wrap(ByteBuffer.allocate(22));
    private final String EXPECTED = "hello world";

    @Before
    public void setup() {
        final Bytes out = new ByteBufferBytes(ByteBuffer.allocate(22));
        out.writeUTF(EXPECTED);
        output = out.flip().slice();

    }

    @Test
    public void testSimpledSingleThreadedWriteRead() throws InterruptedException {
        final BytesQueue bytesRingBuffer = new BytesQueue(ByteBufferBytes.wrap(ByteBuffer.allocate(150 + 1)));

        bytesRingBuffer.offer(output.clear());
        assertEquals(EXPECTED, bytesRingBuffer.poll(input.clear()).readUTF());
    }


    @Test
    public void testFlowAroundSingleThreadedWriteDiffrentSizeBuffers() throws InterruptedException {

        for (int j = 23; j < 100; j++) {
            final BytesQueue bytesRingBuffer = new BytesQueue(ByteBufferBytes.wrap(ByteBuffer.allocate(j + 1)));

            for (int i = 0; i < 50; i++) {
                bytesRingBuffer.offer(output.clear());
                assertEquals(EXPECTED, bytesRingBuffer.poll(input.clear()).readUTF());
            }
        }
    }

    @Test
    public void testWrite3read3SingleThreadedWrite() throws InterruptedException {

        final BytesQueue bytesRingBuffer = new BytesQueue(ByteBufferBytes.wrap(ByteBuffer.allocate(22 * 3 + 1)));

        for (int i = 0; i < 100; i++) {
            bytesRingBuffer.offer(output.clear());
            bytesRingBuffer.offer(output.clear());
            bytesRingBuffer.offer(output.clear());
            assertEquals(EXPECTED, bytesRingBuffer.poll(input.clear()).readUTF());
            assertEquals(EXPECTED, bytesRingBuffer.poll(input.clear()).readUTF());
            assertEquals(EXPECTED, bytesRingBuffer.poll(input.clear()).readUTF());
        }
    }
}
