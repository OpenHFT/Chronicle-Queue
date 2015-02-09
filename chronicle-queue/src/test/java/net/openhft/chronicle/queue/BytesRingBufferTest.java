package net.openhft.chronicle.queue;


import net.openhft.chronicle.queue.impl.ringbuffer.BytesQueue;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class BytesRingBufferTest {
    private static final Logger LOG = LoggerFactory.getLogger(BytesRingBufferTest.class.getName());

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
    public void testPollWithNoData() throws InterruptedException {
        final BytesQueue bytesRingBuffer = new BytesQueue(ByteBufferBytes.wrap(ByteBuffer.allocate(150 + 1)));

        Bytes poll = bytesRingBuffer.poll(input.clear());
        assertEquals(null, poll);
    }

    @Test
    public void testWriteAndRead() throws InterruptedException {
        final BytesQueue bytesRingBuffer = new BytesQueue(ByteBufferBytes.wrap(ByteBuffer.allocate(150 + 1)));
        bytesRingBuffer.offer(output.clear());
        Bytes poll = bytesRingBuffer.poll(input.clear());
        assertEquals(EXPECTED, poll.readUTF());
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


    private final String EXPECTED_VALUE = "value=";


    @Test
    public void testMultiThreaded() throws InterruptedException {


        final BytesQueue bytesRingBuffer = new BytesQueue(ByteBufferBytes.wrap(ByteBuffer.allocate(22 * 3 + 1)));


        //writer
        {
            ExecutorService executorService = Executors.newSingleThreadExecutor();


            for (int i = 0; i < 100; i++) {
                final int j =i;
                executorService.submit(() -> {
                    final Bytes out = new ByteBufferBytes(ByteBuffer.allocate(22));
                    out.clear().writeUTF(EXPECTED_VALUE +j);
                    out.flip();

                    //  System.out.println("hex=" + AbstractBytes.toHex(out));
                    try {
                        boolean offer;
                        do {


                            offer = bytesRingBuffer.offer(out);
                        } while (!offer);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        }


        CountDownLatch count = new CountDownLatch(100);


        //reader
        {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            for (int i = 0; i < 100; i++) {
                executorService.submit(() -> {
                    Bytes bytes = ByteBufferBytes.wrap(ByteBuffer.allocate(25));
                    Bytes result = null;
                    do {
                        try {
                            result = bytesRingBuffer.poll(bytes);
                        } catch (InterruptedException e) {
                            return;
                        }
                    } while (result == null);


                    String actual = result.clear().readUTF();
                    LOG.info(actual);
                    if (actual.startsWith(EXPECTED_VALUE))
                        count.countDown();


                });
            }
        }

        Assert.assertTrue( count.await(5, TimeUnit.SECONDS));
    }
}
