package net.openhft.chronicle.queue;


import net.openhft.chronicle.queue.impl.ringbuffer.BytesQueue;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@Ignore
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
    public void testSimpledSingleThreadedWriteRead() throws Exception {

        try (DirectStore allocate = DirectStore.allocate(150)) {

            final BytesQueue bytesRingBuffer = new BytesQueue(allocate.bytes());

            bytesRingBuffer.offer(output.clear());
            Bytes poll = bytesRingBuffer.poll(input.clear());
            assertEquals(EXPECTED, poll.readUTF());
        }
    }

    @Test
    public void testPollWithNoData() throws Exception {
        try (DirectStore allocate = DirectStore.allocate(150)) {

            final BytesQueue bytesRingBuffer = new BytesQueue(allocate.bytes());

            Bytes poll = bytesRingBuffer.poll(input.clear());
            assertEquals(null, poll);
        }
    }

    @Test
    public void testWriteAndRead() throws Exception {
        try (DirectStore allocate = DirectStore.allocate(150)) {

            final BytesQueue bytesRingBuffer = new BytesQueue(allocate.bytes());
            bytesRingBuffer.offer(output.clear());
            Bytes poll = bytesRingBuffer.poll(input.clear());
            assertEquals(EXPECTED, poll.readUTF());
        }
    }


    @Test
    public void testFlowAroundSingleThreadedWriteDiffrentSizeBuffers() throws Exception {

        for (int j = 23 + 34; j < 100; j++) {
            try (DirectStore allocate = DirectStore.allocate(j)) {

                final BytesQueue bytesRingBuffer = new BytesQueue(allocate.bytes());

                for (int i = 0; i < 50; i++) {
                    bytesRingBuffer.offer(output.clear());
                    assertEquals(EXPECTED, bytesRingBuffer.poll(input.clear()).readUTF());
                }
            }
        }
    }

    @Test
    public void testWrite3read3SingleThreadedWrite() throws Exception {

        try (DirectStore allocate = DirectStore.allocate(40 * 3)) {
            final BytesQueue bytesRingBuffer = new BytesQueue(allocate.bytes());
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


    private final String EXPECTED_VALUE = "value=";


    @Test
    public void testMultiThreadedCheckAllEntriesRetuernedAreValidText() throws Exception {

        try (DirectStore allocate = DirectStore.allocate(1000)) {
            final BytesQueue bytesRingBuffer = new BytesQueue(allocate.bytes());


            //writer
            int iterations = 20_000;
            {
                ExecutorService executorService = Executors.newFixedThreadPool(2);


                for (int i = 0; i < iterations; i++) {
                    final int j = i;
                    executorService.submit(() -> {
                        try {
                            final Bytes out = new ByteBufferBytes(ByteBuffer.allocate(iterations));
                            String expected = EXPECTED_VALUE + j;
                            out.clear().writeUTF(expected);
                            out.flip();

                            boolean offer;
                            do {
                                offer = bytesRingBuffer.offer(out);
                            } while (!offer);

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (AssertionError e) {
                            e.printStackTrace();
                        }
                    });
                }
            }


            CountDownLatch count = new CountDownLatch(iterations);


            //reader
            {
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                for (int i = 0; i < iterations; i++) {
                    executorService.submit(() -> {

                        try {
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

                            if (actual.startsWith(EXPECTED_VALUE))
                                count.countDown();
                        } catch (Error e) {
                            e.printStackTrace();
                        }

                    });
                }
            }

            Assert.assertTrue(count.await(5000, TimeUnit.SECONDS));

        }
    }

    @Test
    public void testMultiThreadedWithIntValues() throws Exception {

        try (DirectStore allocate = DirectStore.allocate(1000)) {
            final BytesQueue bytesRingBuffer = new BytesQueue(allocate.bytes());

            AtomicInteger counter = new AtomicInteger();
            //writer
            int iterations = 20_000;
            {
                ExecutorService executorService = Executors.newFixedThreadPool(2);


                for (int i = 0; i < iterations; i++) {
                    final int j = i;
                    executorService.submit(() -> {
                        try {
                            final Bytes out = new ByteBufferBytes(ByteBuffer.allocate(iterations));
                            String expected = EXPECTED_VALUE + j;
                            out.clear().writeInt(j);
                            counter.addAndGet(j);
                            out.flip();

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


            CountDownLatch count = new CountDownLatch(iterations);


            //reader
            {
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                for (int i = 0; i < iterations; i++) {
                    executorService.submit(() -> {

                        try {
                            Bytes bytes = ByteBufferBytes.wrap(ByteBuffer.allocate(25));
                            Bytes result = null;
                            do {
                                try {
                                    result = bytesRingBuffer.poll(bytes);
                                } catch (InterruptedException e) {
                                    return;
                                }
                            } while (result == null);


                            int value = result.readInt();
                            counter.addAndGet(-value);

                            count.countDown();
                        } catch (Error e) {
                            e.printStackTrace();
                        }

                    });
                }
            }

            Assert.assertTrue(count.await(5000, TimeUnit.SECONDS));
            Assert.assertEquals(0, counter.get());
        }
    }
}
