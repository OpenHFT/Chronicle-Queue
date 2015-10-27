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

package net.openhft.chronicle.queue;


import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.bytes.BytesStore.wrap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Rob Austin.
 */
//@Ignore("Waiting to use the fixed Bytes.bytes() as a slice")
public class BytesRingBufferTest {

    private final String EXPECTED = "123456789";
    private final String EXPECTED_VALUE = "value=";
    Bytes<ByteBuffer> input = wrap(ByteBuffer.allocate(12)).bytesForRead();
    Bytes<ByteBuffer> output;
    Bytes<ByteBuffer> out;
    NativeBytesStore outBuffer;

    @Before
    public void setup() {
        try {
            outBuffer = NativeBytesStore.nativeStoreWithFixedCapacity(12);
            out = outBuffer.bytesForWrite();
            out.writeUTFΔ(EXPECTED);
            output = outBuffer.bytesForRead();
            output.readLimit(out.writeLimit());
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @After
    public void after() {
        outBuffer.release();
    }

    @Test
    public void testWriteAndRead3SingleThreadedWrite() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity
                (150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);

            for (int i = 0; i < 100; i++) {

                bytesRingBuffer.offer(data());

                Bytes bytes = bytesRingBuffer.take(maxSize -> {
                    Bytes<ByteBuffer> clear = input.clear();
                    return clear;
                });


                assertEquals(EXPECTED, bytes.readUTFΔ());

            }
        }
    }


    private Bytes<ByteBuffer> data() {
        final Bytes<ByteBuffer> b = elasticByteBuffer();
        b.writeUtf8(EXPECTED);
        final long l = b.writePosition();
        b.readLimit(l);
        b.readPosition(0);
        return b;
    }

    @Test
    public void testSimpledSingleThreadedWriteRead() throws Exception {

        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity
                (150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.offer(data());
            Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }

    @Test
    public void testPollWithNoData() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity
                (150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            Bytes actual = bytesRingBuffer.poll(maxSize -> input.clear());
            assertEquals(null, actual);
        }

    }

    @Test
    public void testWriteAndRead() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity
                (150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);

            bytesRingBuffer.offer(data());
            Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }


    @Test
    public void testFlowAroundSingleThreadedWriteDifferentSizeBuffers2() throws Exception {
        for (int j = 23 + 34; j < 100; j++) {
            try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity
                    (j)) {
                nativeStore.zeroOut(0, nativeStore.writeLimit());

                final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
                bytesRingBuffer.offer(data());
                Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
                assertEquals(EXPECTED, actual.readUTFΔ());
            }
        }
    }


    @Ignore("works in lang-bytes")
    @Test
    public void testMultiThreadedCheckAllEntriesReturnedAreValidText() throws InterruptedException {
        System.out.println("Hello World");
        try (NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1000)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(allocate);


            //writer
            final int iterations = 100_000;
            {
                ExecutorService executorService = Executors.newFixedThreadPool(2);


                for (int i = 0; i < iterations; i++) {
                    final int j = i;
                    executorService.submit(() -> {
                        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(iterations)) {
                            final Bytes out = nativeStore.bytesForWrite();
                            String expected = EXPECTED_VALUE + j;
                            out.clear();
                            out.writeUTFΔ(expected);
                            //       out.flip();

                            boolean offer;
                            do {
                                offer = bytesRingBuffer.offer(out);
                            } while (!offer);
                            System.out.println("+");

                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    });
                }
            }


            CountDownLatch count = new CountDownLatch(iterations);
            System.out.println(count);


            //reader
            {
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                for (int i = 0; i < iterations; i++) {
                    executorService.submit(() -> {

                        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(25)) {
                            Bytes bytes = nativeStore.bytesForWrite();
                            Bytes result = null;
                            do {
                                result = bytesRingBuffer.poll(maxSize -> bytes);
                            } while (result == null);

                            result.clear();
                            String actual = result.readUTFΔ();

                            if (actual.startsWith(EXPECTED_VALUE))
                                count.countDown();
                            System.out.println("-");
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    });
                }
            }

            Assert.assertTrue(count.await(5000, TimeUnit.SECONDS));

        }
    }

    @Ignore("works in lang-bytes, appears to be a visibility issue that can be fixed by adding a" +
            " synchronized to ringbuffer.poll() and ringbuffer.offer()")
    @Test
    public void testMultiThreadedWithIntValues() throws Exception {

        try (NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1000)) {


            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(allocate.bytesForWrite());

            AtomicInteger counter = new AtomicInteger();
            //writer
            int iterations = 20_000;
            {
                ExecutorService executorService = Executors.newFixedThreadPool(2);


                for (int i = 0; i < iterations; i++) {
                    final int j = i;
                    executorService.submit(() -> {

                        try (NativeBytesStore allocate2 = NativeBytesStore.nativeStoreWithFixedCapacity(iterations)) {
                            final Bytes out = allocate2.bytesForWrite();

                            out.clear();
                            out.writeInt(j);
                            counter.addAndGet(j);
                            //     out.flip();

                            boolean offer;
                            do {
                                offer = bytesRingBuffer.offer(out);
                            } while (!offer);

                        } catch (Exception e) {
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
                            try (NativeBytesStore allocate3 = NativeBytesStore.nativeStoreWithFixedCapacity(25)) {
                                final Bytes bytes = allocate3.bytesForWrite();
                                Bytes result = null;
                                do {
                                    result = bytesRingBuffer.poll(maxsize -> bytes);
                                } while (result == null);


                                int value = result.readInt();
                                counter.addAndGet(-value);

                                count.countDown();
                            } catch (Error e) {
                                e.printStackTrace();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    });
                }
            }

            Assert.assertTrue(count.await(5000, TimeUnit.SECONDS));
            Assert.assertEquals(0, counter.get());
        }
    }


    @Ignore
    @Test
    public void testWriteAndReadX() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);

            for (int i = 0; i < 2; i++) {

                bytesRingBuffer.offer(data());
                bytesRingBuffer.apply(b -> System.out.println("Got: " + b));
            }
        }
    }
}
