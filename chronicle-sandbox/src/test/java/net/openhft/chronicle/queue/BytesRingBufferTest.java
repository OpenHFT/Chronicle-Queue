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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore("Waiting to use the fixed Bytes.bytes() as a slice")
public class BytesRingBufferTest {

    private final String EXPECTED = "hello world";
    private final String EXPECTED_VALUE = "value=";
    Bytes<ByteBuffer> input = Bytes.wrap(ByteBuffer.allocate(12));
    Bytes<ByteBuffer> output;
    Bytes<ByteBuffer> out;
    NativeBytesStore outBuffer;

    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void setup() {
        try {
            outBuffer = NativeBytesStore.nativeStoreWithFixedCapacity(12);
            out = outBuffer.bytes();
            out.writeUTFΔ(EXPECTED);
            output = out.flip().bytes();
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    @After
    public void after() {
        outBuffer.releaseLast();
    }

    @Test
    public void testWriteAndRead3SingleThreadedWrite() {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(24)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            for (int i = 0; i < 100; i++) {
                bytesRingBuffer.offer(data());
                Bytes<?> bytes = bytesRingBuffer.take(maxSize -> {
                    Bytes<ByteBuffer> clear = input.clear();
                    return clear;
                });

                assertEquals(EXPECTED, bytes.readUTFΔ());
            }
        }
    }

    private Bytes<ByteBuffer> data() {
        output.clear();
        return output;
    }

    @Test
    public void testSimpledSingleThreadedWriteRead() {

        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            bytesRingBuffer.offer(data());
            Bytes<?> actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }

    @Test
    public void testPollWithNoData() {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            assert nativeStore.isNative();

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            Bytes<?> actual = bytesRingBuffer.poll(maxSize -> input.clear());
            assertEquals(null, actual);
        }
    }

    @Test
    public void testWriteAndRead() {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            assert nativeStore.isNative();
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());
            data();
            bytesRingBuffer.offer(data());
            Bytes<?> actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }

    @Test
    public void testFlowAroundSingleThreadedWriteDifferentSizeBuffers() {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
           // System.out.println(nativeStore.realCapacity());
           // System.out.println(nativeStore.capacity());
           // System.out.println(nativeStore.limit());
            assert !nativeStore.isElastic();

            Bytes<Void> bytes = nativeStore.bytes();
           // System.out.println(bytes);

            for (int j = 23 + 34; j < 100; j++) {
                final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

                for (int i = 0; i < 50; i++) {
                    bytesRingBuffer.offer(data());
                    String actual = bytesRingBuffer.take(maxSize -> input.clear()).readUTFΔ();
                    assertEquals(EXPECTED, actual);
                }
            }
        }
    }

    @Test
    public void testWrite3read3SingleThreadedWrite() {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            assert nativeStore.bytes().capacity() < (1 << 12);
            for (int i = 0; i < 100; i++) {
                bytesRingBuffer.offer(data());
                     // bytesRingBuffer.offer(data());
                       // bytesRingBuffer.offer(data());
                  // assertEquals(EXPECTED, bytesRingBuffer.take(maxSize -> input.clear()).readUTFΔ());

                Bytes<?> bytes = bytesRingBuffer.take(maxSize -> {
                    Bytes<ByteBuffer> clear = input.clear();
                    return clear;
                });

                try {

                    String s = bytes.readUTFΔ();
                } catch (AssertionError e) {
                   // System.out.println(Bytes.toHex(bytes));
                }

                  // System.out.println("hex="+Bytes.toHex(bytes));
                  // assertEquals(EXPECTED, bytes.readUTFΔ());
            }
        }
    }

    // @Ignore("works in lang-bytes")
    @Test
    public void testMultiThreadedCheckAllEntriesReturnedAreValidText() throws InterruptedException {
       // System.out.println("Hello World");
        try (NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1000)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(allocate.bytes());

            //writer
            final int iterations = 100_000;
            {
                ExecutorService executorService = Executors.newFixedThreadPool(2);

                for (int i = 0; i < iterations; i++) {
                    final int j = i;
                    executorService.submit(() -> {
                        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(iterations)) {
                            final Bytes<?> out = nativeStore.bytes();
                            String expected = EXPECTED_VALUE + j;
                            out.clear();
                            out.writeUTFΔ(expected);
                            out.flip();

                            boolean offer;
                            do {
                                offer = bytesRingBuffer.offer(out);
                            } while (!offer);
                           // System.out.println("+");
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    });
                }
            }

            CountDownLatch count = new CountDownLatch(iterations);
           // System.out.println(count);

            //reader
            {
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                for (int i = 0; i < iterations; i++) {
                    executorService.submit(() -> {

                        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(25)) {
                            Bytes<?> bytes = nativeStore.bytes();
                            Bytes<?> result = null;
                            do {
                                try {
                                    result = bytesRingBuffer.poll(maxSize -> bytes);
                                } catch (InterruptedException e) {
                                    return;
                                }
                            } while (result == null);

                            result.clear();
                            String actual = result.readUTFΔ();

                            if (actual.startsWith(EXPECTED_VALUE))
                                count.countDown();
                           // System.out.println("-");
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                    });
                }
            }

            Assert.assertTrue(count.await(5000, TimeUnit.SECONDS));
        }
    }

    // @Ignore("works in lang-bytes, appears to be a visibility issue that can be fixed by adding
    // a" +
           // " synchronized to ringbuffer.poll() and ringbuffer.offer()")
    @Test
    public void testMultiThreadedWithIntValues() {

        try (NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1000)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(allocate.bytes());

            AtomicInteger counter = new AtomicInteger();
            //writer
            int iterations = 20_000;
            {
                ExecutorService executorService = Executors.newFixedThreadPool(2);

                for (int i = 0; i < iterations; i++) {
                    final int j = i;
                    executorService.submit(() -> {

                        try (NativeBytesStore allocate2 = NativeBytesStore.nativeStoreWithFixedCapacity(iterations)) {
                            final Bytes<?> out = allocate2.bytes();

                            out.clear();
                            out.writeInt(j);
                            counter.addAndGet(j);
                            out.flip();

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
                                final Bytes<?> bytes = allocate3.bytes();
                                Bytes<?> result = null;
                                do {
                                    try {
                                        result = bytesRingBuffer.poll(maxsize -> bytes);
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
}
