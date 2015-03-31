package net.openhft.chronicle.queue;


import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

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
public class BytesRingBufferTest {


    private final String EXPECTED = "hello world";
    private final String EXPECTED_VALUE = "value=";
    Bytes<ByteBuffer> input = Bytes.wrap(ByteBuffer.allocate(12));
    Bytes<ByteBuffer> output;
    Bytes<ByteBuffer> out;
    NativeBytesStore outBuffer;
    @Before
    public void setup() {
        outBuffer = NativeBytesStore.nativeStoreWithFixedCapacity(12);
        out = outBuffer.bytes();
        out.writeUTFΔ(EXPECTED);
        output = out.flip().bytes();
    }

    @After
    public void after() {
        outBuffer.release();
    }


    @Test
    public void testWriteAndRead3SingleThreadedWrite() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(24)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            for (int i = 0; i < 100; i++) {

                bytesRingBuffer.offer(data());
                Bytes bytes = bytesRingBuffer.take(new BytesRingBuffer.BytesProvider() {
                    @NotNull
                    @Override
                    public Bytes provide(long maxSize) {
                        Bytes<ByteBuffer> clear = input.clear();
                        return clear;
                    }
                });

                assertEquals(EXPECTED, bytes.readUTFΔ());

            }
        }
    }


    @Test
    public void testSimpledSingleThreadedWriteRead() throws Exception {

        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            bytesRingBuffer.offer(data());
            Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }

    @Test
    public void testPollWithNoData() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {

            assert nativeStore.isNative();

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            Bytes actual = bytesRingBuffer.poll(maxSize -> input.clear());
            assertEquals(null, actual);
        }

    }

    @Test
    public void testWriteAndRead() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            assert nativeStore.isNative();
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());
            data();
            bytesRingBuffer.offer(data());
            Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }

    private Bytes<ByteBuffer> data() {
        output.clear();
        return output;
    }

    @Test

    public void testFlowAroundSingleThreadedWriteDifferentSizeBuffers() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {

            System.out.println(nativeStore.realCapacity());
            System.out.println(nativeStore.capacity());
            System.out.println(nativeStore.limit());
            assert !nativeStore.isElastic();

            Bytes<Void> bytes = nativeStore.bytes();
            System.out.println(bytes);

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
    public void testWrite3read3SingleThreadedWrite() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(150)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore.bytes());

            assert nativeStore.bytes().capacity() < (1 << 12);
            for (int i = 0; i < 100; i++) {


                bytesRingBuffer.offer(data());
                //      bytesRingBuffer.offer(data());
                //        bytesRingBuffer.offer(data());
                //   assertEquals(EXPECTED, bytesRingBuffer.take(maxSize -> input.clear()).readUTFΔ());
                if (i == 29)
                    System.out.println("");
                Bytes bytes = bytesRingBuffer.take(new BytesRingBuffer.BytesProvider() {
                    @NotNull
                    @Override
                    public Bytes provide(long maxSize) {
                        Bytes<ByteBuffer> clear = input.clear();
                        return clear;
                    }
                });

                try {


                    String s = bytes.readUTFΔ();

                } catch (AssertionError e) {
                    System.out.println(Bytes.toHex(bytes));

                }

                //   System.out.println("hex="+Bytes.toHex(bytes));
                //   assertEquals(EXPECTED, bytes.readUTFΔ());

            }
        }
    }

   // @Ignore("works in lang-bytes")
    @Test
    public void testMultiThreadedCheckAllEntriesReturnedAreValidText() throws Exception {

        try (NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1000)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(allocate.bytes());


            //writer
            int iterations = 20_000;
            {
                ExecutorService executorService = Executors.newFixedThreadPool(2);


                for (int i = 0; i < iterations; i++) {
                    final int j = i;
                    executorService.submit(() -> {
                        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(iterations)) {
                            final Bytes out = nativeStore.bytes();
                            String expected = EXPECTED_VALUE + j;
                            out.clear();
                            out.writeUTFΔ(expected);
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
                            try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity(25)) {
                                Bytes bytes = nativeStore.bytes();
                                Bytes result = null;
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

        }
    }

   // @Ignore("works in lang-bytes, appears to be a visibility issue that can be fixed by adding
   // a" +
    //        " synchronized to ringbuffer.poll() and ringbuffer.offer()")
    @Test
    public void testMultiThreadedWithIntValues() throws Exception {

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
                            final Bytes out = allocate2.bytes();

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
                                final Bytes bytes = allocate3.bytes();
                                Bytes result = null;
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
