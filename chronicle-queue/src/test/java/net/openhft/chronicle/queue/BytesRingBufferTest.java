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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.bytes.BytesStore.wrap;
import static net.openhft.chronicle.bytes.NativeBytesStore.nativeStoreWithFixedCapacity;
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
            outBuffer = nativeStoreWithFixedCapacity(12);
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
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity
                (150)) {

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();

            for (int i = 0; i < 10000; i++) {

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

        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity
                (150)) {

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();

            bytesRingBuffer.offer(data());
            Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }

    @Test
    public void testPollWithNoData() throws Exception {
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity
                (150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();
            Bytes actual = bytesRingBuffer.poll(maxSize -> input.clear());
            assertEquals(null, actual);
        }

    }

    @Test
    public void testWriteAndRead() throws Exception {
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity
                (150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();
            bytesRingBuffer.offer(data());
            Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }


    @Test
    public void testFlowAroundSingleThreadedWriteDifferentSizeBuffers2() throws Exception {
        for (int j = 123 + 34; j < 200; j++) {
            try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity(j)) {

                final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
                bytesRingBuffer.clear();
                bytesRingBuffer.offer(data());
                Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
                assertEquals(EXPECTED, actual.readUTFΔ());
            }
        }
    }


    @Test
    public void testWriteAndRead3SingleThreadedWrite3() throws Exception {

        // ExecutorService executorService = Executors.newFixedThreadPool(2);
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity
                (1000)) {


            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();

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


    @Test
    public void testMultiThreadedFasterWriterThanReader() throws Exception {
        AtomicBoolean shutdown = new AtomicBoolean();

        Bytes data = Bytes.elasticByteBuffer();
        data.writeUtf8(EXPECTED);
        final int max = 100;
        final ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(max);
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity(150)) {


            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            executorService.submit(() -> {
                for (; !shutdown.get(); ) {
                    try {
                        if (bytesRingBuffer.apply(b -> {

                            final String e = b.readUTFΔ();

                            if (e != null)
                                q.offer(e);

                        }) == 0)
                            Thread.sleep(1);


                    } catch (Throwable e) {
                        e.printStackTrace();
                    }

                }
            });

            executorService.submit(() -> {
                for (; !shutdown.get(); ) {
                    try {
                        if (!bytesRingBuffer.offer(data()))
                            Thread.sleep(2);
                        else
                            Thread.sleep(2);
                        // System.out.println("written");
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            });


            for (int i = 0; i < max; i++) {
                Assert.assertEquals(EXPECTED, q.poll(10000000, TimeUnit.SECONDS));
            }
            shutdown.set(true);
            Thread.sleep(3);
            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.SECONDS);

        }
    }


    @Test
    public void testMultiThreadedFasterReaderThanWriter() throws Exception {
        AtomicBoolean shutdown = new AtomicBoolean();

        Bytes data = Bytes.elasticByteBuffer();
        data.writeUtf8(EXPECTED);
        final int max = 100;
        final ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(max);
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity(150)) {

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            executorService.submit(() -> {
                for (; !shutdown.get(); ) {
                    try {
                        if (bytesRingBuffer.apply(b -> {

                            final String e = b.readUTFΔ();

                            if (e != null)
                                q.offer(e);

                        }) == 0)
                            Thread.sleep(3);
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            });

            executorService.submit(() -> {
                for (; !shutdown.get(); ) {
                    try {
                        if (!bytesRingBuffer.offer(data()))
                            Thread.sleep(1);

                        // System.out.println("written");
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
                ;
            });

            for (int i = 0; i < max; i++) {
                Assert.assertEquals(EXPECTED, q.poll(10000000, TimeUnit.SECONDS));

            }


            shutdown.set(true);
            Thread.sleep(3);
            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.SECONDS);


        }
    }


}