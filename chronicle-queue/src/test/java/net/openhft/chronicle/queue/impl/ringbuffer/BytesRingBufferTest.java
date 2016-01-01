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

package net.openhft.chronicle.queue.impl.ringbuffer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.bytes.BytesStore.wrap;
import static net.openhft.chronicle.bytes.NativeBytesStore.nativeStoreWithFixedCapacity;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
//@Ignore("Waiting to use the fixed Bytes.bytes() as a slice")
public class BytesRingBufferTest {

    private final String EXPECTED = "hello world";

    private Bytes<ByteBuffer> input = wrap(ByteBuffer.allocate(12)).bytesForRead();

    /**
     * @return sample data
     */
    private Bytes<ByteBuffer> data() {
        final Bytes<ByteBuffer> b = elasticByteBuffer();
        b.writeUtf8(EXPECTED);
        final long l = b.writePosition();
        b.readLimit(l);
        b.readPosition(0);
        return b;
    }

    @Test
    public void testWriteAndRead() throws Exception {
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity(150)) {
            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();
            bytesRingBuffer.offer(data());
            Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
            assertEquals(EXPECTED, actual.readUTFΔ());
        }
    }

    @Test
    public void testWriteAndReadSingleThreadedWriteManyTimes() throws Exception {
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
    public void testWithDifferentBufferSizes() throws Exception {
        for (int j = 50; j < 200; j++) {
            try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity(j)) {

                final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
                bytesRingBuffer.clear();
                for (int i = 0; i < 1000; i++) {
                    bytesRingBuffer.offer(data());
                    Bytes actual = bytesRingBuffer.take(maxSize -> input.clear());
                    assertEquals(EXPECTED, actual.readUTFΔ());
                }
            }
        }
    }

    /**
     * one writer thread one reader thread, writer if faster than reader
     *
     * @throws Exception if something bad happens
     */
    @Test
    public void testMultiThreadedFasterWriterThanReader() throws Throwable {
        final AtomicBoolean shutdown = new AtomicBoolean();

        final int numberOfIterations = 100;
        final ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(numberOfIterations);
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity(150)) {

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();
            final ExecutorService executorService = Executors.newFixedThreadPool(2);
            final Future<Throwable> f1 = executorService.submit(() -> {

                for (; !shutdown.get(); ) {
                    try {
                        if (bytesRingBuffer.apply(b -> q.offer(b.readUTFΔ())) == 0)
                            Thread.sleep(1);

                    } catch (Throwable e) {
                        return e;
                    }
                }
                return null;
            });

            final Future<Throwable> f2 = executorService.submit(() -> {
                for (; !shutdown.get(); ) {
                    try {
                        if (!bytesRingBuffer.offer(data()))
                            Thread.sleep(2);
                    } catch (Throwable e) {
                        return e;
                    }
                }
                return null;
            });

            for (int i = 0; i < numberOfIterations; i++) {
                Assert.assertEquals(EXPECTED, q.poll(1, TimeUnit.SECONDS));
            }

            shutdown.set(true);

            final Throwable t = f1.get();
            if (t != null)
                throw t;

            final Throwable t2 = f2.get();
            if (t2 != null)
                throw t2;

            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.SECONDS);

        }
    }

    @Test
    public void testMultiThreadedFasterReaderThanWriter() throws Throwable {
        final AtomicBoolean shutdown = new AtomicBoolean();

        final int numberOfIterations = 100;
        final ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(numberOfIterations);
        try (NativeBytesStore<Void> nativeStore = nativeStoreWithFixedCapacity(150)) {

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
            bytesRingBuffer.clear();
            final ExecutorService executorService = Executors.newFixedThreadPool(2);
            final Future<Throwable> f1 = executorService.submit(() -> {

                for (; !shutdown.get(); ) {
                    try {
                        if (bytesRingBuffer.apply(b -> q.offer(b.readUTFΔ())) == 0)
                            Thread.sleep(2);

                    } catch (Throwable e) {
                        return e;
                    }
                }
                return null;
            });

            final Future<Throwable> f2 = executorService.submit(() -> {
                for (; !shutdown.get(); ) {
                    try {
                        if (!bytesRingBuffer.offer(data()))
                            Thread.sleep(1);

                    } catch (Throwable e) {
                        return e;
                    }
                }
                return null;
            });

            for (int i = 0; i < numberOfIterations; i++) {
                Assert.assertEquals(EXPECTED, q.poll(1, TimeUnit.SECONDS));
            }

            shutdown.set(true);

            final Throwable t = f1.get();
            if (t != null)
                throw t;

            final Throwable t2 = f2.get();
            if (t2 != null)
                throw t2;

            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.SECONDS);

        }
    }

}