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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;

/**
 * @author Rob Austin.
 */
//@Ignore("Waiting to use the fixed Bytes.bytes() as a slice")
public class ReadMarshableRingBufferTest {

    private final String EXPECTED = "hello world";

    @Test
    public void testWriteAndReadWithReadBytesMarshallable() throws Exception {

        Bytes data = Bytes.elasticByteBuffer();
        data.writeUtf8(EXPECTED);
        final long len = data.writePosition();
        for (int j = 50; j < 100; j++) {

            try (NativeBytesStore<Void> nativeStore = NativeBytesStore
                    .nativeStoreWithFixedCapacity(j)) {
                nativeStore.zeroOut(0, nativeStore.capacity());

                final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);
                bytesRingBuffer.clear();

                for (int i = 0; i < 100; i++) {
                    data.readPosition(0);
                    data.readLimit(len);
                    bytesRingBuffer.offer(data);

                    final ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(1);
                    bytesRingBuffer.read(b -> q.add(b.readUTFΔ()));

                    Assert.assertEquals(EXPECTED, q.poll(1, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void testWriteAndRead3SingleThreadedWrite() throws Exception {
        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity
                (150)) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(nativeStore);

            for (int i = 0; i < 100; i++) {

                bytesRingBuffer.offer(data());

                final ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(1);
                bytesRingBuffer.read(b -> q.add(b.readUTFΔ()));

                Assert.assertEquals(EXPECTED, q.poll(1, TimeUnit.SECONDS));
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

}
