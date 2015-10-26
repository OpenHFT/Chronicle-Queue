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
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

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
        for (int j = 30; j < 100; j++) {

            try (NativeBytesStore<Void> nativeStore = NativeBytesStore
                    .nativeStoreWithFixedCapacity(j)) {
                nativeStore.zeroOut(0, nativeStore.capacity());
                VanillaBytes<Void> buffer = nativeStore.bytesForWrite();
                Bytes<Void> clearedBytes = buffer.zeroOut(0, buffer.capacity());

                final BytesRingBuffer bytesRingBuffer = new BytesRingBuffer(clearedBytes);

                for (int i = 0; i < 100; i++) {
                    data.readPosition(0);
                    data.readLimit(len);
                    bytesRingBuffer.offer(data);

                    ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(1);

                    bytesRingBuffer.apply(b -> q.add(b.readUTFΔ()));

                    Assert.assertEquals(EXPECTED, q.poll(1, TimeUnit.SECONDS));

                }
            }
        }
    }

}
