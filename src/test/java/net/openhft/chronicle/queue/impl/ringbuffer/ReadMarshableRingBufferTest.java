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
import net.openhft.chronicle.core.OS;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;

/**
 * @author Rob Austin.
 */
//@Ignore("Waiting to use the fixed Bytes.bytes() as a slice")
public class ReadMarshableRingBufferTest {

    private final String EXPECTED = "hello world";

    @Test
    public void testWriteAndReadWithReadBytesMarshallable() throws Exception {
        Bytes bytesRead = Bytes.elasticByteBuffer();

        Bytes data = Bytes.elasticByteBuffer();
        data.writeUtf8(EXPECTED);
        final long len = data.writePosition();
        for (int j = 0; j < 10; j++) {

            try (NativeBytesStore<Void> nativeStore = NativeBytesStore
                    .nativeStoreWithFixedCapacity(BytesRingBuffer.sizeFor(OS.pageSize() << j))) {
                nativeStore.zeroOut(0, nativeStore.capacity());

                final BytesRingBuffer bytesRingBuffer = BytesRingBuffer.newInstance(nativeStore);
                bytesRingBuffer.clear();

                for (int i = 0; i < 100; i++) {
                    data.readPosition(0);
                    data.readLimit(len);
                    bytesRingBuffer.offer(data);


                    bytesRead.clear();
                    bytesRingBuffer.read(bytesRead);
                    final String s = bytesRead.readUTFΔ();


                    Assert.assertEquals(EXPECTED, s);
                }
            }
        }
    }

    @Test
    public void testWriteAndRead3SingleThreadedWrite() throws Exception {
        Bytes bytesRead = Bytes.elasticByteBuffer();

        try (NativeBytesStore<Void> nativeStore = NativeBytesStore.nativeStoreWithFixedCapacity
                (BytesRingBuffer.sizeFor(OS.pageSize()))) {
            nativeStore.zeroOut(0, nativeStore.writeLimit());

            final BytesRingBuffer bytesRingBuffer = BytesRingBuffer.newInstance(nativeStore);

            for (int i = 0; i < 100; i++) {

                bytesRingBuffer.offer(data());


                bytesRead.clear();
                bytesRingBuffer.read(bytesRead);
                final String s = bytesRead.readUTFΔ();
                Assert.assertEquals(EXPECTED, s);
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
