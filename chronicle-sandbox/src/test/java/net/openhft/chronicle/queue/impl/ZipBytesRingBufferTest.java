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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

@Ignore("Waiting to use the fixed Bytes.bytes() as a slice")
public class ZipBytesRingBufferTest {

    public static Header getHeader(SingleChronicleQueue singleChronicleQueue) {
        Field header = singleChronicleQueue.getClass().getDeclaredField("header");
        header.setAccessible(true);

        return (Header) header.get(singleChronicleQueue);
    }

    public static long lastWrite(SingleChronicleQueue chronicle) {
        return getHeader(chronicle).writeByte().getVolatileValue();
    }

    @Test
    public void testZipAndAppend() {
        File file = null;

        try {

            NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1024);
            NativeBytesStore msgBytes = NativeBytesStore.nativeStoreWithFixedCapacity(150);

            net.openhft.chronicle.bytes.Bytes<?> message = msgBytes.bytes();
            message.writeUTFΔ("Hello World");
            message.flip();

            file = File.createTempFile("chronicle", "q");
            DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder
                    (file.getName()).build();

            final long writeAddress = getHeader((SingleChronicleQueue) chronicle).getWriteByte();

            final BytesRingBuffer ring = new BytesRingBuffer(allocate.bytes());

            final ZippedDocumentAppender zippedDocumentAppender = new ZippedDocumentAppender(
                    ring,
                    chronicle
            );

            zippedDocumentAppender.append(message);

            long initialValue = chronicle.firstBytes();
            AtomicLong offset = new AtomicLong(initialValue);

            while (lastWrite((SingleChronicleQueue) chronicle) == writeAddress) {
                // wait for data to be written ( via another thread )
            }

            // read the data from chronicle into actual
            Bytes<?> actual = NativeBytesStore.nativeStoreWithFixedCapacity(100).bytes();
            chronicle.readDocument(offset, actual);

            // "Hello World" zipped should be 12 chars
            Assert.assertEquals(12, actual.flip().remaining());
        } finally {
            if (file != null)
                file.delete();
        }
    }
}