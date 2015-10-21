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

import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.work.in.progress.Header;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;

@Ignore("Waiting to use the fixed Bytes.bytes() as a slice")
public class ZipBytesRingBufferTest {

    @Ignore
    @Test
    public void testZipAndAppend() throws Exception {
        File file = null;

        try {

            NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(1024);
            NativeBytesStore msgBytes = NativeBytesStore.nativeStoreWithFixedCapacity(150);

            net.openhft.chronicle.bytes.Bytes message = msgBytes.bytesForWrite();
            message.writeUTFΔ("Hello World");


            file = File.createTempFile("chronicle", "q");
            ChronicleQueue chronicle = new SingleChronicleQueueBuilder(File.createTempFile("chron", "queue")).build();

            final long writeAddress = getHeader(chronicle).getWriteByte();


            final BytesRingBuffer ring = new BytesRingBuffer(allocate.bytesForWrite());

            final ZippedDocumentAppender zippedDocumentAppender = new ZippedDocumentAppender(
                    ring,
                    chronicle
            );

            zippedDocumentAppender.append(message);

            //   long initialValue = chronicle.firstBytes();
            //   AtomicLong offset = new AtomicLong(initialValue);

            while (lastWrite(chronicle) == writeAddress) {
                // wait for data to be written ( via another thread )
            }

            ExcerptTailer tailer = chronicle.createTailer();


            // read the data from chronicle into actual
            tailer.readDocument(wire -> Assert.assertEquals("Hello World", wire.read().text()));

        } finally {
            if (file != null)
                file.delete();
        }

    }

    public static Header getHeader(ChronicleQueue singleChronicleQueue) throws Exception {
        Field header = singleChronicleQueue.getClass().getDeclaredField("header");
        header.setAccessible(true);

        return (Header) header.get(singleChronicleQueue);
    }

    public static long lastWrite(ChronicleQueue chronicle) throws Exception {
        return getHeader(chronicle).writeByte().getVolatileValue();
    }
}