package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import net.openhft.chronicle.queue.impl.ringbuffer.ZippedDocumentAppender;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public class ZipBytesRingBufferTest {

    @Test
    public void testZipAndAppend() throws Exception {
        File file = null;

        try {

            NativeBytesStore allocate =  NativeBytesStore.nativeStore(1024);
            NativeBytesStore msgBytes = NativeBytesStore.nativeStore(150);

            net.openhft.chronicle.bytes.Bytes message = msgBytes.bytes();
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
            Bytes actual = NativeBytesStore.nativeStore(100).bytes();
            chronicle.readDocument(offset, actual);

            // "Hello World" zipped should be 12 chars
            Assert.assertEquals(12, actual.flip().remaining());


        } finally {
            if (file != null)
                file.delete();
        }

    }

    public static long lastWrite(SingleChronicleQueue chronicle) throws Exception {
        return getHeader(chronicle).writeByte().getVolatileValue();
    }

    public static Header getHeader(SingleChronicleQueue singleChronicleQueue) throws Exception {
        Field header = singleChronicleQueue.getClass().getDeclaredField("header");
        header.setAccessible(true);

        return (Header) header.get(singleChronicleQueue);
    }
}