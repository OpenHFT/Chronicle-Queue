package net.openhft.chronicle.queue.impl;

import junit.framework.TestCase;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import net.openhft.chronicle.queue.impl.ringbuffer.ZippedDocumentAppender;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public class ZipBytesRingBufferTest extends TestCase {

    @Test
    public void testZipAndAppend() throws Exception {
        File file = null;

        try {


            DirectStore allocate = DirectStore.allocate(1024);
            DirectStore msgBytes = DirectStore.allocate(150);

            DirectBytes message = msgBytes.bytes();
            message.writeUTF("Hello World");
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
            Bytes actual = DirectStore.allocate(100).bytes();
            chronicle.readDocument(offset, actual);

            // "Hello World" zipped should be 13 chars
            Assert.assertEquals(13, actual.flip().remaining());


        } finally {
            if (file != null)
                file.delete();
        }

    }

    public static long lastWrite(SingleChronicleQueue chronicle) throws Exception {
        return getHeader(chronicle).writeByte.getVolatileValue();
    }

    public static Header getHeader(SingleChronicleQueue singleChronicleQueue) throws Exception {
        Field header = singleChronicleQueue.getClass().getDeclaredField("header");
        header.setAccessible(true);

        return (Header) header.get(singleChronicleQueue);
    }
}