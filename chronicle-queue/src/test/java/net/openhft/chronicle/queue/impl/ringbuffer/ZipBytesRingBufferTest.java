package net.openhft.chronicle.queue.impl.ringbuffer;

import junit.framework.TestCase;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import org.junit.Test;

import java.io.File;
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

            final BytesRingBuffer ring = new BytesRingBuffer(allocate.bytes());

            final ZippedDocumentAppender zippedDocumentAppender = new ZippedDocumentAppender
                    (ring, chronicle);
            zippedDocumentAppender.append(message);

            Thread.sleep(1000);
            Bytes expected = DirectStore.allocate(100).bytes();

            AtomicLong offset = new AtomicLong(chronicle.firstBytes());

            // todo check the result
            chronicle.readDocument(offset, expected);
            System.out.println(expected.remaining());


        } finally {
            if (file != null)
                file.delete();
        }

    }
}