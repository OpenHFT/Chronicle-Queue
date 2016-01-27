package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Rob Austin.
 */
public class ThreadedQueueTest {

    public static final int MESSAGE_SIZE = 1024;
    public static final int REQUIRED_COUNT = 10;
    private static final int BLOCK_SIZE = 256 << 20;

    @Test(timeout = 1000)
    public void testMultipleThreads() throws Exception {

        final String path = ChronicleQueueTestBase.getTmpDir() + "/deleteme.q";

        new File(path).deleteOnExit();

        final Bytes bytes = Bytes.elasticByteBuffer();
        final AtomicInteger counter = new AtomicInteger();
        final ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        final ExcerptTailer tailer = rqueue.createTailer();

        final ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        final ExcerptAppender appender = wqueue.createAppender();
        final Bytes message = Bytes.elasticByteBuffer();

        Executors.newSingleThreadExecutor(new NamedThreadFactory("tailer", true)).submit(() -> {
            while (counter.get() < REQUIRED_COUNT) {
                bytes.clear();
                if (tailer.readBytes(bytes))
                    counter.incrementAndGet();

            }
        });

        Executors.newSingleThreadExecutor(new NamedThreadFactory("appender", true)).submit(() -> {
            for (int i = 0; i < REQUIRED_COUNT; i++) {
                message.clear();
                message.append(i);
                appender.writeBytes(message);
            }

        });

        while (counter.get() < REQUIRED_COUNT) {
            Thread.yield();
        }

    }


    @Test(timeout = 5000)
    public void testTailerReadingEmptyQueue() throws Exception {

        final String path = ChronicleQueueTestBase.getTmpDir() + "/deleteme.q";

        new File(path).deleteOnExit();


        final AtomicInteger counter = new AtomicInteger();
        final ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        final ExcerptTailer tailer = rqueue.createTailer();

        final ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();


        Bytes bytes = Bytes.elasticByteBuffer();
        tailer.readBytes(bytes);

        final ExcerptAppender appender = wqueue.createAppender();
        appender.writeBytes(Bytes.wrapForRead("Hello World".getBytes()));

        bytes.clear();
        tailer.readBytes(bytes);

        //   System.out.println("");


    }

}
