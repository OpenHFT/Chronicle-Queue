package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Rob Austin.
 */
public class ThreadedQueueTest {

    public static final int MESSAGE_SIZE = 1024;
    public static final int REQUIRED_COUNT = 10;
    private static final int BLOCK_SIZE = 256 << 20;

    @Test(timeout = 1000000)
    public void testMultipleThreads() throws java.io.IOException {

        final String path = ChronicleQueueTestBase.getTmpDir() + "/deleteme.q";

        new File(path).deleteOnExit();

        final Bytes bytes = Bytes.elasticByteBuffer();
        final AtomicInteger counter = new AtomicInteger();


        final ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        final ExcerptAppender appender = wqueue.createAppender();


        final ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                .wireType(WireType.FIELDLESS_BINARY)
                .blockSize(BLOCK_SIZE)
                .build();

        final ExcerptTailer tailer = rqueue.createTailer();

        final Bytes message = Bytes.elasticByteBuffer();

        ExecutorService tailerES = Executors.newSingleThreadExecutor(/*new NamedThreadFactory("tailer", true)*/);
        tailerES.submit(() -> {

            try {
                int i = 0;
                while (counter.get() < REQUIRED_COUNT) {
                    bytes.clear();
                    if (tailer.readBytes(bytes))
                        counter.incrementAndGet();


                }

            } catch (Throwable t) {
                t.printStackTrace();
            }

        });

        ExecutorService appenderES = Executors.newSingleThreadExecutor(/*new NamedThreadFactory("appender", true)*/);
        appenderES.submit(() -> {
            try {
                for (int i = 0; i < REQUIRED_COUNT; i++) {
                    message.clear();
                    message.append(i);
                    appender.writeBytes(message);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }

        });

        while (counter.get() < REQUIRED_COUNT) {
            Thread.yield();


        }
        appenderES.shutdown();
        tailerES.shutdown();

    }


    @Test(timeout = 5000)
    public void testTailerReadingEmptyQueue() throws java.io.IOException {

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



    }

}
