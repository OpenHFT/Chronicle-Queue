package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.util.concurrent.*;

import static net.openhft.chronicle.queue.ChronicleQueueTestBase.getTmpDir;
import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;

/**
 * @author Rob Austin.
 */
public class MultiThreadedRollTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {

        ExecutorService reader = Executors.newSingleThreadExecutor(new NamedThreadFactory("reader"));

        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(1000);
        String path = getTmpDir() + "/backRoll.q";


        final RollingChronicleQueue wqueue = SingleChronicleQueueBuilder.binary(path)
                .timeProvider(timeProvider)
                .rollCycle(TEST_SECONDLY)
                .build();

        wqueue.acquireAppender().writeText("hello world");

        final RollingChronicleQueue rqueue = SingleChronicleQueueBuilder.binary(path)
                .timeProvider(timeProvider)
                .rollCycle(TEST_SECONDLY)
                .build();

        CountDownLatch c = new CountDownLatch(1);

        ExcerptTailer tailer = rqueue.createTailer();

        Future f = reader.submit(() -> {
            long index;
            try (DocumentContext documentContext = tailer.readingDocument()) {
                index = documentContext.index();
                if (documentContext.isPresent())
                    System.out.println(Long.toHexString(index));

            }

            c.countDown();


            do {

                try (DocumentContext documentContext = tailer.readingDocument()) {
                    index = documentContext.index();
                    //    if (documentContext.isPresent())
                    System.out.println("documentContext.isPresent=" + documentContext.isPresent()
                            + ",index="
                            + Long.toHexString(index));
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while (index != 0x200000000L);

        });

        timeProvider.currentTimeMillis(2000);
        c.await();

        wqueue.acquireAppender().writeText("hello world");


        f.get();
    }
}
