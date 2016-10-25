package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class RollCycleTest {

    Path path;

    @Before
    public void setUp() throws Exception {
        path = Files.createTempDirectory("rollCycleTest");
    }

    @After
    public void tearDown() throws IOException {
        Files.walk(path)
                .collect(Collectors.toCollection(LinkedList::new))
                .descendingIterator()
                .forEachRemaining(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception e) {
                    }
                });
    }

    class TestTimeProvider implements TimeProvider {

        volatile long add = 0;

        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis() + add;
        }

        public void add(long addInMs) {
            add += addInMs;
        }

    }

    class ParallelQueueObserver implements Runnable, StoreFileListener {
        SingleChronicleQueue queue;
        CountDownLatch progressLatch;
        int documentsRead;

        public ParallelQueueObserver(TimeProvider timeProvider, Path path) {
            queue = SingleChronicleQueueBuilder.binary(path)
                    .rollCycle(RollCycles.DAILY).timeProvider(timeProvider).storeFileListener(this).wireType(WireType.FIELDLESS_BINARY).build();

            documentsRead = 0;
            progressLatch = new CountDownLatch(1);
        }

        @Override
        public void run() {

            ExcerptTailer tailer = queue.createTailer();

            progressLatch.countDown();

            while (!Thread.currentThread().isInterrupted()) {

                String readText = tailer.readText();
                if (readText != null) {
                    System.out.println("Read a document " + readText);
                    documentsRead++;
                }
            }
        }

        public void await() throws Exception {
            progressLatch.await();
        }

        public int documentsRead() {
            return documentsRead;
        }

        @Override
        public void onAcquired(int cycle, File file) {
            System.out.println("Acquiring " + file);
        }

        @Override
        public void onReleased(int cycle, File file) {
            System.out.println("Releasing " + file);
        }
    }

    @Test
    public void newRollCycleIgnored() throws Exception {
        TestTimeProvider timeProvider = new TestTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.DAILY).timeProvider(timeProvider).wireType(WireType.FIELDLESS_BINARY).build()) {
            ExcerptAppender appender = queue.acquireAppender();

            Thread thread = new Thread(observer);
            thread.start();

            observer.await();

            // two days pass
            timeProvider.add(TimeUnit.DAYS.toMillis(2));

            appender.writeText("Day 3 data");

            // allow parallel tailer to finish iteration
            Thread.sleep(2000);

            thread.interrupt();
        }

        assertEquals(1, observer.documentsRead);
    }

    @Test
    public void newRollCycleIgnored2() throws Exception {
        TestTimeProvider timeProvider = new TestTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .rollCycle(RollCycles.DAILY).timeProvider(timeProvider).wireType(WireType.FIELDLESS_BINARY).build()) {
            ExcerptAppender appender = queue.acquireAppender();
            // uncomment next line to make the test pass
            appender.writeText("Day 1 data");

            Thread thread = new Thread(observer);
            thread.start();

            observer.await();

            // two days pass
            timeProvider.add(TimeUnit.DAYS.toMillis(2));

            appender.writeText("Day 3 data");

            // allow parallel tailer to finish iteration
            Thread.sleep(2000);

            thread.interrupt();
        }

        assertEquals(2, observer.documentsRead);
    }
}