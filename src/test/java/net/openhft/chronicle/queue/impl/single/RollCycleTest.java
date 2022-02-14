package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RollCycleTest extends ChronicleQueueTestBase {
    @Test
    public void newRollCycleIgnored() throws InterruptedException {
        File path = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path.toPath());

        try (ChronicleQueue queue = SingleChronicleQueueBuilder
                .fieldlessBinary(path)
                .testBlockSize()
                .rollCycle(RollCycles.DEFAULT)
                .timeProvider(timeProvider)
                .build()) {
            ExcerptAppender appender = queue.acquireAppender();

            Thread thread = new Thread(observer);
            thread.start();

            observer.await();

            // two days pass
            timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(2));

            appender.writeText("0");

            // allow parallel tailer to finish iteration
            for (int i = 0; i < 5_000 && observer.documentsRead != 1; i++) {
                timeProvider.advanceMicros(100);
                Thread.sleep(1);
            }

            thread.interrupt();
        }

        assertEquals(1, observer.documentsRead);
        observer.queue.close();
    }

    @Test
    public void newRollCycleIgnored2() throws InterruptedException {
        File path = getTmpDir();

        SetTimeProvider timeProvider = new SetTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path.toPath());

        int cyclesToWrite = 100;
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.fieldlessBinary(path)
                .testBlockSize()
                .rollCycle(RollCycles.DEFAULT)
                .timeProvider(timeProvider)
                .build()) {
            ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("0");

            Thread thread = new Thread(observer);
            thread.start();

            observer.await();

            for (int i = 1; i <= cyclesToWrite; i++) {
                // two days pass
                timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(2));
                appender.writeText(Integer.toString(i));
            }

            // allow parallel tailer to finish iteration
            for (int i = 0; i < 5_000 && observer.documentsRead != 1 + cyclesToWrite; i++) {
                Thread.sleep(1);
            }

            thread.interrupt();
        }

        assertEquals(1 + cyclesToWrite, observer.documentsRead);
        observer.queue.close();
    }

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }

    class ParallelQueueObserver implements Runnable, StoreFileListener {
        ChronicleQueue queue;
        CountDownLatch progressLatch;
        volatile int documentsRead;

        public ParallelQueueObserver(TimeProvider timeProvider, @NotNull Path path) {
            queue = SingleChronicleQueueBuilder.fieldlessBinary(path.toFile())
                    .testBlockSize()
                    .rollCycle(RollCycles.DEFAULT)
                    .timeProvider(timeProvider)
                    .storeFileListener(this)
                    .build();

            documentsRead = 0;
            progressLatch = new CountDownLatch(1);
        }

        @Override
        public void run() {

            ExcerptTailer tailer = queue.createTailer();

            progressLatch.countDown();

            int lastDocId = -1;
            while (!Thread.currentThread().isInterrupted()) {

                String readText = tailer.readText();
                if (readText != null) {
                   // System.out.println("Read a document " + readText);
                    documentsRead++;
                    int docId = Integer.parseInt(readText);
                    assertEquals(docId, lastDocId + 1);
                    lastDocId = docId;
                }
            }
        }

        public void await() throws InterruptedException {
            progressLatch.await();
        }

        public int documentsRead() {
            return documentsRead;
        }

        @Override
        public void onAcquired(int cycle, File file) {
           // System.out.println("Acquiring " + file);
        }

        @Override
        public void onReleased(int cycle, File file) {
           // System.out.println("Releasing " + file);
        }
    }
}