package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RollCycleTest {
    @Test
    public void newRollCycleIgnored() throws Exception {
        File path = Utils.tempDir("newRollCycleIgnored");
        TestTimeProvider timeProvider = new TestTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path.toPath());

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .fieldlessBinary(path)
                .testBlockSize()
                .rollCycle(RollCycles.DAILY)
                .timeProvider(timeProvider)
                .build()) {
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
        observer.queue.close();
    }

    @Test
    public void newRollCycleIgnored2() throws Exception {
        File path = Utils.tempDir("newRollCycleIgnored2");

        TestTimeProvider timeProvider = new TestTimeProvider();
        ParallelQueueObserver observer = new ParallelQueueObserver(timeProvider, path.toPath());

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.fieldlessBinary(path)
                .testBlockSize()
                .rollCycle(RollCycles.DAILY)
                .timeProvider(timeProvider)
                .build()) {
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
        observer.queue.close();
    }

    @Test
    public void testWriteToCorruptedFile() throws Exception {

        File dir = Utils.tempDir("testWriteToCorruptedFile");
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("hello world");
            }
            Bytes bytes;
            long pos;
            try (DocumentContext dc = appender.writingDocument()) {
                bytes = dc.wire().bytes();
                pos = bytes.writePosition() - 4;
            }

            // write as not complete.
            bytes.writeInt(pos, Wires.NOT_COMPLETE_UNKNOWN_LENGTH);

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("hello world 2");
            }

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write().text("hello world 3");
            }
        }
    }

    @After
    public void checkMappedFiles() {
        MappedFile.checkMappedFiles();
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

        public ParallelQueueObserver(TimeProvider timeProvider, @NotNull Path path) {
            queue = SingleChronicleQueueBuilder.fieldlessBinary(path.toFile())
                    .testBlockSize()
                    .rollCycle(RollCycles.DAILY)
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
}