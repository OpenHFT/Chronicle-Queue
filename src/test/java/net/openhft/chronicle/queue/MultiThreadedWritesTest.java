package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.threads.MilliPauser;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultiThreadedWritesTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedWritesTest.class);

    @Test
    public void multiThreadedWrites_2Threads() throws InterruptedException {
        runMultiThreadedWriteTestAWithNThreads(2);
    }

    @Test
    public void multiThreadedWrites_5Threads() throws InterruptedException {
        runMultiThreadedWriteTestAWithNThreads(5);
    }

    @Test
    public void multiThreadedWrites_10Threads() throws InterruptedException {
        runMultiThreadedWriteTestAWithNThreads(10);
    }

    private void runMultiThreadedWriteTestAWithNThreads(int threadCount) throws InterruptedException {
        String queuePath = OS.getTarget() + "/MultiThreadedWritesTest-multiThreadWrites-" + Time.uniqueId();
        try (ChronicleQueue queue = ChronicleQueue.single(queuePath)) {

            TestContext testContext = new TestContext(queue);
            Collection<Thread> threads = createWriterThreads(testContext, threadCount);
            threads.forEach(Thread::start);

            // Let the test run for 10s
            Thread.sleep(10_000);

            // Stop it running
            testContext.stop();

            // Wait for threads to exit
            waitForThreadsToExit(threads);
        } finally {
            // Clean up
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    private static void waitForThreadsToExit(Collection<Thread> threads) {
        threads.forEach(t -> {
            try {
                t.join(5_000, 0);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Collection<Thread> createWriterThreads(TestContext testContext, int count) {
        return IntStream.range(0, count).mapToObj(i -> createWriterThread(testContext, "Writer-" + i)).collect(Collectors.toList());
    }

    private Thread createWriterThread(TestContext testContext, String threadName) {
        Thread thread = new Thread(() -> {
            try (ExcerptAppender appender = testContext.chronicleQueue.acquireAppender()) {

                MilliPauser pauser = Pauser.millis(10);
                long lastPreTouch = 0;

                // Append to the queue repeatedly until stopped
                while (testContext.isRunning()) {

                    // Pre-touch every second
                    if (System.currentTimeMillis() >= lastPreTouch + 1_000) {
                        LOGGER.info("Pre-touching on thread " + Thread.currentThread().getName());
                        lastPreTouch = System.currentTimeMillis();
                        appender.pretouch();
                    }

                    // Write something
                    try (DocumentContext context = appender.writingDocument()) {
                        context.wire().writeText("<data>");
                        LOGGER.info("Thread {} writing at index {}", Thread.currentThread().getName(), context.index());
                    }

                    pauser.pause();
                }
            }
        });
        thread.setName(threadName);
        return thread;
    }

    private static class TestContext {

        private final ChronicleQueue chronicleQueue;
        private volatile boolean running = true;

        private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        private TestContext(ChronicleQueue chronicleQueue) {
            this.chronicleQueue = chronicleQueue;
        }

        public void stop() {
            running = false;
        }

        public boolean isRunning() {
            return running;
        }

    }

}
