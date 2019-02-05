package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TwoAppendersTest {
    private static final int MAX = 100;
    private static Logger LOG = LoggerFactory.getLogger(TwoAppendersTest.class);
    private static final RollCycles ROLL_CYCLE = RollCycles.HUGE_DAILY_XSPARSE; // TEST_DAILY; //TEST_SECONDLY;

    @Test
    public void testAlternate() throws Exception {
        try (final SingleChronicleQueue queue = builder(DirectoryUtils.tempDir("two_appenders")).build()) {

            final ExecutorService es = Executors.newFixedThreadPool(2);

            ArrayList<Future> futures = new ArrayList<>();
            AppenderThread thread1 = new AppenderThread(queue, 1);
            futures.add(es.submit(thread1));
            AppenderThread thread2 = new AppenderThread(queue, 2);
            futures.add(es.submit(thread2));

            // write one then timeout xxxxx

            writeBoth(thread1, thread2);
            writeBoth(thread1, thread2);
            writeBoth(thread1, thread2);

            es.shutdownNow();
            for (Future f : futures)
                f.get(1, TimeUnit.SECONDS);
        }
    }

    private void writeBoth(AppenderThread thread1, AppenderThread thread2) {
        thread1.signal.set(true);
        Jvm.pause(500);
        thread2.signal.set(true);
        Jvm.pause(500);
    }

    @Test
    @Ignore
    public void testFaster() throws Exception {
        try (final SingleChronicleQueue queue = builder(DirectoryUtils.tempDir("two_appenders2")).build()) {

            final ExecutorService es = Executors.newFixedThreadPool(2);

            ArrayList<Future> futures = new ArrayList<>();
            AppenderThread thread1 = new AppenderThread(queue, 1);
            futures.add(es.submit(thread1));
            AppenderThread thread2 = new AppenderThread(queue, 2);
            futures.add(es.submit(thread2));

            thread1.signalAll.set(true);
            Jvm.pause(500);
            thread2.signalAll.set(true);
            Jvm.pause(500_000);

            es.shutdownNow();
            for (Future f : futures)
                f.get(1, TimeUnit.SECONDS);
        }
    }

    private SingleChronicleQueueBuilder builder(final File dir) {
        return SingleChronicleQueueBuilder.binary(dir).testBlockSize().rollCycle(ROLL_CYCLE);
    }

    private class AppenderThread implements Runnable {
        final AtomicBoolean signal = new AtomicBoolean();
        final AtomicBoolean signalAll = new AtomicBoolean();
        final ChronicleQueue queue;
        final int id;

        public AppenderThread(SingleChronicleQueue queue, int id) {
            this.queue = queue;
            this.id = id;
        }

        @Override
        public void run() {
            try {
                ExcerptAppender appender = queue.acquireAppender();
                while (!Thread.currentThread().isInterrupted()) {
                    if (signal.compareAndSet(true, false)) {
                        LOG.info("ID {} writing", id);
                        try (DocumentContext dc = appender.writingDocument()) {
                            dc.wire().bytes().writeInt(id);
                        }
                    }
                    if (signalAll.compareAndSet(true, false)) {
                        LOG.info("ID {} loading", id);
                        while (!Thread.currentThread().isInterrupted()) {
                            try (DocumentContext dc = appender.writingDocument()) {
                                dc.wire().bytes().writeInt(id);
                            }
                            Jvm.pause(5);
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}