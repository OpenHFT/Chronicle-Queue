package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

@RequiredForClient
public class AcquireReleaseTest extends ChronicleQueueTestBase {
    @Test
    public void testAcquireAndRelease() {
        File dir = getTmpDir();

        AtomicInteger acount = new AtomicInteger();
        AtomicInteger qcount = new AtomicInteger();
        StoreFileListener sfl = new StoreFileListener() {
            @Override
            public void onAcquired(int cycle, File file) {
                // System.out.println("onAcquired(): " + file);
                acount.incrementAndGet();
            }

            @Override
            public void onReleased(int cycle, File file) {
                // System.out.println("onReleased(): " + file);
                qcount.incrementAndGet();
            }
        };
        AtomicLong time = new AtomicLong(1000L);
        TimeProvider tp = () -> time.getAndAccumulate(1000, (x, y) -> x + y);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .storeFileListener(sfl)
                .timeProvider(tp)
                .build()) {

            int iter = 4;
            try (ExcerptAppender excerptAppender = queue.acquireAppender()) {
                for (int i = 0; i < iter; i++) {
                    excerptAppender.writeDocument(w -> {
                        w.write("a").marshallable(m -> {
                            m.write("b").text("c");
                        });
                    });
                }
            }

            BackgroundResourceReleaser.releasePendingResources();

            Assert.assertEquals(iter, acount.get());
            Assert.assertEquals(iter, qcount.get());
        }
    }

    @Test
    public void testReserveAndRelease() {
        File dir = getTmpDir();

        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(1000);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {
            queue.acquireAppender().writeText("Hello World");
            stp.currentTimeMillis(2000);
            queue.acquireAppender().writeText("Hello World");
            queue.createTailer().readText();
            try (ExcerptTailer tailer = queue.createTailer()) {
                tailer.readText();
                tailer.readText();
                tailer.readText();
            }
        }
    }

    @Test
    public void testWithCleanupStoreFilesWithNoDataAcquireAndRelease() throws InterruptedException, ExecutionException {
        final File dir = getTmpDir();
        final SetTimeProvider stp = new SetTimeProvider();
        final AtomicInteger acount = new AtomicInteger();
        final AtomicInteger qcount = new AtomicInteger();
        final StoreFileListener storeFileListener = new StoreFileListener() {
            public void onAcquired(int cycle, File file) {
                acount.incrementAndGet();
            }

            @Override
            public void onReleased(int cycle, File file) {
                qcount.incrementAndGet();
            }
        };

        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .storeFileListener(storeFileListener)
                .timeProvider(stp)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();) {
            // new appender created
            final ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("Main thread: Hello world");
            assertEquals(1, acount.get());

            stp.advanceMillis(1000L); // advance 1 cycle, so that cleanupStoreFilesWithNoData() acquires store

            // other appender is created
            CompletableFuture.runAsync(queue::acquireAppender).get();  // Here store is Acquired twice (second time in cleanupStoreFilesWithNoData())

            Thread.sleep(10); // Let BackgroundResourceReleaser run onAcquired()/onReleased() callbacks
        }

        // Once is called when creating first appender, and twice when creating second appender ( extra time in cleanupStoreFilesWithNoData())
        assertEquals(3, acount.get());

        assertEquals(3, qcount.get());
    }
}
