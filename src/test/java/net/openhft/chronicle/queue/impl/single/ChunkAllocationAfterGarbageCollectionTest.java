package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static java.util.stream.IntStream.range;

public final class ChunkAllocationAfterGarbageCollectionTest {
    private static final byte[] DATA = new byte[8192];

    private final File path = DirectoryUtils.tempDir(ChunkAllocationAfterGarbageCollectionTest.class.getSimpleName());

    @Ignore("demonstrates an issue")
    @Test
    public void pretoucherAppenderShouldNotResetToStartOfMappedFile() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().submit(() -> {
            String lastAppenderHashCode = "";
            Thread.currentThread().setName("pre-toucher-thread");
            try (final SingleChronicleQueue queue = createQueue(path, System::currentTimeMillis)) {

                while (!Thread.currentThread().isInterrupted()) {
                    lastAppenderHashCode = preTouch(lastAppenderHashCode, queue, new LoggingNewChunkListener());
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10L));
                }

            }
        });

        try (final SingleChronicleQueue queue = createQueue(path, System::currentTimeMillis)) {
            final ExcerptAppender appender = queue.acquireAppender();
            queue.storeForCycle(queue.cycle(), 0, true).bytes().
                    setNewChunkListener(new LoggingNewChunkListener());
            appender.lazyIndexing(true);

            IntStream.range(0, 5_000).forEach(i -> {
                range(0, 100).forEach(j -> {
                    try (final DocumentContext ctx = appender.writingDocument()) {
                        ctx.wire().write().bytes(DATA);
                    }
                });

                latch.countDown();
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1L));

                if (i % 30 == 0) {
                    System.out.println("GC");
                    GcControls.waitForGcCycle();
                }
            });
        }
    }

    @NotNull
    private String preTouch(final String lastAppenderHashCode, final SingleChronicleQueue queue,
                            final LoggingNewChunkListener chunkListener) {
        final ExcerptAppender appender = queue.acquireAppender();
        queue.storeForCycle(queue.cycle(), 0, true).bytes().
                setNewChunkListener(chunkListener);
        if (!lastAppenderHashCode.equals(Integer.toHexString(System.identityHashCode(appender)))) {
            System.out.println("Appender changed");
        }

        appender.pretouch();
        return Integer.toHexString(System.identityHashCode(appender));
    }

    private static final class LoggingNewChunkListener implements NewChunkListener {
        @Override
        public void onNewChunk(final String filename, final int chunk, final long delayMicros) {
            System.out.printf("%s chunk %d%n", Thread.currentThread().getName(), chunk);
        }
    }

    @After
    public void deleteDir() throws Exception {
        DirectoryUtils.deleteDir(path);
    }

    private static SingleChronicleQueue createQueue(final File path, final TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.
                binary(path).
                timeProvider(timeProvider).
                rollCycle(RollCycles.DAILY).
                testBlockSize().
                wireType(WireType.BINARY).
                build();
    }
}