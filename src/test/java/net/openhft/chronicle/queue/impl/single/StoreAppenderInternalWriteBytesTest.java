package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;

@Ignore("Broken https://github.com/ChronicleEnterprise/Chronicle-Queue-Enterprise/issues/211")
public class StoreAppenderInternalWriteBytesTest extends ChronicleQueueTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreAppenderInternalWriteBytesTest.class);

    private static final int MESSAGES_TO_WRITE = 10_000;

    @Test
    public void internalWriteBytesShouldBeIdempotentUnderConcurrentUpdates() throws InterruptedException {
        testInternalWriteBytes(3, true);
    }

    @Test
    public void internalWriteBytesShouldBeIdempotent() throws InterruptedException {
        testInternalWriteBytes(3, false);
    }

    public void testInternalWriteBytes(int numCopiers, boolean concurrent) throws InterruptedException {
        final Path sourceDir = IOTools.createTempDirectory("sourceQueue");
        final Path destinationDir = IOTools.createTempDirectory("destinationQueue");

        populateSourceQueue(sourceDir);

        copySourceToDestination(numCopiers, concurrent, sourceDir, destinationDir);

        assertQueueContentsAreTheSame(sourceDir, destinationDir);
    }

    private void copySourceToDestination(int numCopiers, boolean concurrent, Path sourceDir, Path destinationDir) throws InterruptedException {
        ExecutorService es = newFixedThreadPool(concurrent ? numCopiers : 1);
        try {
            List<Future<?>> copierFutures = new ArrayList<>();
            for (int i = 0; i < numCopiers; i++) {
                copierFutures.add(es.submit(new QueueCopier(sourceDir, destinationDir)));
            }
            copierFutures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            es.shutdown();
            assert es.awaitTermination(5, TimeUnit.SECONDS) : "Copier threads didn't stop";
        }
    }

    private void assertQueueContentsAreTheSame(Path sourceDir, Path destinationDir) {
        try (final ChronicleQueue sourceQueue = createQueue(sourceDir, null);
             final ChronicleQueue destinationQueue = createQueue(destinationDir)) {

            /*
             * Normalise destination EOFs first
             *
             * part of the contract with {@link net.openhft.chronicle.queue.impl.single.StoreAppender.writeBytes(long, net.openhft.chronicle.bytes.BytesStore)}
             */
            try (final ExcerptAppender appender = destinationQueue.acquireAppender()) {
                appender.normaliseEOFs();
            }

            try (final ExcerptTailer sourceTailer = sourceQueue.createTailer();
                 final ExcerptTailer destinationTailer = destinationQueue.createTailer()) {
                Bytes<?> sourceBuffer = Bytes.allocateElasticOnHeap(1024);
                Bytes<?> destinationBuffer = Bytes.allocateElasticOnHeap(1024);
                for (int i = 0; i < MESSAGES_TO_WRITE; i++) {
                    sourceBuffer.clear();
                    destinationBuffer.clear();
                    long sourceIndex = sourceTailer.index();
                    long destinationIndex = destinationTailer.index();
                    assert sourceTailer.readBytes(sourceBuffer) : "Source queue is shorter than expected";
                    assert destinationTailer.readBytes(destinationBuffer) : "Destination queue is shorter than expected";
                    assertEquals(format("Mismatch at index %s/%s", Long.toHexString(sourceIndex), Long.toHexString(destinationIndex)),
                            sourceBuffer.toString(), destinationBuffer.toString());
                }
            }
        }
    }

    private class QueueCopier implements Runnable {

        private final Path sourceDir;
        private final Path destinationDir;

        public QueueCopier(Path sourceDir, Path destinationDir) {
            this.sourceDir = sourceDir;
            this.destinationDir = destinationDir;
        }

        @Override
        public void run() {
            LOGGER.info("Starting copier...");
            try (final ChronicleQueue sourceQueue = createQueue(sourceDir, null);
                 final ChronicleQueue destinationQueue = createQueue(destinationDir, null)) {
                try (final ExcerptTailer sourceTailer = sourceQueue.createTailer();
                     final ExcerptAppender destinationAppender = destinationQueue.acquireAppender()) {
                    Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
                    long index;
                    while (true) {
                        buffer.clear();
                        index = sourceTailer.index();
                        if (!sourceTailer.readBytes(buffer)) {
                            break;
                        }
                        ((InternalAppender) destinationAppender).writeBytes(index, buffer);
                    }
                }
            }
            LOGGER.info("Copier finished");
        }
    }

    private void populateSourceQueue(Path queueDir) {
        SetTimeProvider tp = new SetTimeProvider();
        LOGGER.info("Populating source queue...");
        try (final ChronicleQueue queue = createQueue(queueDir)) {
            final long messagePerCycleLimit = queue.rollCycle().maxMessagesPerCycle() - 100;
            try (final ExcerptAppender appender = queue.acquireAppender()) {
                Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
                for (int i = 0; i < MESSAGES_TO_WRITE; i++) {
                    if (i % messagePerCycleLimit == 0) {
                        tp.advanceMillis(TimeUnit.SECONDS.toMillis(1));
                    }
                    buffer.clear();
                    buffer.write(messageForIndex(i));
                    appender.writeBytes(buffer);
                }
            }
        }
        LOGGER.info("Populated source queue");
    }

    private byte[] messageForIndex(long index) {
        return format("Message %d", index).getBytes(StandardCharsets.UTF_8);
    }

    private SingleChronicleQueue createQueue(Path queueDir) {
        return createQueue(queueDir, null);
    }

    @NotNull
    private SingleChronicleQueue createQueue(Path queueDir, TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder
                .binary(queueDir)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(timeProvider)
                .build();
    }
}
