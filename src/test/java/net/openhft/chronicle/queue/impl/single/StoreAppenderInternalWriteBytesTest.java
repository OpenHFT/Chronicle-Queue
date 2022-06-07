package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

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
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class StoreAppenderInternalWriteBytesTest extends ChronicleQueueTestBase {

    private static final int MESSAGES_TO_WRITE = 200;

    @Before
    public void check64bit() {
        assumeTrue(Jvm.is64bit());
    }

    @Test
    public void internalWriteBytesShouldBeIdempotentUnderConcurrentUpdates() throws InterruptedException {
        testInternalWriteBytes(5, true);
    }

    @Test
    public void internalWriteBytesShouldBeIdempotent() throws InterruptedException {
        testInternalWriteBytes(5, false);
    }

    public void testInternalWriteBytes(int numCopiers, boolean concurrent) throws InterruptedException {
        final Path sourceDir = IOTools.createTempDirectory("sourceQueue");
        final Path destinationDir = IOTools.createTempDirectory("destinationQueue");
         /**
        final Path sourceDir = Paths.get("/dev/shm/sourceQueue");
        final Path destinationDir = Paths.get("/dev/shm/destinationQueue");
        IOTools.deleteDirWithFiles(sourceDir.toFile());
        IOTools.deleteDirWithFiles(destinationDir.toFile());
*/

        populateSourceQueue(sourceDir);

        copySourceToDestination(numCopiers, concurrent, sourceDir, destinationDir);

        assertQueueContentsAreTheSame(sourceDir, destinationDir);
    }

    private void copySourceToDestination(int numCopiers, boolean concurrent, Path sourceDir, Path destinationDir) throws InterruptedException {
        ExecutorService es = newFixedThreadPool(concurrent ? numCopiers : 1);
        try {
            List<Future<?>> copierFutures = new ArrayList<>();
            for (int i = 0; i < numCopiers; i++) {
                copierFutures.add(es.submit(new QueueCopier(sourceDir, destinationDir, i)));
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
            assert es.awaitTermination(30, TimeUnit.SECONDS) : "Copier threads didn't stop";
        }
    }

    private void assertQueueContentsAreTheSame(Path sourceDir, Path destinationDir) {
        try (final ChronicleQueue sourceQueue = createQueue(sourceDir, null);
             final ChronicleQueue destinationQueue = createQueue(destinationDir)) {
//            System.out.println(destinationQueue.dump());
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
                    final String s = destinationBuffer.toString();
                    assertEquals(format("Mismatch at index %s/%s was %s", Long.toHexString(sourceIndex), Long.toHexString(destinationIndex), s),
                            sourceBuffer.toString(), s.replaceAll(" - .*", ""));
                }
            }
        }
    }

    private class QueueCopier implements Runnable {

        private final Path sourceDir;
        private final Path destinationDir;
        private final int copyId;

        public QueueCopier(Path sourceDir, Path destinationDir, int copyId) {
            this.sourceDir = sourceDir;
            this.destinationDir = destinationDir;
            this.copyId = copyId;
        }

        @Override
        public void run() {
//            LOGGER.info("Starting copier...");
            try (final ChronicleQueue sourceQueue = createQueue(sourceDir, null);
                 final ChronicleQueue destinationQueue = createQueue(destinationDir, null)) {
                try (final ExcerptTailer sourceTailer = sourceQueue.createTailer();
                     final ExcerptTailer destinationTailer = destinationQueue.createTailer();
                     final ExcerptAppender destinationAppender = destinationQueue.acquireAppender()) {
                    Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
                    Bytes<?> prev = Bytes.allocateElasticOnHeap(1024);
                    long index;
                    while (true) {
                        buffer.clear();
                        index = sourceTailer.index();
                        if (!sourceTailer.readBytes(buffer)) {
                            break;
                        }
                        index = sourceTailer.lastReadIndex();

                        if (prev.contentEquals(buffer))
                            fail("duplicate " + buffer);
                        buffer.append(" - ").append(copyId);
                        ((InternalAppender) destinationAppender).writeBytes(index, buffer);
                        try (@NotNull DocumentContext dc = destinationTailer.readingDocument()) {
                            if (!dc.isPresent()) {
                                fail("no write " + buffer);
                            }
                            final long dtIndex = destinationTailer.index();
                            if (dtIndex != index)
                                assertEquals(Long.toHexString(index), Long.toHexString(dtIndex));
                        }
                        prev.clear().append(buffer);
//                        if (false && index %17 == 0) {
                        try (final ChronicleQueue dq = createQueue(destinationDir, null);
                             final ExcerptAppender da = dq.acquireAppender()) {

                        }
                        //                      }
                    }
                }
            }
//            LOGGER.info("Copier finished");
        }
    }

    private void populateSourceQueue(Path queueDir) {
        SetTimeProvider tp = new SetTimeProvider();
        Jvm.debug().on(getClass(), "Populating source queue...");
        try (final ChronicleQueue queue = createQueue(queueDir)) {
            try (final ExcerptAppender appender = queue.acquireAppender()) {
                Bytes<?> buffer = Bytes.allocateElasticOnHeap(1024);
                for (int i = 0; i < MESSAGES_TO_WRITE; i++) {
                    if (i == MESSAGES_TO_WRITE / 3 || i == 2*MESSAGES_TO_WRITE/3) {
                        Jvm.pause(1000);
//                        tp.advanceMillis(TimeUnit.SECONDS.toMillis(1));
                    }
                    buffer.clear();
                    buffer.write(messageForIndex(i));
                    appender.writeBytes(buffer);
                }
            }
        }
        Jvm.debug().on(getClass(), "Populated source queue");
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
                .rollCycle(RollCycles.TEST4_SECONDLY)
                .timeProvider(timeProvider)
                .build();
    }
}
