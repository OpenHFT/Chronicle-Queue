package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.queue.RollCycles.DAILY;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The ChronicleQueueIT class implements a test that causes Chronicle Queue to
 * fail with a BufferUnderflowException whilst executing a tailer.toEnd() call.
 */
public class MoveToWrongIndexThenToEndTest extends QueueTestCommon {

    private static final int msgSize = 64;

    private static final int numOfToEndCalls = 100;

    private static final long noIndex = 0;

    private static final RollCycle rollCycle = DAILY;
    private static final ReferenceOwner test = ReferenceOwner.temporary("test");
    private final Path basePath;
    private final SingleChronicleQueue queue;
    private final ExcerptAppender appender;
    private Bytes<ByteBuffer> outbound;

    public MoveToWrongIndexThenToEndTest() throws IOException {
        basePath = IOTools.createTempDirectory("MoveToWrongIndexThenToEndTest");
        basePath.toFile().deleteOnExit();

        queue = createChronicle(basePath);
        appender = queue.acquireAppender();
        outbound = Bytes.elasticByteBuffer();
    }

    @After
    public void after() {
        outbound.releaseLast();
        queue.close();
        DirectoryUtils.deleteDir(basePath.toFile());
    }

    private void waitFor(Semaphore semaphore, String message)
            throws InterruptedException {
        boolean ok = semaphore.tryAcquire(5, SECONDS);
        assertTrue(message, ok);
    }

    private void append() {
        outbound.clear();
        outbound.write(new byte[msgSize], 0, msgSize);
        appender.writeBytes(outbound);
    }

    @Test
    public void testBufferUnderflowException() throws InterruptedException {
        append();
        append();

        long lastIndex = getLastIndex(basePath);

        ExecutorService executor = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("executor"));
        try {
            Semaphore l0 = new Semaphore(0);
            Semaphore l1 = new Semaphore(0);
            AtomicReference<Throwable> refThrowable = new AtomicReference<>();

            executor.execute(() -> {

                try (SingleChronicleQueue chronicle = createChronicle(basePath)) {

                    ExcerptTailer tailer = chronicle.createTailer();

                    tailer.moveToIndex(lastIndex);

                    l0.release();

                    for (int i = 0; i < numOfToEndCalls; ++i) {
                        tailer.toEnd(); // BufferUnderflowException in readSkip()
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    refThrowable.set(e);
                } finally {
                    l1.release();
                }
            });

            waitFor(l0, "tailer start");

            append();
            append();

            waitFor(l1, "tailer finish");

            assertNull("refThrowable", refThrowable.get());

        } finally {
            try {
                executor.shutdown();
            } finally {
                if (!executor.isShutdown()) {
                    executor.shutdownNow();
                }
            }
        }
    }

    private long getLastIndex(Path queuePath) {
        try (SingleChronicleQueue chronicle = createChronicle(queuePath);
             StoreTailer tailer = chronicle.acquireTailer()) {

            int firstCycle = chronicle.firstCycle();
            int lastCycle = chronicle.lastCycle();

            long lastKnownIndex = noIndex;
            int numFiles = 0;

            if (firstCycle != Integer.MAX_VALUE && lastCycle != Integer.MIN_VALUE) {
                for (int cycle = firstCycle; cycle <= lastCycle; ++cycle) {
                    long lastIndex = approximateLastIndex(cycle, chronicle, tailer);
                    if (lastIndex != noIndex) {
                        lastKnownIndex = lastIndex;
                        ++numFiles;
                    }
                }
            }

            if (numFiles <= 0) {
                throw new IllegalStateException(
                        "Missing Chronicle file for path " + chronicle.fileAbsolutePath());
            }

            return lastKnownIndex;
        }
    }

    private long approximateLastIndex(int cycle, SingleChronicleQueue queue,
                                      StoreTailer tailer) {
        try (SingleChronicleQueueStore wireStore = queue.storeForCycle(cycle, queue.epoch(), false, null)) {
            if (wireStore == null) {
                return noIndex;
            }

            long baseIndex = rollCycle.toIndex(cycle, 0);

            tailer.moveToIndex(baseIndex);

            long seq = wireStore.sequenceForPosition(tailer, Long.MAX_VALUE, false);
            long sequenceNumber = seq + 1;
            long index = rollCycle.toIndex(cycle, sequenceNumber);

            int cycleOfIndex = rollCycle.toCycle(index);
            if (cycleOfIndex != cycle) {
                throw new IllegalStateException(
                        "Expected cycle " + cycle + " but got " + cycleOfIndex);
            }

            return index;
        } catch (StreamCorruptedException | UnrecoverableTimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    private SingleChronicleQueue createChronicle(Path queuePath) {
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.builder();
        builder.path(queuePath);
        builder.wireType(WireType.FIELDLESS_BINARY);
        builder.rollCycle(rollCycle);
        return builder.build();
    }

}