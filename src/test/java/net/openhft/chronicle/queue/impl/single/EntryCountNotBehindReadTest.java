package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertFalse;

public final class EntryCountNotBehindReadTest extends QueueTestCommon {
    private static final int TOTAL_EVENTS = 100_000;

    @Test
    public void testExcerptsPerCycleNotBehind() throws IOException {
        final File file = Files.createTempDirectory("exact-excerpts-per-cycle").toFile();
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(file).build()) {

            final CyclicBarrier startBarrier = new CyclicBarrier(3);
            final AtomicLong lastIndex = new AtomicLong();
            final Thread reader = new Thread
                (() -> runReader(queue, startBarrier, lastIndex::set));

            startWriter(queue, startBarrier);
            reader.start();

            waitOn(startBarrier);
            while (reader.isAlive()) {
                final long readIndex = lastIndex.get();
                if (readIndex != 0) {
                    checkExactExcerptCount(queue, readIndex);
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(file);
        }
    }

    @Test
    public void testToEndNotBehind() throws IOException {
        final File file = Files.createTempDirectory("to-end").toFile();
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(file).build()) {

            final CyclicBarrier startBarrier = new CyclicBarrier(3);
            final AtomicLong lastIndex = new AtomicLong();
            final Thread reader = new Thread
                    (() -> runReader(queue, startBarrier, lastIndex::set));

            startWriter(queue, startBarrier);
            reader.start();

            waitOn(startBarrier);
            while (reader.isAlive()) {
                final long readIndex = lastIndex.get();
                if (readIndex != 0) {
                    checkToEnd(queue, readIndex);
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(file);
        }
    }

    private void checkExactExcerptCount(SingleChronicleQueue queue, long readIndex) {
        final RollCycle cycleType = queue.rollCycle();
        final int cycle = cycleType.toCycle(readIndex);
        final long readCount = cycleType.toSequenceNumber(readIndex) + 1;
        final long excerptCount = queue.exactExcerptsInCycle(cycle);
        assertFalse(readCount > excerptCount);
    }


    private void checkToEnd(SingleChronicleQueue queue, long readIndex) {
        final RollCycle cycleType = queue.rollCycle();
        final int cycle = cycleType.toCycle(readIndex);
        final long readCount = cycleType.toSequenceNumber(readIndex) + 1;
        long excerptCount = 0;
        try (ExcerptTailer tailer = queue.createTailer()) {
            if (tailer.moveToCycle(cycle)) {
                excerptCount = cycleType.toSequenceNumber(tailer.toEnd().index());
            }
        }
        assertFalse(readCount > excerptCount);
    }

    private void startWriter(SingleChronicleQueue queue, CyclicBarrier startBarrier) {
        new Thread(() -> {
            waitOn(startBarrier);
            for (int i = 0; i < TOTAL_EVENTS; ++i) {
                queue.acquireAppender().writingDocument().close();
            }
        }).start();
    }

    private void runReader(SingleChronicleQueue queue, CyclicBarrier startBarrier, LongConsumer onRead) {
        try (final ExcerptTailer tailer = queue.createTailer()) {
            waitOn(startBarrier);
            int count = 0;
            while (count < TOTAL_EVENTS) {
                try (DocumentContext entry = tailer.readingDocument()) {
                    if (entry.isData() && !entry.isNotComplete()) {
                        onRead.accept(entry.index());
                        ++count;
                    }
                }
            }
        }
    }

    private static void waitOn(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new IllegalStateException(e);
        }
    }
}
