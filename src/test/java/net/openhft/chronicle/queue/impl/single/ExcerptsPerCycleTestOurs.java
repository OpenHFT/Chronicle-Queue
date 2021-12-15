package net.openhft.chronicle.queue.impl.single;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.file.Files;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

public final class ExcerptsPerCycleTestOurs {

    private static final int TOTAL_EVENTS = 200_000;

    @Test
    public void testExcerptsPerCycleIsAhead() throws IOException {

        final File file = Files.createTempDirectory("queue").toFile();
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
                    checkExcerptCount(queue, readIndex);
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(file);
        }
    }

    private void startWriter
            (SingleChronicleQueue queue, CyclicBarrier startBarrier) {

        new Thread(() -> {
            waitOn(startBarrier);
            for (int i = 0; i < TOTAL_EVENTS; ++i) {
                queue.acquireAppender().writingDocument().close();
            }
        }).start();
    }

    private void runReader
            (SingleChronicleQueue queue, CyclicBarrier startBarrier,
             LongConsumer onRead) {

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

    private void checkExcerptCount
            (SingleChronicleQueue queue, long readIndex) throws StreamCorruptedException {

        final RollCycle cycleType = queue.rollCycle();
        final int cycle = cycleType.toCycle(readIndex);
        final long readCount = cycleType.toSequenceNumber(readIndex) + 1;
        long excerptCount = 0;//queue.exceptsPerCycle(cycle);
        //ExcerptTailer tailer = queue.createTailer();
        StoreTailer tailer = queue.acquireTailer();
        if (tailer.moveToCycle(cycle))
            excerptCount = //cycleType.toSequenceNumber(tailer.toEnd().index()) + 1;
                            //tailer.store.sequenceForPosition(tailer, Long.MAX_VALUE, false) + 1;
                            tailer.store.lastSequenceNumber(tailer) + 1;
        if (readCount > excerptCount) {
            throw new AssertionError(
                "excerptsPerCycle of " + excerptCount +
                " is less than read count of " + readCount + " delta " + (readCount - excerptCount) +
                " in cycle " + readIndex + " of " + queue.file());
            //System.exit(0);
        } else {
            //System.out.println(
            //        ("excerptsPerCycle of " + excerptCount +
            //                " is not less than read count of " + readCount));

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
