package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.ref.LongReference;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.io.File;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConcurrentNamedTailersTest {
    @Test
    public void concurrentNamedTailers() {
        File tmpDir = new File(OS.getTarget(), IOTools.tempName("concurrentNamedTailers"));

        final SetTimeProvider timeProvider = new SetTimeProvider("2021/12/03T12:34:56").advanceMillis(1000);
        final String tailerName = "named";
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().timeProvider(timeProvider).build();
             final ExcerptAppender appender = q.acquireAppender();
             final ExcerptTailer tailer0 = q.createTailer(tailerName);
             final ExcerptTailer tailer1 = q.createTailer(tailerName);
             final ExcerptTailer tailer2 = q.createTailer(tailerName)) {

            final Tasker tasker = appender.methodWriter(Tasker.class);
            for (int i = 0; i < 20; i++)
                tasker.task(i);

            assertEquals(0x0, tailer0.index());
            assertEquals(0x0, tailer1.index());
            assertEquals(0x0, tailer2.index());

            try (DocumentContext dc0 = tailer0.readingDocument()) {
                assertEquals(0x4a1400000000L, tailer0.index());

                try (DocumentContext dc1 = tailer1.readingDocument()) {
                    assertEquals(0x4a1400000001L, tailer1.index());
                    assertEquals(0x4a1400000000L, tailer0.index());

                    try (DocumentContext dc2 = tailer2.readingDocument()) {
                        assertEquals(0x4a1400000002L, tailer2.index());
                        assertEquals(0x4a1400000001L, tailer1.index());
                        assertEquals(0x4a1400000000L, tailer0.index());
                    }
                }
            }

            try (DocumentContext dc0 = tailer0.readingDocument()) {
                assertEquals(0x4a1400000003L, tailer0.index());

                assertTrue(tailer2.moveToIndex(0x4a140000000AL));

                try (DocumentContext dc1 = tailer1.readingDocument()) {
                    assertEquals(0x4a140000000AL, tailer1.index());
                    assertEquals(0x4a1400000003L, tailer0.index());

                    try (DocumentContext dc2 = tailer2.readingDocument()) {
                        assertEquals(0x4a140000000BL, tailer2.index());
                        assertEquals(0x4a140000000AL, tailer1.index());
                        assertEquals(0x4a1400000003L, tailer0.index());
                    }
                }
            }

            IOTools.deleteDirWithFiles(tmpDir);
        }
    }

    @Test
    public void raceConditions() throws IllegalAccessException {
        File tmpDir = new File(OS.getTarget(), IOTools.tempName("raceConditions"));

        final SetTimeProvider timeProvider = new SetTimeProvider("2021/12/03T12:34:56").advanceMillis(1000);
        final String tailerName = "named";
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().timeProvider(timeProvider).build();
             final ExcerptAppender appender = q.acquireAppender();
             final ExcerptTailer tailer0 = q.createTailer(tailerName)) {

            final Tasker tasker = appender.methodWriter(Tasker.class);
            for (int i = 0; i < 20; i++)
                tasker.task(i);

            DummyLongReference indexValue = new DummyLongReference();
            Jvm.getField(tailer0.getClass(), "indexValue")
                    .set(tailer0, indexValue);

            indexValue.getValues.add(0x4a1100000000L);

            assertEquals(0x4a1100000000L, tailer0.index());

            assertEquals(0, indexValue.setValues.size());

            indexValue.getValues.add(0x4a1100000000L);
            indexValue.getValues.add(0x4a1200000000L);
            // pretend another tailer came in
            indexValue.getValues.add(0x4a1400000001L);
            indexValue.getValues.add(0x4a1400000001L);
            indexValue.getValues.add(0x4a1400000001L);


            try (DocumentContext dc0 = tailer0.readingDocument()) {
                assertEquals(0x4a1400000001L, tailer0.index());
            }

            // changed before read
            indexValue.getValues.add(0x4a1400000003L);
            indexValue.getValues.add(0x4a1400000003L);

            // changed during read
            indexValue.getValues.add(0x4a1400000005L);
            // changed during read again
            indexValue.getValues.add(0x4a1400000007L);
            // stable
            indexValue.getValues.add(0x4a1400000007L);
            indexValue.getValues.add(0x4a1400000007L);

            try (DocumentContext dc0 = tailer0.readingDocument()) {
                assertEquals(0x4a1400000007L, tailer0.index());
            }
            assertEquals("[4a1400000002, 4a1400000003, 4a1400000007, 4a1400000007, 4a1400000008]",
                    indexValue.setValues.stream().map(Long::toHexString).collect(Collectors.toList()).toString());

            IOTools.deleteDirWithFiles(tmpDir);
        }
    }

    @Test
    public void concurrency() {
        File tmpDir = new File(OS.getTarget(), IOTools.tempName("concurrency"));

        final String tailerName = "named";
        final int numberOfEntries = 1_000_000;

        // populate the queue
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(tmpDir).testBlockSize().rollCycle(RollCycles.TEST_SECONDLY).build();
             final ExcerptAppender appender = q.acquireAppender()) {
            for (int i = 0; i < numberOfEntries; i++) {
                try (final DocumentContext documentContext = appender.writingDocument()) {
                    documentContext.wire().write("index").int64(i);
                }
            }
        }
        Jvm.startup().on(ConcurrentNamedTailersTest.class, "Wrote " + numberOfEntries + " entries");

        final int numReaders = 6;
        final AtomicLong counter = new AtomicLong(-1);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(numReaders);
        final ExecutorService executorService = Executors.newFixedThreadPool(numReaders);
        final List<Future<?>> futures = IntStream.range(0, numReaders)
                .mapToObj(i -> executorService.submit(new CompetingConsumer(tmpDir, tailerName, numberOfEntries, counter, cyclicBarrier)))
                .collect(Collectors.toList());
        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                throw new AssertionError("Error in Consumer", e);
            }
        });

        IOTools.deleteDirWithFiles(tmpDir);
    }

    private static class CompetingConsumer implements Runnable {

        private static final int LOG_EVERY = 10_000;

        private final File queueDir;
        private final String tailerName;
        private final long highestIndex;
        private final AtomicLong counter;
        private final CyclicBarrier barrier;

        public CompetingConsumer(File queueDir, String tailerName, long numberOfEntries, AtomicLong counter, CyclicBarrier barrier) {
            this.queueDir = queueDir;
            this.tailerName = tailerName;
            this.highestIndex = numberOfEntries - 1;
            this.counter = counter;
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try (ChronicleQueue q = SingleChronicleQueueBuilder.single(queueDir).testBlockSize().rollCycle(RollCycles.TEST_SECONDLY).build();
                 final ExcerptTailer namedTailer = q.createTailer(tailerName)) {

                try {
                    Jvm.startup().on(ConcurrentNamedTailersTest.class, "Waiting at barrier");
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError("Failed waiting at barrier", e);
                }
                TimingPauser pauser = Pauser.balanced();
                while (counter.get() < highestIndex) {
                    try (final DocumentContext documentContext = namedTailer.readingDocument()) {
                        final long index = documentContext.wire().read("index").int64();
                        if (index % LOG_EVERY == 0) {
                            Jvm.startup().on(ConcurrentNamedTailersTest.class, "Read index " + index);
                        }
                        while (!counter.compareAndSet(index - 1, index)) {
                            try {
                                pauser.pause(1, TimeUnit.SECONDS);
                            } catch (TimeoutException e) {
                                throw new AssertionError("Timed out trying to write " + index + " current value is " + counter.get());
                            }
                        }
                        pauser.reset();
                    }
                }
            }
        }
    }


    interface Tasker {
        void task(int taskId);
    }

    static class DummyLongReference implements LongReference {
        List<Long> getValues = new ArrayList<>();
        List<Long> setValues = new ArrayList<>();

        @Override
        public void bytesStore(BytesStore bytesStore, long offset, long length) throws IllegalStateException, IllegalArgumentException, BufferOverflowException, BufferUnderflowException {
            throw new AssertionError();
        }

        @Override
        public @Nullable BytesStore bytesStore() {
            throw new AssertionError();
        }

        @Override
        public long offset() {
            return 0;
        }

        @Override
        public long maxSize() {
            return 0;
        }

        @Override
        public long getValue() throws IllegalStateException {
            return getValues.remove(0);
        }

        @Override
        public void setValue(long value) throws IllegalStateException {
            setValues.add(value);
        }

        @Override
        public long getVolatileValue() throws IllegalStateException {
            return getValue();
        }

        @Override
        public void setVolatileValue(long value) throws IllegalStateException {
            setValue(value);
        }

        @Override
        public void setOrderedValue(long value) throws IllegalStateException {
            setValue(value);
        }

        @Override
        public long addValue(long delta) throws IllegalStateException {
            throw new AssertionError();
        }

        @Override
        public long addAtomicValue(long delta) throws IllegalStateException {
            throw new AssertionError();
        }

        @Override
        public boolean compareAndSwapValue(long expected, long value) throws IllegalStateException {
            if (getValue() == expected) {
                setValue(value);
                return true;
            }
            return false;
        }
    }
}
