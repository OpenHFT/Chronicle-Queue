/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.InvalidMarshallableException;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ConcurrentBinarySearchTest extends QueueTestCommon {

    /**
     * Number of items written per roll cycle before advancing time to force a new cycle.
     */
    private final RollCycle rollCycle;


    private final int numberOfSearchItems;
    private final Pauser writerPauser;

    public ConcurrentBinarySearchTest(int numberOfSearchItems, String pauserType, RollCycle rollCycle) {
        this.numberOfSearchItems = numberOfSearchItems;
        this.writerPauser = PauserMode.valueOf(pauserType).get();
        this.rollCycle = rollCycle;
    }

    @BeforeClass
    public static void init() {
        System.setProperty("disableLoopBlockMonitor", "true");
    }

    @Parameterized.Parameters(name = "searchItems={0} pauser={1} rollCycle={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {10, "balanced", TestRollCycles.TEST_SECONDLY},
                {10, "balanced", TestRollCycles.TEST_HOURLY},
                {10, "sleepy", TestRollCycles.TEST_SECONDLY},
                {10, "sleepy", TestRollCycles.TEST_HOURLY},
                {10, "busy", TestRollCycles.TEST_SECONDLY},
                {10, "busy", TestRollCycles.TEST_HOURLY},

                {40, "balanced", TestRollCycles.TEST_SECONDLY},
                {40, "balanced", TestRollCycles.TEST_HOURLY},
                {40, "sleepy", TestRollCycles.TEST_SECONDLY},
                {40, "sleepy", TestRollCycles.TEST_HOURLY},
                {40, "busy", TestRollCycles.TEST_SECONDLY},
                {40, "busy", TestRollCycles.TEST_HOURLY},
        });
    }

    @Test
    public void testConcurrentWriteAndBinarySearch() {
        final ConcurrentHashMap<Integer, Data> writtenItems = new ConcurrentHashMap<>();
        final List<Integer> endOfCycleKeys = new LinkedList<>();
        final AtomicReference<Throwable> writerError = new AtomicReference<>();
        final AtomicReference<Throwable> searcherError = new AtomicReference<>();

        final SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(System.currentTimeMillis());
        String queueDir = getTmpDir().getAbsolutePath();
        SingleChronicleQueueBuilder queueBuilder = ChronicleQueue.singleBuilder(queueDir)
                .rollCycle(rollCycle)
                .timeProvider(stp);
        WriterEventHandler writerHandler = new WriterEventHandler(queueBuilder.clone(), writtenItems, endOfCycleKeys, stp, writerError);
        final Set<Integer> searched = new HashSet<>();
        final Comparator<Wire> comparator = comparator();
        try (SingleChronicleQueue queue = queueBuilder.build();
             EventLoop writerEventLoop = EventGroup.builder().withPauser(writerPauser).withName("writer").withDaemon(true).build()) {
            writerEventLoop.addHandler(writerHandler);
            writerEventLoop.start();

            try (ExcerptTailer tailer = queue.createTailer()) {


                while (writerHandler.isRunning() && searched.size() < numberOfSearchItems) {
                    int available = writtenItems.size();
                    if (available == 0) {
                        Thread.yield();
                        continue;
                    }

                    // Prioritise end-of-cycle items to exercise cross-cycle binary search
                    int keyToSearch;

                    int endOfCycleKeyIndex = pickUnsearched(searched, endOfCycleKeys.size());
                    Integer endOfCycleKey = endOfCycleKeyIndex >= 0 ? endOfCycleKeys.remove(endOfCycleKeyIndex) : null;
                    String keyType;
                    if (endOfCycleKey != null && searched.size() % 3 == 0) {
                        keyToSearch = endOfCycleKey;
                        keyType = "end-of-cycle";
                    } else {
                        keyToSearch = pickUnsearched(searched, available);
                        if (keyToSearch < 0) {
                            Thread.yield();
                            continue;
                        }
                        keyType = "random";
                    }

                    Data data = writtenItems.get(keyToSearch);
                    if (data == null) {
                        Thread.yield();
                        continue;
                    }
                    System.out.println("Searching for " + keyType + " key (" + data.getClass().getSimpleName() + ") " + keyToSearch + " (search " + (searched.size() + 1) + "/" + numberOfSearchItems + ")");

                    Wire keyWire = toWire(data);
                    try {
                        long foundIndex = BinarySearch.search(tailer, keyWire, comparator);
                        assertEquals("Binary search failed for key=" + keyToSearch, data.writeIndex(), foundIndex);
                    } finally {
                        keyWire.bytes().releaseLast();
                    }

                    searched.add(keyToSearch);
                }
            } catch (Throwable t) {
                searcherError.set(t);
            }

            if (writerError.get() != null) {
                // which writer
                throw new AssertionError("Writer thread failed", writerError.get());
            }
            if (searcherError.get() != null) {
                throw new AssertionError("Searcher thread failed", searcherError.get());
            }

            if (searched.size() < numberOfSearchItems) {
                fail("Only searched " + searched.size() + " out of " + numberOfSearchItems + " items before writer thread finished");
            }
        }
    }

    private static <T extends Data> boolean read(Wire wire, T reusable) {
        long readPosition = wire.bytes().readPosition();
        try {
            reusable.readMarshallable(wire);
            if (!reusable.readSuccess()) {
                wire.bytes().readPosition(readPosition);
                return false;
            }
            return true;
        } catch (Exception e) {
            wire.bytes().readPosition(readPosition);
            return false;
        }
    }

    private static @NotNull Comparator<Wire> comparator() {
        final MyData reusable = new MyData();
        final MyData2 reusable2 = new MyData2();
        return (o1, o2) -> {
            Data key;
            long time2;
            int k2;
            if (read(o2, reusable)) {
                key = reusable;
                time2 = reusable.writeTimestamp;
                k2 = reusable.key;
            } else if (read(o2, reusable2)) {
                key = reusable2;
                time2 = reusable2.timestamp;
                k2 = reusable2.key;
            } else {
                throw NotComparableException.INSTANCE;
            }

            if (key instanceof MyData) {
                if (read(o1, reusable)) {
                    long time1 = reusable.writeTimestamp;
                    int k1 = reusable.key;
                    if (time1 != time2) {
                        return Long.compare(time1, time2);
                    }
                    return Integer.compare(k1, k2);
                } else {
                    throw NotComparableException.INSTANCE;
                }
            } else {
                    if (read(o1, reusable2)) {
                        long time1 = reusable2.timestamp;
                        int k1 = reusable2.key;
                        if (time1 != time2) {
                            return Long.compare(time1, time2);
                        }
                        return Integer.compare(k1, k2);
                    } else {
                        throw NotComparableException.INSTANCE;
                    }
            }

        };
    }

    /**
     * Picks a random key in [0, available) that is not in the searched set.
     * Returns -1 if all available keys have been searched.
     */
    private static int pickUnsearched(Set<Integer> searched, int available) {
        if (searched.size() >= available) {
            return -1;
        }
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int key;
        do {
            key = rng.nextInt(available);
        } while (searched.contains(key));
        return key;
    }

    @NotNull
    private static Wire toWire(Data data) {
        Wire wire = WireType.BINARY.apply(Bytes.allocateElasticOnHeap());
        wire.usePadding(true);
        data.writeMarshallable(wire);
        return wire;
    }

    public interface Data extends Marshallable {

        void write(int key, String value, long writeTimestamp, String secondValue, long writeIndex);
        long writeIndex();
        boolean readSuccess();
    }

    public static class MyData extends SelfDescribingMarshallable implements Data {
        private String clazz;
        private int key;
        private String value;
        private long writeTimestamp;
        private String secondValue;
        private long writeIndex;

        @Override
        public void write(int key, String value, long writeTimestamp, String secondValue, long writeIndex) {
            this.clazz = MyData.class.getName();
            this.key = key;
            this.value = value;
            this.writeTimestamp = writeTimestamp;
            this.secondValue = secondValue;
            this.writeIndex = writeIndex;
        }

        @Override
        public long writeIndex() {
            return writeIndex;
        }

        @Override
        public boolean readSuccess() {
            return MyData.class.getName().equals(clazz);
        }
    }

    public static class MyData2 extends SelfDescribingMarshallable implements Data {
        private String clazz;
        private long timestamp;
        private int key;
        private String description;
        private long index;

        @Override
        public void write(int key, String value, long writeTimestamp, String secondValue, long writeIndex) {
            this.clazz = MyData2.class.getName();
            this.key = key;
            this.timestamp = writeTimestamp;
            this.description = value;
            this.index = writeIndex;
        }

        @Override
        public long writeIndex() {
            return index;
        }

        @Override
        public boolean readSuccess() {
            return MyData2.class.getName().equals(clazz);
        }
    }

    public static class WriterEventHandler extends AbstractCloseable implements EventHandler {
        private final ConcurrentHashMap<Integer, Data> writtenItems;
        private final List<Integer> endOfCycleKeys;
        private final SetTimeProvider stp;
        private ExcerptAppender appender;
        private final AtomicReference<Throwable> error;
        private int key = 0;
        private long currentCycleWriteCount = 0;
        private long currentCycle;
        private final ChronicleQueue queue;
        private boolean isRunning = true;
        private long itemsPerCycle;
        private long cycleLengthInMillis;

        public WriterEventHandler(SingleChronicleQueueBuilder queueBuilder,
                                  ConcurrentHashMap<Integer, Data> writtenItems,
                                  List<Integer> endOfCycleKeys,
                                  SetTimeProvider stp,
                                  AtomicReference<Throwable> error) {
            this.queue = queueBuilder.build();
            this.writtenItems = writtenItems;
            this.endOfCycleKeys = endOfCycleKeys;
            this.stp = stp;
            this.error = error;
        }

        @Override
        public void loopStarted() {
            this.appender = queue.createAppender();
            currentCycle = appender.cycle();
            itemsPerCycle = queue.rollCycle().maxMessagesPerCycle();
            cycleLengthInMillis = queue.rollCycle().lengthInMillis();
            System.out.println("Writer started in cycle " + currentCycle + " with itemsPerCycle=" + itemsPerCycle + " (" + queue.rollCycle() + ")");
        }

        @Override
        public boolean action() throws InvalidEventHandlerException, InvalidMarshallableException {
            try {
                // Before writing, advance time to force a new cycle every ITEMS_PER_CYCLE items.
                // The previous key (key - 1) becomes the last item in the completed cycle.
                if (appender.cycle() != currentCycle) {
                    currentCycle = appender.cycle();
                    currentCycleWriteCount = 0;
                    endOfCycleKeys.add(key - 1);
                }

                if (++currentCycleWriteCount == itemsPerCycle) {
                    // advance time to go to the next cycle
                    stp.advanceMillis(cycleLengthInMillis + 1);
                    return false;
                }

                Data data = key % 2 == 0 ? new MyData() : new MyData2();

                try (DocumentContext dc = appender.writingDocument()) {
                    data.write(key, "Binary searchable value entry: " + key, stp.currentTimeMillis(), "value-" + key, dc.index());
                    data.writeMarshallable(dc.wire());
                }

                // Publish after the DocumentContext is closed (committed) so the
                // searcher never sees an item whose write has not yet been committed.
                writtenItems.put(key, data);
                key++;

                return key % 2 == 0;
            } catch (Exception e) {
                error.set(e);
                throw InvalidEventHandlerException.reusable();
            }
        }

        @Override
        public void loopFinished() {
            isRunning = false;
        }

        public boolean isRunning() {
            return isRunning;
        }

        @Override
        protected void performClose() {
            if (!queue.isClosed()) {
                Closeable.closeQuietly(appender, queue);
            }
        }
    }
}
