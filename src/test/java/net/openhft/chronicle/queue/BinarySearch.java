package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.service.TestSearch;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;

/**
 * @author Rob Austin.
 */
public enum BinarySearch {
    INSTANCE;

    public <T> long search(@NotNull SingleChronicleQueue q,
                           @NotNull Wire key,
                           @NotNull Comparator<Wire> c) throws ParseException {
        final long readPosition = key.bytes().readPosition();
        try {
            final ExcerptTailer tailer = q.createTailer();
            final long start = tailer.toStart().index();
            final long end = tailer.toEnd().index();

            final int startCycle = q.rollCycle().toCycle(start);
            final int endCycle = q.rollCycle().toCycle(end);

            if (startCycle == endCycle)
                return findWithinCycle(key, c, startCycle, tailer, q);

            final NavigableSet<Long> cycles = q.listCyclesBetween(startCycle, endCycle);


            final int cycle = (int) findCycle(q.rollCycle(), cycles, key, c, tailer);
            if (cycle == -1)
                return -1;


            tailer.direction(TailerDirection.FORWARD);

            return findWithinCycle(key, c, cycle, tailer, q);
        } finally {
            key.bytes().readPosition(readPosition);
        }
    }


    /**
     * @param rollCycle
     * @param cycles
     * @param key
     * @param c
     * @param tailer
     * @return -1 value if not found, otherwise the cycle number
     */
    private long findCycle(RollCycle rollCycle, NavigableSet cycles, Wire key, Comparator<Wire> c, ExcerptTailer tailer) {

        final long readPosition = key.bytes().readPosition();
        try {
            final SingleChronicleQueue q = ((SingleChronicleQueue) tailer.queue());
            int low = 0;
            int high = cycles.size() - 1;
            final List<Long> arrayList = new ArrayList<>(cycles);

            int mid = -1;
            Long midCycle = 1L;
            while (low <= high) {
                mid = (low + high) >>> 1;
                midCycle = arrayList.get(mid);
                final long index = rollCycle.toIndex((int) (long) midCycle, 0);


                tailer.moveToIndex(rollCycle.toIndex((int) (long) arrayList.get(low), 0));
                try (final DocumentContext dc = tailer.readingDocument()) {
                    final TestSearch.MyData myData = new TestSearch.MyData();
                    dc.wire().getValueIn().marshallable(myData);
                    System.out.println("findCycle - low=" + myData.toString());
                }


                tailer.moveToIndex(rollCycle.toIndex((int) (long) arrayList.get(high), 0));
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        System.out.println("");
                    else {
                        final TestSearch.MyData myData = new TestSearch.MyData();
                        dc.wire().getValueIn().marshallable(myData);
                        System.out.println("findCycle - high=" + myData.toString());
                    }

                }


                tailer.moveToIndex(index);
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        System.out.println("");
                    else {
                        final TestSearch.MyData myData = new TestSearch.MyData();
                        dc.wire().getValueIn().marshallable(myData);
                        System.out.println("findCycle - mid=" + myData.toString());
                    }

                }

                if (low == high) {
                    return arrayList.get(high);
                }

                if (low + 1 == high) {

                    try (DocumentContext dc = moveTo(rollCycle.toIndex((int) (long) arrayList.get(high), 0), tailer)) {
                        int cmp = c.compare(dc.wire(), key);
                        if (cmp == 0 || cmp == -1)
                            return arrayList.get(high);

                        return arrayList.get(low);
                    }
                }

                try (DocumentContext dc = moveTo(index, tailer)) {

                    key.bytes().readPosition(readPosition);
                    int cmp = c.compare(dc.wire(), key);

                    if (cmp < 0) {
                        low = mid + 1;
                    } else if (cmp > 0)
                        high = mid - 1;
                    else
                        return midCycle;

                    if (low == high)
                        return arrayList.get(low);  // key not found

                }
            }
            if (mid == -1 || low > high)
                return -1;

            return midCycle;
        } finally {
            key.bytes().readPosition(readPosition);
        }
    }


    private long findWithinCycle(Wire key, Comparator<Wire> c, int cycle, ExcerptTailer tailer, SingleChronicleQueue q) {
        final long readPosition = key.bytes().readPosition();
        try {
            long lowSeqNum = 0;

            long highSeqNum = q.exceptsPerCycle(cycle) - 1;
            if (highSeqNum == 0)
                return q.rollCycle().toIndex(cycle, 0);

            tailer.moveToIndex(q.rollCycle().toIndex(cycle, 0));
            try (final DocumentContext dc = tailer.readingDocument()) {
                final TestSearch.MyData myData = new TestSearch.MyData();
                dc.wire().getValueIn().marshallable(myData);
                System.out.println("findWithinCycle - low=" + myData.toString());
            }


            tailer.moveToIndex(q.rollCycle().toIndex(cycle, highSeqNum));
            try (final DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    System.out.println("");
                else {
                    final TestSearch.MyData myData = new TestSearch.MyData();
                    dc.wire().getValueIn().marshallable(myData);
                    System.out.println("findWithinCycle - hig=" + myData.toString());
                }

            }


            final RollCycle rollCycle = q.rollCycle();

            while (lowSeqNum <= highSeqNum) {
                long midSeqNumber = (lowSeqNum + highSeqNum) >>> 1L;
                System.out.println("lowSeqNum" + lowSeqNum + ",highSeqNum=" + highSeqNum + ",midSeqNum=" + midSeqNumber);

                final long midIndex = rollCycle.toIndex(cycle, midSeqNumber);
                try (DocumentContext dc = moveTo(midIndex, tailer)) {
                    if (!dc.isPresent())
                        return -1;
                    key.bytes().readPosition(readPosition);
                    int cmp = c.compare(dc.wire(), key);

                    if (cmp < 0)
                        lowSeqNum = midSeqNumber + 1;
                    else if (cmp > 0)
                        highSeqNum = midSeqNumber - 1;
                    else
                        return midIndex; // key found
                }

            }
            return -1;  // key not found
        } finally {
            key.bytes().readPosition(readPosition);
        }
    }


    /**
     * Gets the ith element from the given list by repositioning the specified
     * list listIterator.
     */

    private DocumentContext moveTo(long index, final ExcerptTailer tailer) {
        final boolean b = tailer.moveToIndex(index);
        assert b;
        return tailer.readingDocument();
    }


}

