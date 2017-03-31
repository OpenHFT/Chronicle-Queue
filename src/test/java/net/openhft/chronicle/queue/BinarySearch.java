package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.NavigableSet;

/**
 * @author Rob Austin.
 */
public enum BinarySearch {
    INSTANCE;

    public <T> long search(@NotNull SingleChronicleQueue q,
                           @NotNull Wire key,
                           @NotNull Comparator<Wire> c) throws ParseException {

        final ExcerptTailer tailer = q.createTailer();
        final long start = tailer.toStart().index();
        final long end = tailer.toEnd().index();

        final int startCycle = q.rollCycle().toCycle(start);
        final int endCycle = q.rollCycle().toCycle(end);

        if (startCycle == endCycle) {
            return findWithinCycle(key, c, start, end, tailer, q.rollCycle());
        }

        final NavigableSet<Long> cycles = q.listCyclesBetween(startCycle, endCycle);

        final int cycle = (int) findCycle(q.rollCycle(), cycles, key, c, tailer);
        if (cycle == -1)
            return -1;

        final long count = q.exceptsPerCycle(cycle);

        final long startIndex = q.rollCycle().toIndex(cycle, 0);
        final long endIndex = q.rollCycle().toIndex(cycle, count - 1);


        System.out.print(q.rollCycle().toCycle(endIndex));

        tailer.direction(TailerDirection.FORWARD);

        return findWithinCycle(key, c, startIndex, endIndex, tailer, q.rollCycle());

    }


    private long findCycle(RollCycle rollCycle, NavigableSet cycles, Wire key, Comparator<Wire> c, ExcerptTailer tailer) {
        int low = 0;
        int high = cycles.size() - 1;
        final ArrayList<Long> arrayList = new ArrayList<>(cycles);
        final long readPosition = key.bytes().readPosition();
        while (low <= high) {
            int mid = (low + high) >>> 1;
            final Long midCycle = arrayList.get(mid);
            final int midCycle1 = (int) (long) midCycle;
            final long index = rollCycle.toIndex(midCycle1, 0);
            try (DocumentContext midVal = get(index, tailer)) {

                int cmp = c.compare(midVal.wire(), key);
                if (cmp == 0 && mid == high)
                    return arrayList.get(high);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else if (low == high - 1)
                    return arrayList.get(low);

            } finally {
                key.bytes().readPosition(readPosition);
            }
        }
        return -1;  // key not found
    }


    private <T> long findWithinCycle(Wire key, Comparator<Wire> c, long low, long high, ExcerptTailer tailer, final RollCycle rollCycle) {

        final long readPosition = key.bytes().readPosition();
        while (low <= high) {
            long mid = (low + high) >>> 1L;
            System.out.println("low" + rollCycle.toSequenceNumber(low) + ",high" + rollCycle.toSequenceNumber(high) + ",mid" + rollCycle.toSequenceNumber(mid));
            try (DocumentContext dc = get(mid, tailer)) {
                if (!dc.isPresent())
                    return -1;
                int cmp = c.compare(dc.wire(), key);


                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid; // key found
            } finally {
                key.bytes().readPosition(readPosition);
            }

        }
        return -1;  // key not found
    }


    /**
     * Gets the ith element from the given list by repositioning the specified
     * list listIterator.
     */
    private DocumentContext get(long index, final ExcerptTailer tailer) {
        tailer.moveToIndex(index);
        return tailer.readingDocument();
    }


}

