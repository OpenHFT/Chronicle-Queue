package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.util.Comparator;
import java.util.Iterator;
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

        final NavigableSet<Long> longs = q.listCyclesBetween(startCycle, endCycle);

        final int cycle = findCycle(q.rollCycle(), longs, key, c, tailer);
        if (cycle == -1)
            return -1;

        final long startIndex = q.rollCycle().toIndex(cycle, 0);
        final long cycleEnd = q.rollCycle().toIndex(cycle + 1, 0);
        tailer.direction(TailerDirection.BACKWARD);
        tailer.moveToIndex(cycleEnd);
        tailer.readingDocument().close();
        final long endIndex = tailer.index();
        tailer.direction(TailerDirection.FORWARD);

        return findWithinCycle(key, c, startIndex, endIndex, tailer, q.rollCycle());

    }


    private int findCycle(RollCycle rollCycle, NavigableSet l, Wire key, Comparator<Wire> c, ExcerptTailer tailer) {
        int low = 0;
        int high = l.size() - 1;
        Iterator i = l.iterator();
        final long readPosition = key.bytes().readPosition();
        while (low <= high) {
            int mid = (low + high) >>> 1;

            final long index = rollCycle.toIndex(mid, 0);
            try (DocumentContext midVal = get(index, tailer)) {

                int cmp = c.compare(midVal.wire(), key);
                if (cmp == 0)
                    return low;
                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                if (low == high)
                    return low;
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
        return -(low + 1);  // key not found
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

