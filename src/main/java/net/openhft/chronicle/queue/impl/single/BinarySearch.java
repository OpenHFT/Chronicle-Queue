package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
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

    /**
     * returns the index or -1 if not found or the index if an exact match is found, an approximation in the form of -approximateIndex
     * or -1 if there was no searching to be done.
     * <p>
     * Warning : This implementation is unreliable as index are an encoded 64bits, where we could use all the bits including the
     * high bit which is used for the sign. At the moment  it will work as its unlikely to reach a point where we store
     * enough messages in the chronicle queue to use the high bit, having said this its possible in the future the the
     * high bit in the index ( used for the sign ) may be used, this implementation is unsafe as it relies on this
     * bit not being set ( in other words set to zero ).
     */
    public static long search(@NotNull SingleChronicleQueue q,
                              @NotNull Wire key,
                              @NotNull Comparator<Wire> c) throws ParseException {
        final long readPosition = key.bytes().readPosition();
        try {
            final ExcerptTailer tailer = q.createTailer();
            final long start = tailer.toStart().index();
            final long end = tailer.toEnd().index();

            final RollCycle rollCycle = q.rollCycle();
            final int startCycle = rollCycle.toCycle(start);
            final int endCycle = rollCycle.toCycle(end);

            if (startCycle == endCycle)
                return findWithinCycle(key, c, startCycle, tailer, q, rollCycle);

            final NavigableSet<Long> cycles = q.listCyclesBetween(startCycle, endCycle);
            final int cycle = (int) findCycleLinearSearch(cycles, key, c, tailer, q);

            if (cycle == -1)
                return -1;
            return findWithinCycle(key, c, cycle, tailer, q, rollCycle);
        } finally {
            key.bytes().readPosition(readPosition);
        }

    }

    private static long findCycleLinearSearch(NavigableSet<Long> cycles, Wire key,
                                              Comparator<Wire> c,
                                              ExcerptTailer tailer,
                                              final SingleChronicleQueue queue) {

        final Iterator<Long> iterator = cycles.iterator();
        if (!iterator.hasNext())
            return -1;
        final RollCycle rollCycle = queue.rollCycle();
        long prevIndex = iterator.next();

        while (iterator.hasNext()) {

            final Long current = iterator.next();

            final boolean b = tailer.moveToIndex(rollCycle.toIndex((int) (long) current, 0));
            if (!b)
                return prevIndex;

            try (final DocumentContext dc = tailer.readingDocument()) {
                final int compare = c.compare(dc.wire(), key);
                if (compare == 0)
                    return current;
                else if (compare == 1)
                    return prevIndex;
                prevIndex = current;
            }
        }

        return prevIndex;
    }


    /**
     * @return The index if an exact match is found, an approximation in the form of -approximateIndex
     * or -1 if there was no searching to be done.
     * <p>
     * Warning : This implementation is unreliable as index are an encoded 64bits, where we could use all the bits including the
     * high bit which is used for the sign. At the moment  it will work as its unlikely to reach a point where we store
     * enough messages in the chronicle queue to use the high bit, having said this its possible in the future the the
     * high bit in the index ( used for the sign ) may be used, this implementation is unsafe as it relies on this
     * bit not being set ( in other words set to zero ).
     */
    public static long findWithinCycle(Wire key,
                                       Comparator<Wire> c,
                                       int cycle,
                                       ExcerptTailer tailer,
                                       SingleChronicleQueue q,
                                       final RollCycle rollCycle) {
        final long readPosition = key.bytes().readPosition();
        try {
            long lowSeqNum = 0;

            long highSeqNum = q.exceptsPerCycle(cycle) - 1;
            if (highSeqNum == 0)
                return rollCycle.toIndex(cycle, 0);

            // nothing to search
            if (highSeqNum < lowSeqNum)
                return -1;

            long midIndex = 0;

            while (lowSeqNum <= highSeqNum) {
                long midSeqNumber = (lowSeqNum + highSeqNum) >>> 1L;

                midIndex = rollCycle.toIndex(cycle, midSeqNumber);

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

            return -midIndex;  // -approximateIndex
        } finally {
            key.bytes().readPosition(readPosition);
        }
    }

    /**
     * moves to index
     */
    private static DocumentContext moveTo(long index, final ExcerptTailer tailer) {
        final boolean b = tailer.moveToIndex(index);
        assert b;
        return tailer.readingDocument();
    }

}
