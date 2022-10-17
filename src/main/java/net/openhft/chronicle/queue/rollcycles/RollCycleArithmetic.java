package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.core.Maths;

import static net.openhft.chronicle.queue.RollCycle.MAX_INDEX_COUNT;

/**
 * Reusable optimised arithmetic for converting between cycles/indices/sequenceNumbers
 */
public final class RollCycleArithmetic {
    private final int cycleShift;
    private final int indexCount;
    private final int indexSpacing;
    private final long sequenceMask;

    public static RollCycleArithmetic of(int indexCount, int indexSpacing) {
        return new RollCycleArithmetic(indexCount, indexSpacing);
    }

    private RollCycleArithmetic(int indexCount, int indexSpacing) {
        this.indexCount = Maths.nextPower2(indexCount, 8);
        assert this.indexCount <= MAX_INDEX_COUNT : "indexCount: " + indexCount;
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        assert cycleShift < Long.SIZE : "cycleShift: " + cycleShift;
        sequenceMask = (1L << cycleShift) - 1;
    }

    public long maxMessagesPerCycle() {
        return Math.min(sequenceMask, ((long) indexCount * indexCount * indexSpacing));
    }

    public long toIndex(int cycle, long sequenceNumber) {
        return ((long) cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    public long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

    public int toCycle(long index) {
        return Maths.toUInt31(index >> cycleShift);
    }

    public int indexSpacing() {
        return indexSpacing;
    }

    public int indexCount() {
        return indexCount;
    }
}
