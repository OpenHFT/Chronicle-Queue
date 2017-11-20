package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.Sequence;


class RollCycleEncodeSequence implements Sequence {
    private final LongValue sequenceValue;
    private int cycleShift = 0;
    private long sequenceMask = 0;

    RollCycleEncodeSequence(LongValue sequenceValue, int indexCount, int indexSpacing) {
        this.sequenceValue = sequenceValue;
        cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        sequenceMask = (1L << cycleShift) - 1;
    }

    @Override
    public void setSequence(long sequence, long position) {
        if (sequenceValue == null)
            return;
        long value = toLongValue((int) position, sequence);
        sequenceValue.setOrderedValue(value);
    }

    @Override
    public long toIndex(long headerNumber, long sequence) {
        int cycle = Maths.toUInt31(headerNumber >> cycleShift);
        return toLongValue(cycle, sequence);
    }

    /**
     * gets the sequence for a writePosition
     * <p>
     * This method will only return a valid sequence number of the write position if the write position is the
     * last write position in the queue. YOU CAN NOT USE THIS METHOD TO LOOK UP RANDOM SEQUENCES FOR ANY WRITE POSITION.
     * NOT_FOUND_RETRY will be return if a sequence number can not be found  ( so can retry )
     * or NOT_FOUND when you should not retry
     *
     * @param forWritePosition the last write position, expected to be the end of queue
     * @return NOT_FOUND_RETRY if the sequence for this write position can not be found, or NOT_FOUND if sequenceValue==null or the sequence for this {@code writePosition}
     */
    public long getSequence(long forWritePosition) {

        if (sequenceValue == null)
            return Sequence.NOT_FOUND;

        // todo optimize the maths in the method below

        final long sequenceValue = this.sequenceValue.getVolatileValue();
        if (sequenceValue == 0)
            return Sequence.NOT_FOUND;

        final int lowerBitsOfWp = toLowerBitsWritePosition(toLongValue((int) forWritePosition, 0));
        final int toLowerBitsWritePosition = toLowerBitsWritePosition(sequenceValue);

        if (lowerBitsOfWp == toLowerBitsWritePosition)
            return toSequenceNumber(sequenceValue);

        return Sequence.NOT_FOUND_RETRY;
    }

    private long toLongValue(int cycle, long sequenceNumber) {
        return ((long) cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    private long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

    private int toLowerBitsWritePosition(long index) {
        return Maths.toUInt31(index >> cycleShift);
    }
}
