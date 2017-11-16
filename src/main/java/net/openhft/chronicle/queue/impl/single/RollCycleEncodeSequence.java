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
    public void sequence(long sequence, long position) {
        if (sequenceValue != null)
            sequenceValue.setOrderedValue(toLongValue((int) sequence, position));
    }

    /**
     * This method will only return a valid sequence number of the write position id the write position is the
     * last write position in the queue. YOU CAN NOT USE THIS METHOD TO LOOK UP RANDOM SEQUENCES FOR ANY WRITE POSITION.
     * -1 will be return if a sequence number can not be found
     *
     * @param writePosition the last write position, expected to be the end of queue
     * @return -1 if the sequence for this write position can not be found
     */
    public long sequence(long writePosition) {
        if (sequenceValue == null)
            return -1;

        // todo optimize the maths in the method below
        final int lowerBitsOfWp = toLowerBitsWritePosition(toLongValue((int) writePosition, 0));
        final long sequenceValue = this.sequenceValue.getVolatileValue();

        if (lowerBitsOfWp == toLowerBitsWritePosition(sequenceValue))
            return toSequenceNumber(sequenceValue);

        return -1;
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
