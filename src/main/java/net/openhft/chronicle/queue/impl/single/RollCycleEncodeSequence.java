package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.core.values.TwoLongValue;
import net.openhft.chronicle.wire.Sequence;


class RollCycleEncodeSequence implements Sequence {
    private static final long THIRTY_ONE_BITS = (1L << 31) - 1;
    private final TwoLongValue writePositionAndSequence;
    private final int cycleShift;
    private final long sequenceMask;

    RollCycleEncodeSequence(LongValue writePositionAndSequence, int indexCount, int indexSpacing) {
        this.cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        this.sequenceMask = (1L << cycleShift) - 1;
        this.writePositionAndSequence = writePositionAndSequence instanceof TwoLongValue ?
                (TwoLongValue) writePositionAndSequence : null;
    }

    @Override
    public void setSequence(long sequence, long position) {
        if (writePositionAndSequence == null)
            return;
        long value = toLongValue((int) position, sequence);
        writePositionAndSequence.setOrderedValue2(value);
    }

    @Override
    public long toIndex(long headerNumber, long sequence) {
        int cycle = toCycle(headerNumber);
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

        if (writePositionAndSequence == null)
            return Sequence.NOT_FOUND;

        // todo optimize the maths in the method below

        final long sequenceValue = this.writePositionAndSequence.getVolatileValue2();
        if (sequenceValue == 0)
            return Sequence.NOT_FOUND;

        // the below cast is safe as cycleMask always returns a number guaranteed within int range
        int writePositionCycle = (int) cycleMask(forWritePosition);
        final long lowerBitsOfWp = toLowerBitsWritePosition(toLongValue(writePositionCycle, 0));
        final long toLowerBitsWritePosition = toLowerBitsWritePosition(cycleMask(sequenceValue));

        if (lowerBitsOfWp == toLowerBitsWritePosition)
            return toSequenceNumber(sequenceValue);

        return Sequence.NOT_FOUND_RETRY;
    }

    private long cycleMask(long number) {
        return number & THIRTY_ONE_BITS;
    }

    private long toLongValue(long cycle, long sequenceNumber) {
        return (cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    public long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

    private long toLowerBitsWritePosition(long index) {
        return index >> cycleShift;
    }

    private int toCycle(long number) {
        return Maths.toUInt31(number >> cycleShift);
    }
}
