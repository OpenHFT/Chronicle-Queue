/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.core.values.TwoLongValue;
import net.openhft.chronicle.wire.Sequence;

/**
 * This class encodes and manages the sequence in a Chronicle Queue based on roll cycles.
 * It is responsible for encoding the sequence and position, and handling sequence retrieval.
 */
class RollCycleEncodeSequence implements Sequence {
    private final TwoLongValue writePositionAndSequence;
    private final int cycleShift;
    private final long sequenceMask;

    /**
     * Constructs an instance of RollCycleEncodeSequence.
     *
     * @param writePositionAndSequence The value containing both write position and sequence.
     * @param indexCount The number of indices.
     * @param indexSpacing The spacing between indices.
     */
    RollCycleEncodeSequence(LongValue writePositionAndSequence, int indexCount, int indexSpacing) {
        this.cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        this.sequenceMask = (1L << cycleShift) - 1;
        this.writePositionAndSequence = writePositionAndSequence instanceof TwoLongValue ?
                (TwoLongValue) writePositionAndSequence : null;
    }

    /**
     * Sets the sequence value and position in the underlying TwoLongValue.
     *
     * @param sequence The sequence number to set.
     * @param position The position to set.
     */
    @Override
    public void setSequence(long sequence, long position) {
        if (writePositionAndSequence == null)
            return;
        long value = toLongValue(position, sequence);
        writePositionAndSequence.setOrderedValue2(value);
    }

    /**
     * Converts the given header number and sequence into an index.
     *
     * @param headerNumber The header number.
     * @param sequence The sequence number.
     * @return The index value.
     */
    @Override
    public long toIndex(long headerNumber, long sequence) {
        long cycle = toLowerBitsWritePosition(headerNumber);
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
     * @param forWritePosition The write position, expected to be the end of the queue.
     * @return The sequence number or {@link Sequence#NOT_FOUND_RETRY} if the sequence is not found.
     */
    public long getSequence(long forWritePosition) {

        if (writePositionAndSequence == null)
            return Sequence.NOT_FOUND;

        // We only deal with the 2nd long in the TwoLongValue, and we use it to keep track of current position
        // and current sequence. We use the same encoding as index (cycle number is shifted left by cycleShift
        // and sequence number occupied the lower 64-cycleShift bits) but for this use case we mask and shift
        // position into the space used for cycle number.

        // todo optimize the maths in the method below

        final long sequenceValue = this.writePositionAndSequence.getVolatileValue2();
        if (sequenceValue == 0)
            return Sequence.NOT_FOUND;

        long writePositionAsCycle = toLongValue(forWritePosition, 0);
        long lowerBitsOfWp = toLowerBitsWritePosition(writePositionAsCycle);
        final long toLowerBitsWritePosition = toLowerBitsWritePosition(sequenceValue);

        if (lowerBitsOfWp == toLowerBitsWritePosition)
            return toSequenceNumber(sequenceValue);

        return Sequence.NOT_FOUND_RETRY;
    }

    /**
     * Combines the cycle and sequence number into a single long value.
     *
     * @param cycle The cycle number.
     * @param sequenceNumber The sequence number.
     * @return The combined long value.
     */
    private long toLongValue(long cycle, long sequenceNumber) {
        return (cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    /**
     * Extracts the sequence number from an index.
     *
     * @param index The encoded index.
     * @return The sequence number.
     */
    public long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

    /**
     * Extracts the cycle portion of the index by shifting right.
     *
     * @param index The encoded index.
     * @return The lower bits of the write position.
     */
    private long toLowerBitsWritePosition(long index) {
        return index >>> cycleShift;
    }

    @Override
    public String toString() {
        return "RollCycleEncodeSequence{" +
                "writePositionAndSequence=" + writePositionAndSequence +
                ", cycleShift=" + cycleShift +
                ", sequenceMask=" + sequenceMask +
                '}';
    }
}
