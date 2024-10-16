/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.core.values.TwoLongValue;
import net.openhft.chronicle.wire.Sequence;

class RollCycleEncodeSequence implements Sequence {
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
        long value = toLongValue(position, sequence);
        writePositionAndSequence.setOrderedValue2(value);
    }

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
     * @param forWritePosition the last write position, expected to be the end of queue
     * @return NOT_FOUND_RETRY if the sequence for this write position can not be found, or NOT_FOUND if sequenceValue==null or the sequence for this {@code writePosition}
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

    private long toLongValue(long cycle, long sequenceNumber) {
        return (cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    public long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

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
