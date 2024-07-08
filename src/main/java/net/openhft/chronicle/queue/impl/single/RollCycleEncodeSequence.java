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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.core.values.TwoLongValue;

class RollCycleEncodeSequence implements StoreSequence {
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
    @SuppressWarnings("deprecation")
    public long getSequence(long forWritePosition) {

        if (writePositionAndSequence == null)
            return StoreSequence.NOT_FOUND;

        // We only deal with the 2nd long in the TwoLongValue, and we use it to keep track of current position
        // and current sequence. We use the same encoding as index (cycle number is shifted left by cycleShift
        // and sequence number occupied the lower 64-cycleShift bits) but for this use case we mask and shift
        // position into the space used for cycle number.

        // todo optimize the maths in the method below

        final long sequenceValue = this.writePositionAndSequence.getVolatileValue2();
        if (sequenceValue == 0)
            return StoreSequence.NOT_FOUND;

        long writePositionAsCycle = toLongValue(forWritePosition, 0);
        long lowerBitsOfWp = toLowerBitsWritePosition(writePositionAsCycle);
        final long toLowerBitsWritePosition = toLowerBitsWritePosition(sequenceValue);

        if (lowerBitsOfWp == toLowerBitsWritePosition)
            return toSequenceNumber(sequenceValue);

        // This should only occur if the writePosition is out of sync with the sequence.
        // This can happen if the queue is not shutdown cleanly or under heavy writes, and you don't have a lock.
        return StoreSequence.NOT_FOUND_RETRY;
    }

    private long toLongValue(long cycle, long sequenceNumber) {
        return (cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    public long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

    /**
     * Validate the sequence number for the given position. The foundSequence is the sequence number read
     * from the end of the file.
     *
     * @param foundSequence the sequence number read from the end of the file
     * @param position the position in the file
     */
    @Override
    public void validateSequence(long foundSequence, long position) {
        if (writePositionAndSequence == null)
            return;

        long sequenceValue = this.writePositionAndSequence.getVolatileValue2();
        long sequence = toSequenceNumber(sequenceValue);

        // If the sequence is less than foundSequence, then we're out of sync.
        // Attempt to update via cas. If there's multiple writers, we ignore the update.
        if (sequence < foundSequence) {
            if (this.writePositionAndSequence.compareAndSwapValue2(sequenceValue, toLongValue(position, foundSequence))) {
                Jvm.warn().on(getClass(), "Sequence number was out of sync. Updated sequence number from " + sequence + " to " + foundSequence);
            }
        }
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
