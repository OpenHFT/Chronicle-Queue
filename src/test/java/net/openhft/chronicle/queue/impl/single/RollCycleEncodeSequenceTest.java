/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ref.BinaryTwoLongReference;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.Sequence;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static net.openhft.chronicle.queue.RollCycles.DEFAULT;
import static net.openhft.chronicle.queue.rollcycles.LargeRollCycles.HUGE_DAILY;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.*;
import static org.junit.jupiter.api.Assertions.*;

class RollCycleEncodeSequenceTest extends QueueTestCommon {

    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {DAILY},
                {DEFAULT},
                {HOURLY},
                {MINUTELY},
                {HUGE_DAILY}
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void forWritePosition(RollCycle cycle) {
        BinaryTwoLongReference longValue = new BinaryTwoLongReference();
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try {
            longValue.bytesStore(bytes, 0, 16);
            RollCycleEncodeSequence rollCycleEncodeSequence = new RollCycleEncodeSequence(longValue, cycle.defaultIndexCount(), cycle.defaultIndexSpacing());
            longValue.setOrderedValue(1);
            longValue.setOrderedValue2(2);
            // a cast to int of this magic number was causing problems
            long forWritePosition = 0x8001cc54L;
            long sequence = rollCycleEncodeSequence.getSequence(forWritePosition);
            assertEquals(Sequence.NOT_FOUND_RETRY, sequence);
        } finally {
            longValue.close();
            bytes.releaseLast();
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void setGet(RollCycle cycle) {
        BinaryTwoLongReference longValue = new BinaryTwoLongReference();
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try {
            longValue.bytesStore(bytes, 0, 16);
            RollCycleEncodeSequence rollCycleEncodeSequence = new RollCycleEncodeSequence(longValue, cycle.defaultIndexCount(), cycle.defaultIndexSpacing());
            int sequenceInitial = 0xb;
            int position = 0x40284;
            rollCycleEncodeSequence.setSequence(sequenceInitial, position);
            long sequence = rollCycleEncodeSequence.getSequence(position);
            assertEquals(sequenceInitial, sequence);
        } finally {
            longValue.close();
            bytes.releaseLast();
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void setGetPositionNeedsMasking(RollCycle cycle) {
        BinaryTwoLongReference longValue = new BinaryTwoLongReference();
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try {
            longValue.bytesStore(bytes, 0, 16);
            RollCycleEncodeSequence rollCycleEncodeSequence = new RollCycleEncodeSequence(longValue, cycle.defaultIndexCount(), cycle.defaultIndexSpacing());
            int sequenceInitial = 0xb;
            long position = 0x123456789abL;
            rollCycleEncodeSequence.setSequence(sequenceInitial, position);
            long sequence = rollCycleEncodeSequence.getSequence(position);
            assertEquals(sequenceInitial, sequence);
        } finally {
            longValue.close();
            bytes.releaseLast();
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    void setGetPositionMinus1(RollCycle cycle) {
        BinaryTwoLongReference longValue = new BinaryTwoLongReference();
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try {
            longValue.bytesStore(bytes, 0, 16);
            RollCycleEncodeSequence rollCycleEncodeSequence = new RollCycleEncodeSequence(longValue, cycle.defaultIndexCount(), cycle.defaultIndexSpacing());
            int sequenceInitial = 0xb;
            long position = (1L << 48) - 1;
            rollCycleEncodeSequence.setSequence(sequenceInitial, position);
            long sequence = rollCycleEncodeSequence.getSequence(position);
            assertEquals(sequenceInitial, sequence);
        } finally {
            longValue.close();
            bytes.releaseLast();
        }
    }
}
