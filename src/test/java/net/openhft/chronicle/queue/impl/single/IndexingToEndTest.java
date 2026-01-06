/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.TailerDirection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexingToEndTest extends IndexingTestCommon {

    @ParameterizedTest
    @DisplayName("toEnd from start keeps index at zero for empty queue")
    @MethodSource("tailerDirections")
    void fromStart_noData(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index(), "tailer should start at index 0 on empty queue");
        tailer.toEnd();
        assertEquals(0, tailer.index(), "toEnd should keep index at 0 when queue has no data");
    }

    @ParameterizedTest
    @DisplayName("toEnd from start reaches last index in single cycle")
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesSingleCycle(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index(), "tailer should start at index 0 before writing entries");
        long lastIndexAppended = 0;
        for (int i = 0; i < 1_000; i++) {
            appender.writeText("<test>");
            lastIndexAppended = appender.lastIndexAppended();
        }
        tailer.toEnd();
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index(), "toEnd should position tailer at expected index after last entry in single cycle");
    }

    @ParameterizedTest
    @DisplayName("toEnd from start is idempotent in single cycle")
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesSingleCycle_idempotent(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index(), "tailer should start at index 0 for idempotent toEnd test");
        long lastIndexAppended = 0;
        for (int i = 0; i < 1_000; i++) {
            appender.writeText("<test>");
            lastIndexAppended = appender.lastIndexAppended();
        }

        for (int i = 0; i < 100; i++) {
            tailer.toEnd();
            assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index(),
                    "repeated toEnd calls should produce consistent index position at iteration " + i);
        }
    }

    @ParameterizedTest
    @DisplayName("toEnd from start reaches last index across multiple cycles")
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesMultiCycle(TailerDirection tailerDirection) {
        long lastIndexAppended = populateQueue(tailerDirection);
        tailer.toEnd();
        assertEquals(2, rollCycle().toCycle(lastIndexAppended), "test should populate queue across 3 cycles with last entry in cycle 2");
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index(), "toEnd should position tailer at expected index after last entry across multiple cycles");
    }

    @ParameterizedTest
    @DisplayName("toEnd from middle reaches last index across multiple cycles")
    @MethodSource("tailerDirections")
    void fromMiddle_manyEntriesMultiCycle(TailerDirection tailerDirection) {
        long lastIndexAppended = populateQueue(tailerDirection);
        moveToMidpoint(lastIndexAppended);
        tailer.toEnd();
        assertEquals(2, rollCycle().toCycle(lastIndexAppended),
                "midpoint test should populate queue across 3 cycles with last entry in cycle 2");
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index(), "toEnd should position tailer at expected index when starting from middle of queue");
    }

    private void moveToMidpoint(long lastIndexAppended) {
        int cycle = queue.rollCycle().toCycle(lastIndexAppended);
        int middleCycle = cycle / 2;
        long desiredIndex = queue.rollCycle().toIndex(middleCycle, 0);
        boolean moveToIndexResult = tailer.moveToIndex(desiredIndex);
        assertTrue(moveToIndexResult, "moveToIndex should succeed when positioning to middle cycle");
        assertEquals(desiredIndex, tailer.index(), "tailer should be positioned at start of middle cycle");
    }

    private long populateQueue(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        appender.writeText("<init>");
        assertEquals(0, tailer.index(), "tailer should start at index 0 after initialization");
        long lastIndexAppended = 0;
        for (int i = 0; i < 3; i++) {
            appender.writeText("<test>");
            timeProvider.advanceMillis(1001);
            lastIndexAppended = appender.lastIndexAppended();
        }
        return lastIndexAppended;
    }

    private static Stream<TailerDirection> tailerDirections() {
        return Stream.of(TailerDirection.NONE, TailerDirection.FORWARD, TailerDirection.BACKWARD);
    }

    private long expectedIndexAfterToEnd(long lastIndexAppended, TailerDirection tailerDirection) {
        if (tailerDirection == TailerDirection.BACKWARD) {
            return lastIndexAppended;
        } else {
            return lastIndexAppended + 1;
        }
    }
}
