/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.TailerDirection;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexingToEndTest extends IndexingTestCommon {

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_noData(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index());
        tailer.toEnd();
        assertEquals(0, tailer.index());
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesSingleCycle(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index());
        long lastIndexAppended = 0;
        for (int i = 0; i < 1_000; i++) {
            appender.writeText("<test>");
            lastIndexAppended = appender.lastIndexAppended();
        }
        tailer.toEnd();
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesSingleCycle_idempotent(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        assertEquals(0, tailer.index());
        long lastIndexAppended = 0;
        for (int i = 0; i < 1_000; i++) {
            appender.writeText("<test>");
            lastIndexAppended = appender.lastIndexAppended();
        }

        for (int i = 0; i < 100; i++) {
            tailer.toEnd();
            assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
        }
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromStart_manyEntriesMultiCycle(TailerDirection tailerDirection) {
        long lastIndexAppended = populateQueue(tailerDirection);
        tailer.toEnd();
        assertEquals(2, rollCycle().toCycle(lastIndexAppended));
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
    }

    @ParameterizedTest
    @MethodSource("tailerDirections")
    void fromMiddle_manyEntriesMultiCycle(TailerDirection tailerDirection) {
        long lastIndexAppended = populateQueue(tailerDirection);
        moveToMidpoint(lastIndexAppended);
        tailer.toEnd();
        assertEquals(2, rollCycle().toCycle(lastIndexAppended));
        assertEquals(expectedIndexAfterToEnd(lastIndexAppended, tailerDirection), tailer.index());
    }

    private void moveToMidpoint(long lastIndexAppended) {
        int cycle = queue.rollCycle().toCycle(lastIndexAppended);
        int middleCycle = cycle / 2;
        long desiredIndex = queue.rollCycle().toIndex(middleCycle, 0);
        boolean moveToIndexResult = tailer.moveToIndex(desiredIndex);
        assertTrue(moveToIndexResult);
        assertEquals(desiredIndex, tailer.index());
    }

    private long populateQueue(TailerDirection tailerDirection) {
        tailer.direction(tailerDirection);
        appender.writeText("<init>");
        assertEquals(0, tailer.index());
        long lastIndexAppended = 0;
        for (int i = 0; i < 3; i++) {
            appender.writeText("<test>");
            timeProvider.advanceMillis(1001);
            lastIndexAppended = appender.lastIndexAppended();
        }
        return lastIndexAppended;
    }

    public static Stream<TailerDirection> tailerDirections() {
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
