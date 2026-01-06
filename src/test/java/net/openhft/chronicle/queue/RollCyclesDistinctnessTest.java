/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RollCyclesDistinctnessTest extends QueueTestCommon {

    @Test
    @DisplayName("Roll cycle format strings are distinct")
    public void definedRollCycleFormatsAreDistinct() {
        Set<String> allPatterns = new HashSet<>();
        int count = 0;
        for (RollCycle cycle : RollCycles.all()) {
            allPatterns.add(cycle.format());
            count++;
        }
        assertEquals(allPatterns.size(), count, "rollCycle formats should be distinct");
    }
}
