/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RollCyclesDistinctnessTest extends QueueTestCommon {

    @Test
    public void definedRollCycleFormatsAreDistinct() {
        Set<String> allPatterns = new HashSet<>();
        int count = 0;
        for (RollCycle cycle : RollCycles.all()) {
            allPatterns.add(cycle.format());
            count++;
        }
        assertEquals(allPatterns.size(), count);
    }
}
