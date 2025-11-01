/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.RollCyclesTest;
import org.junit.Test;

import static java.lang.String.format;

public class RollCyclesAsciiDocGeneratingTest extends QueueTestCommon {
    /**
     * This generates the asciidoc for the table in /docs/FAQ.adoc
     */
    @Test
    public void dumpAllRollCycles() {
        StringBuilder stringBuilder = new StringBuilder().append("\n\n");
        for (RollCycle cycle : RollCycles.all()) {
            stringBuilder.append(format("| %s | %,d | `0x%x` | %,d%n",
                    cycle.getClass().getSimpleName() + "." + ((Enum<?>) cycle).name(),
                    cycle.maxMessagesPerCycle(),
                    cycle.maxMessagesPerCycle(),
                    cycle.maxMessagesPerCycle() / (cycle.lengthInMillis() / 1000)));
        }
        Jvm.startup().on(RollCyclesTest.class, stringBuilder.toString());
    }
}
