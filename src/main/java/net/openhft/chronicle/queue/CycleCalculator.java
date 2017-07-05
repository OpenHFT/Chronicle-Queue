package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;

public interface CycleCalculator {
    int currentCycle(final RollCycle rollCycle, final TimeProvider timeProvider, final long offsetMillis);
}