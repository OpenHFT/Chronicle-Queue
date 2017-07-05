package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;

public enum DefaultCycleCalculator implements CycleCalculator {
    INSTANCE;

    @Override
    public int currentCycle(final RollCycle rollCycle, final TimeProvider timeProvider, final long offsetMillis) {
        return rollCycle.current(timeProvider, offsetMillis);
    }
}
