/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;

/**
 * Singleton enum implementation of {@link CycleCalculator} that provides a default mechanism
 * for calculating the current cycle based on the provided {@link RollCycle} and {@link TimeProvider}.
 * <p>
 * This enum ensures there is only one instance of the cycle calculator, represented by {@code INSTANCE}.
 */
public enum DefaultCycleCalculator implements CycleCalculator {
    /**
     * The single instance of the {@code DefaultCycleCalculator}.
     */
    INSTANCE;

    /**
     * Calculates the current cycle by delegating to the provided {@link RollCycle}.
     * Uses the {@link TimeProvider} and an optional offset in milliseconds to determine the current cycle.
     *
     * @param rollCycle     The roll cycle that defines the periodicity of the data rolls
     * @param timeProvider  The time provider that supplies the current time
     * @param offsetMillis  The time offset in milliseconds, typically used for adjusting the cycle calculation
     * @return The current cycle as an integer, calculated according to the given roll cycle and time
     */
    @Override
    public int currentCycle(final RollCycle rollCycle, final TimeProvider timeProvider, final long offsetMillis) {
        return rollCycle.current(timeProvider, offsetMillis);
    }
}
