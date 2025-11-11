/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;

/**
 * Functional interface representing a calculator for determining the current cycle based on
 * a {@link RollCycle}, a {@link TimeProvider}, and an optional offset in milliseconds.
 * <p>
 * This interface is intended to be used for customizing the cycle calculation logic in
 * Chronicle Queue, particularly when working with different roll cycles or time-based patterns.
 */
@FunctionalInterface
public interface CycleCalculator {

    /**
     * Calculates the current cycle based on the provided {@link RollCycle}, {@link TimeProvider}, and an offset in milliseconds.
     *
     * @param rollCycle    The roll cycle that defines the periodicity of the data rolls
     * @param timeProvider The time provider that supplies the current time
     * @param offsetMillis The time offset in milliseconds, typically used for adjusting the cycle calculation
     * @return The current cycle as an integer, calculated according to the given roll cycle and time
     */
    int currentCycle(RollCycle rollCycle, TimeProvider timeProvider, long offsetMillis);
}
