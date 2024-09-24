/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        return rollCycle.current(timeProvider, offsetMillis); // Delegate the cycle calculation to RollCycle
    }
}
