/*
 * Copyright 2016-2025 chronicle.software
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
