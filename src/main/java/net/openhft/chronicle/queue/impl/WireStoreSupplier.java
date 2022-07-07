/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.ParseException;
import java.util.NavigableSet;

public interface WireStoreSupplier {
    @Nullable
    SingleChronicleQueueStore acquire(int cycle, boolean createIfAbsent);

    /**
     * the next available cycle, no cycle will be created by this method, typically used by a
     * tailer.
     *
     * @param currentCycle the current cycle
     * @param direction    the direction
     * @return the next available cycle from the current cycle, or -1 if there is not a next cycle
     */
    int nextCycle(int currentCycle, TailerDirection direction) throws ParseException;

    /**
     * the cycles between a range, inclusive
     *
     * @param lowerCycle the lower cycle inclusive
     * @param upperCycle the upper cycle inclusive
     * @return the cycles between a range, inclusive
     */
    NavigableSet<Long> cycles(int lowerCycle, int upperCycle) throws ParseException;

    boolean canBeReused(@NotNull SingleChronicleQueueStore store);
}
