/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.ParseException;
import java.util.NavigableSet;

/**
 * The {@code WireStoreSupplier} interface defines methods for acquiring and managing {@link SingleChronicleQueueStore}
 * instances for a specific cycle. It also provides functionality to navigate between cycles and determine which
 * stores can be reused.
 */
public interface WireStoreSupplier {

    /**
     * Enum defining the strategy for creating or initializing a store.
     */
    enum CreateStrategy {
        /** Always create a new file. */
        CREATE,

        /** Reinitialize existing file if the header is not ready, used for normalizing EOF. */
        REINITIALIZE_EXISTING,

        /** Open the file in read-only mode. */
        READ_ONLY
    }

    /**
     * Acquires a {@link SingleChronicleQueueStore} for the specified cycle, using the given creation strategy.
     *
     * @param cycle          the cycle number to acquire the store for
     * @param createStrategy the strategy to use for creating or initializing the store
     * @return the acquired store, or {@code null} if acquisition fails
     */
    @Nullable
    SingleChronicleQueueStore acquire(int cycle, CreateStrategy createStrategy);

    /**
     * Determines the next available cycle, without creating new cycles.
     * This method is typically used by a tailer to move to the next cycle.
     *
     * @param currentCycle the current cycle
     * @param direction    the direction (FORWARD or BACKWARD)
     * @return the next available cycle, or -1 if no next cycle exists
     * @throws ParseException if parsing the cycle fails
     */
    int nextCycle(int currentCycle, TailerDirection direction) throws ParseException;

    /**
     * Retrieves a set of cycle numbers within the specified range, inclusive of both the lower and upper cycles.
     *
     * @param lowerCycle the lower bound of the cycle range (inclusive)
     * @param upperCycle the upper bound of the cycle range (inclusive)
     * @return a {@link NavigableSet} containing the cycle numbers within the specified range
     */
    NavigableSet<Long> cycles(int lowerCycle, int upperCycle);

    /**
     * Checks if the specified store can be reused for another cycle.
     *
     * @param store the {@link SingleChronicleQueueStore} to check for reuse
     * @return {@code true} if the store can be reused, {@code false} otherwise
     */
    boolean canBeReused(@NotNull SingleChronicleQueueStore store);
}
