/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.ParseException;
import java.util.NavigableSet;

/**
 * The {@code WireStorePool} class is responsible for managing a pool of {@link SingleChronicleQueueStore} instances.
 * It acquires and releases stores as needed, ensuring they are reused when applicable. The class also handles
 * interaction with {@link StoreFileListener} to notify when a store is acquired or released.
 */
public class WireStorePool extends SimpleCloseable {
    @NotNull
    private final WireStoreSupplier supplier;
    @NotNull
    private final StoreFileListener storeFileListener;

    private WireStorePool(@NotNull WireStoreSupplier supplier, @NotNull StoreFileListener storeFileListener) {
        this.supplier = supplier;
        this.storeFileListener = storeFileListener;
    }

    /**
     * Creates a new {@code WireStorePool} instance using the provided supplier and store file listener.
     *
     * @param supplier            the supplier used to acquire stores
     * @param storeFileListener   the listener notified of file acquisitions and releases
     * @return a new {@code WireStorePool} instance
     */
    @NotNull
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier, @NotNull StoreFileListener storeFileListener) {
        return new WireStorePool(supplier, storeFileListener);
    }

    /**
     * Acquires a store for the given cycle. Reuses an existing store if available, or acquires a new one using
     * the supplied create strategy.
     *
     * @param cycle          the cycle number to acquire the store for
     * @param createStrategy the strategy used to create a new store if needed
     * @param oldStore       the previous store, if available for reuse
     * @return the acquired store, or null if acquisition fails
     */
    @Nullable
    public SingleChronicleQueueStore acquire(
            final int cycle,
            WireStoreSupplier.CreateStrategy createStrategy,
            SingleChronicleQueueStore oldStore) {
        throwExceptionIfClosed();

        // reuse cycle store when applicable
        if (oldStore != null && oldStore.cycle() == cycle && !oldStore.isClosed())
            return oldStore;

        SingleChronicleQueueStore store = this.supplier.acquire(cycle, createStrategy);
        if (store != null) {
            store.cycle(cycle);
            if (store != oldStore && storeFileListener.isActive())
                BackgroundResourceReleaser.run(() -> storeFileListener.onAcquired(cycle, store.file()));
        }
        return store;
    }

    /**
     * Determines the next available cycle, based on the provided current cycle and direction.
     *
     * @param currentCycle the current cycle
     * @param direction    the direction (FORWARD or BACKWARD)
     * @return the next cycle number
     * @throws ParseException if parsing the cycle fails
     */
    public int nextCycle(final int currentCycle, @NotNull TailerDirection direction) throws ParseException {
        throwExceptionIfClosed();
        return supplier.nextCycle(currentCycle, direction);
    }

    /**
     * Closes the specified store and notifies the {@link StoreFileListener} if active.
     *
     * @param store the store to close
     */
    public void closeStore(@NotNull SingleChronicleQueueStore store) {
        BackgroundResourceReleaser.release(store);
        if (storeFileListener.isActive())
            BackgroundResourceReleaser.run(() -> storeFileListener.onReleased(store.cycle(), store.file()));
    }

    /**
     * Lists cycles between the given lower and upper cycle, inclusive.
     *
     * @param lowerCycle the lower cycle number
     * @param upperCycle the upper cycle number
     * @return a {@link NavigableSet} of cycle numbers between the given range
     */
    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) {
        throwExceptionIfClosed();

        return supplier.cycles(lowerCycle, upperCycle);
    }
}
