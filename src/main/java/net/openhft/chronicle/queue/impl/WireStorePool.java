/*
 * Copyright 2016-2020 chronicle.software
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
    private final StoreFileListener storeFileListener;

    private WireStorePool(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
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
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
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

        // Reuse cycle store when applicable
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
