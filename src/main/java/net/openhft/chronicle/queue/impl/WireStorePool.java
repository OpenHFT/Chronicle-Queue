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

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.ParseException;
import java.util.NavigableSet;

public class WireStorePool extends SimpleCloseable {
    @NotNull
    private final WireStoreSupplier supplier;
    private final StoreFileListener storeFileListener;

    private WireStorePool(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        this.supplier = supplier;
        this.storeFileListener = storeFileListener;
    }

    @NotNull
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        return new WireStorePool(supplier, storeFileListener);
    }

    @Nullable
    public SingleChronicleQueueStore acquire(
            final int cycle,
            final long epoch,
            boolean createIfAbsent,
            SingleChronicleQueueStore oldStore) {
        throwExceptionIfClosed();

        SingleChronicleQueueStore store = this.supplier.acquire(cycle, createIfAbsent);
        if (store != null) {
            if (store != oldStore) {
                if (storeFileListener.isActive())
                    BackgroundResourceReleaser.run(() -> storeFileListener.onAcquired(cycle, store.file()));
                store.cycle(cycle);
            }
        }
        return store;
    }

    public int nextCycle(final int currentCycle, @NotNull TailerDirection direction) throws ParseException {
        throwExceptionIfClosed();

        return supplier.nextCycle(currentCycle, direction);
    }

    public void closeStore(@NotNull SingleChronicleQueueStore store) {
        BackgroundResourceReleaser.release(store);
        if (storeFileListener.isActive())
            BackgroundResourceReleaser.run(() -> storeFileListener.onReleased(store.cycle(), store.file()));
    }

    /**
     * list cycles between ( inclusive )
     *
     * @param lowerCycle the lower cycle
     * @param upperCycle the upper cycle
     * @return an array including these cycles and all the intermediate cycles
     */
    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) throws ParseException {
        throwExceptionIfClosed();

        return supplier.cycles(lowerCycle, upperCycle);
    }
}
