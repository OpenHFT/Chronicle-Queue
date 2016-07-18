/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.annotation.Nullable;
import net.openhft.chronicle.queue.RollDetails;
import net.openhft.chronicle.queue.TailerDirection;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WireStorePool {
    @NotNull
    private final WireStoreSupplier supplier;
    @NotNull
    private final Map<RollDetails, WireStore> stores;

    private final StoreFileListener storeFileListener;

    private WireStorePool(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        this.supplier = supplier;
        this.storeFileListener = storeFileListener;
        this.stores = new ConcurrentHashMap<>();
    }

    @NotNull
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        return new WireStorePool(supplier, storeFileListener);
    }

    public void close() {
        stores.entrySet().forEach(e -> e.getValue().close());
    }

    @Nullable
    public synchronized WireStore acquire(final int cycle, final long epoch, boolean createIfAbsent) {
        RollDetails rollDetails = new RollDetails(cycle, epoch);
        WireStore store = stores.get(rollDetails);
        if (store != null && store.tryReserve())
            return store;

        store = this.supplier.acquire(cycle, createIfAbsent);
        if (store != null) {
            stores.put(rollDetails, store);
            storeFileListener.onAcquired(cycle, store.file());
        }
        return store;
    }

    public int nextCycle(final int currentCycle, @NotNull TailerDirection direction) throws ParseException {
        return supplier.nextCycle(currentCycle, direction);
    }

    public synchronized void release(@NotNull WireStore store) {
        store.release();
        if (store.refCount() <= 0) {
            for (Map.Entry<RollDetails, WireStore> entry : stores.entrySet()) {
                if (entry.getValue() == store) {
                    stores.remove(entry.getKey());
                    storeFileListener.onReleased(entry.getKey().cycle(), store.file());
                    break;
                }
            }
        }
    }
}
