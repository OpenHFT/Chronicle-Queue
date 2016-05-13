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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.openhft.chronicle.queue.RollDetails;
import org.jetbrains.annotations.NotNull;

public class WireStorePool {
    @NotNull
    private final WireStoreSupplier supplier;
    @NotNull
    private final Map<RollDetails, WireStore> stores;

    private WireStorePool(@NotNull WireStoreSupplier supplier) {
        this.supplier = supplier;
        this.stores = new ConcurrentHashMap<>();
    }

    @NotNull
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier) {
        return new WireStorePool(supplier);
    }

    public void close() {
        stores.entrySet().forEach(e -> e.getValue().close());
    }

    public synchronized WireStore acquire(final int cycle, final long epoch) {
        RollDetails rollDetails = new RollDetails(cycle, epoch);
        WireStore store = stores.get(rollDetails);
        if (store == null) {
            store = this.supplier.apply(cycle, epoch);
            stores.put(rollDetails, store);
        } else {
            store.reserve();
        }
        return store;
    }

    public synchronized void release(@NotNull WireStore store) {
        store.release();
        if (store.refCount() <= 0) {
            for (Map.Entry<RollDetails, WireStore> entry : stores.entrySet()) {
                if (entry.getValue() == store) {
                    stores.remove(entry.getKey());
                    break;
                }
            }
        }
    }
}
