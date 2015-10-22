/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;

public class WireStorePool {
    private final WireStoreSupplier supplier;
    private final Map<Long, WireStore> stores;

    public WireStorePool(@NotNull WireStoreSupplier supplier) {
        this.supplier = supplier;
        this.stores = HashLongObjMaps.newMutableMap();
    }

    public synchronized WireStore acquire(long cycle) throws IOException {
        WireStore store = stores.get(cycle);
        if(store == null) {
            stores.put(cycle, store = this.supplier.apply(cycle));
        } else {
            store.reserve();
        }

        return store;
    }

    public synchronized void release(WireStore store) {
        store.release();
        if(store.refCount() <= 0) {
            stores.remove(store.cycle());
        }
    }

    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier) {
        return new WireStorePool(supplier);
    }
}
