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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WireStorePool {
    private final WireStoreSupplier supplier;

    private class RollDetails {

        private long cycle;
        private long epoch;

        public RollDetails(long cycle, long epoc) {
            this.cycle = cycle;
            this.epoch = epoc;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RollDetails)) return false;

            RollDetails rollDetails = (RollDetails) o;

            if (cycle != rollDetails.cycle) return false;
            return epoch == rollDetails.epoch;

        }

        @Override
        public int hashCode() {
            int result = (int) (cycle ^ (cycle >>> 32));
            result = 31 * result + (int) (epoch ^ (epoch >>> 32));
            return result;
        }
    }

    private final Map<RollDetails, WireStore> stores;

    public WireStorePool(@NotNull WireStoreSupplier supplier) {
        this.supplier = supplier;
        this.stores = new ConcurrentHashMap<>();
    }

    public synchronized WireStore acquire(long cycle, final long epoch) throws IOException {
        final RollDetails rollDetails = new RollDetails(cycle, epoch);
        WireStore store = stores.get(rollDetails);
        if (store == null) {
            stores.put(rollDetails, store = this.supplier.apply(cycle, epoch));
        } else {
            store.reserve();
        }

        return store;
    }

    public synchronized void release(WireStore store) {
        store.release();
        if (store.refCount() <= 0) {
            stores.remove(new RollDetails(store.cycle(), store.epoc()));
        }
    }

    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier) {
        return new WireStorePool(supplier);
    }
}
