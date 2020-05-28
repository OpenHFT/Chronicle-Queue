/*
 * Copyright 2016-2020 Chronicle Software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.queue.RollDetails;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.text.ParseException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class WireStorePool extends AbstractCloseable {
    // must be power-of-two
    private static final int ROLL_CYCLE_CACHE_SIZE = 64;
    private static final int INDEX_MASK = ROLL_CYCLE_CACHE_SIZE - 1;
    @NotNull
    private final WireStoreSupplier supplier;
    @NotNull
    private final Map<RollDetails, WeakReference<SingleChronicleQueueStore>> stores;
    private final StoreFileListener storeFileListener;
    // protected by synchronized on acquire()
    private final RollDetails[] cache = new RollDetails[ROLL_CYCLE_CACHE_SIZE];

    private WireStorePool(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        this.supplier = supplier;
        this.storeFileListener = storeFileListener;
        this.stores = new ConcurrentHashMap<>();
    }

    @NotNull
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        return new WireStorePool(supplier, storeFileListener);
    }

    private static int cacheIndex(final int cycle) {
        return cycle & INDEX_MASK;
    }

    @Override
    protected void performClose() {
        stores.values().stream()
                .map(Reference::get)
                .filter(Objects::nonNull)
                .forEach(this::release);
    }

    @Nullable
    public synchronized SingleChronicleQueueStore acquire(final int cycle, final long epoch, boolean createIfAbsent) {
        throwExceptionIfClosed();
        final int cacheIndex = cacheIndex(cycle);
        RollDetails rollDetails;
        rollDetails = cache[cacheIndex];
        if (rollDetails == null || rollDetails.cycle() != cycle) {
            rollDetails = new RollDetails(cycle, epoch);
            cache[cacheIndex] = rollDetails;
        }

        WeakReference<SingleChronicleQueueStore> reference = stores.get(rollDetails);
        SingleChronicleQueueStore store;
        if (reference != null) {
            store = reference.get();
            if (store != null) {
                if (store.tryReserve())
                    return store;
                else
                    /// this should never happen,
                    // this method is synchronized
                    // and this remove below, is only any use if the acquire method below that fails
                    Jvm.warn().on(getClass(), "Logic failure - should never happen " + store);
                stores.remove(rollDetails);
            }
        }

        store = this.supplier.acquire(cycle, createIfAbsent);
        if (store != null) {
            stores.put(rollDetails, new WeakReference<>(store));
            storeFileListener.onAcquired(cycle, store.file());
        }
        return store;
    }

    public int nextCycle(final int currentCycle, @NotNull TailerDirection direction) throws ParseException {
        return supplier.nextCycle(currentCycle, direction);
    }

    public synchronized void release(@NotNull SingleChronicleQueueStore store) {

        store.release();

        long refCount = store.refCount();
        assert refCount >= 0;
        if (refCount == 0) {
            for (Map.Entry<RollDetails, WeakReference<SingleChronicleQueueStore>> entry : stores.entrySet()) {
                WeakReference<SingleChronicleQueueStore> ref = entry.getValue();
                if (ref != null && ref.get() == store) {
                    stores.remove(entry.getKey());
                    storeFileListener.onReleased(entry.getKey().cycle(), store.file());

                    store.close();
                    return;
                }
            }
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "Store was not registered: " + store.file());
        }
    }

    /**
     * list cycles between ( inclusive )
     *
     * @param lowerCycle the lower cycle
     * @param upperCycle the upper cycle
     * @return an array including these cycles and all the intermediate cycles
     */
    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) throws ParseException {
        return supplier.cycles(lowerCycle, upperCycle);
    }

    public boolean isEmpty() {
        return stores.isEmpty();
    }
}
