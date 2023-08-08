/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.function.ToIntFunction;

class TableDirectoryListing extends AbstractCloseable implements DirectoryListing {

    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle";
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";
    private static final String MOD_COUNT = "listing.modCount";
    static final int UNSET_MAX_CYCLE = Integer.MIN_VALUE;
    static final int UNSET_MIN_CYCLE = Integer.MAX_VALUE;
    static final String INITIAL_MIN_FILENAME = Character.toString(Character.MAX_VALUE);
    static final String INITIAL_MAX_FILENAME = Character.toString(Character.MIN_VALUE);
    private final TableStore<?> tableStore;
    private final Path queuePath;
    private final ToIntFunction<String> fileNameToCycleFunction;
    private volatile LongValue maxCycleValue;
    private volatile LongValue minCycleValue;
    private volatile LongValue modCount;
    private long lastRefreshTimeMS = 0;

    TableDirectoryListing(
            final @NotNull TableStore<?> tableStore,
            final Path queuePath,
            final ToIntFunction<String> fileNameToCycleFunction) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileNameToCycleFunction = fileNameToCycleFunction;

        checkReadOnly(tableStore);
        singleThreadedCheckDisabled(true);
    }

    protected void checkReadOnly(@NotNull TableStore<?> tableStore) {
        if (tableStore.readOnly()) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " should only be used for writable queues");
        }
    }

    @Override
    public void init() {
        throwExceptionIfClosedInSetter();

        tableStore.doWithExclusiveLock(ts -> {
            initLongValues();
            minCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_MIN_CYCLE);
            if (modCount.getVolatileValue() == Long.MIN_VALUE) {
                modCount.compareAndSwapValue(Long.MIN_VALUE, 0);
            }
            return this;
        });
    }

    protected void initLongValues() {
        maxCycleValue = tableStore.acquireValueFor(HIGHEST_CREATED_CYCLE);
        minCycleValue = tableStore.acquireValueFor(LOWEST_CREATED_CYCLE);
        modCount = tableStore.acquireValueFor(MOD_COUNT);
    }

    @Override
    public void refresh(final boolean force) {

        if (!force) {
            return;
        }

        lastRefreshTimeMS = System.currentTimeMillis();

        final long currentMin0 = minCycleValue.getVolatileValue();
        final long currentMax0 = maxCycleValue.getVolatileValue();

        while (true) {
            throwExceptionIfClosed();
            tableStore.throwExceptionIfClosed();
            Jvm.safepoint();
            final long currentMax = maxCycleValue.getVolatileValue();

            final String[] fileNamesList = queuePath.toFile().list();
            String minFilename = INITIAL_MIN_FILENAME;
            String maxFilename = INITIAL_MAX_FILENAME;
            if (fileNamesList != null) {
                for (String fileName : fileNamesList) {
                    if (fileName.endsWith(SingleChronicleQueue.SUFFIX)) {
                        if (minFilename.compareTo(fileName) > 0)
                            minFilename = fileName;

                        if (maxFilename.compareTo(fileName) < 0)
                            maxFilename = fileName;
                    }
                }
            }

            int min = UNSET_MIN_CYCLE;
            if (!INITIAL_MIN_FILENAME.equals(minFilename))
                min = fileNameToCycleFunction.applyAsInt(minFilename);

            int max = UNSET_MAX_CYCLE;
            if (!INITIAL_MAX_FILENAME.equals(maxFilename))
                max = fileNameToCycleFunction.applyAsInt(maxFilename);

            if (currentMin0 == min && currentMax0 == max)
                return;

            minCycleValue.setOrderedValue(min);
            if (maxCycleValue.compareAndSwapValue(currentMax, max)) {
                modCount.addAtomicValue(1);
                break;
            }
            Jvm.nanoPause();
        }
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);
    }

    @Override
    public void onRoll(int cycle) {
        minCycleValue.setMinValue(cycle);
        maxCycleValue.setMaxValue(cycle);
        modCount.addAtomicValue(1);
    }

    @Override
    public long lastRefreshTimeMS() {
        return lastRefreshTimeMS;
    }

    @Override
    public int getMaxCreatedCycle() {
        return getMaxCycleValue();
    }

    @Override
    public int getMinCreatedCycle() {
        return getMinCycleValue();
    }

    @Override
    public long modCount() {
        return modCount.getVolatileValue();
    }

    @Override
    public String toString() {
        return tableStore.dump(WireType.BINARY_LIGHT);
    }

    protected void performClose() {
        Closeable.closeQuietly(minCycleValue, maxCycleValue, modCount);
    }

    private int getMaxCycleValue() {
        return (int) maxCycleValue.getVolatileValue();
    }

    private int getMinCycleValue() {
        return (int) minCycleValue.getVolatileValue();
    }

}