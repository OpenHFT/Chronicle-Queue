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

/**
 * TableDirectoryListing manages the cycle metadata for a Chronicle Queue stored in a table.
 * This class is responsible for keeping track of the minimum and maximum cycle numbers created in the queue.
 * It ensures that the cycle information is properly synchronized and updated, allowing for the detection of new files
 * and handling the queue's directory listing.
 */
class TableDirectoryListing extends AbstractCloseable implements DirectoryListing {

    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle"; // Key for storing the highest created cycle
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";   // Key for storing the lowest created cycle
    private static final String MOD_COUNT = "listing.modCount";                 // Key for storing the modification count
    static final int UNSET_MAX_CYCLE = Integer.MIN_VALUE;                       // Constant for unset maximum cycle value
    static final int UNSET_MIN_CYCLE = Integer.MAX_VALUE;                       // Constant for unset minimum cycle value
    static final String INITIAL_MIN_FILENAME = Character.toString(Character.MAX_VALUE); // Initial minimum filename value
    static final String INITIAL_MAX_FILENAME = Character.toString(Character.MIN_VALUE); // Initial maximum filename value

    private final TableStore<?> tableStore;              // The table store that holds cycle metadata
    private final Path queuePath;                        // The path to the Chronicle Queue directory
    private final ToIntFunction<String> fileNameToCycleFunction; // Function to convert filenames to cycle numbers
    private volatile LongValue maxCycleValue;            // Stores the maximum cycle number
    private volatile LongValue minCycleValue;            // Stores the minimum cycle number
    private volatile LongValue modCount;                 // Tracks the modification count
    private long lastRefreshTimeMS = 0;                  // Last time the directory listing was refreshed

    /**
     * Constructs a new TableDirectoryListing with the specified table store, queue path, and filename to cycle function.
     *
     * @param tableStore The table store that holds the cycle metadata.
     * @param queuePath The path to the Chronicle Queue directory.
     * @param fileNameToCycleFunction Function to convert filenames to cycle numbers.
     */
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

    /**
     * Ensures that this listing is only used for writable queues. Throws an exception if the table store is read-only.
     *
     * @param tableStore The table store to check.
     */
    protected void checkReadOnly(@NotNull TableStore<?> tableStore) {
        if (tableStore.readOnly()) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " should only be used for writable queues");
        }
    }

    /**
     * Initializes the directory listing by acquiring values from the table store.
     */
    @Override
    public void init() {
        throwExceptionIfClosedInSetter();

        tableStore.doWithExclusiveLock(ts -> {
            initLongValues();  // Initialize the LongValue fields
            minCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_MIN_CYCLE);
            if (modCount.getVolatileValue() == Long.MIN_VALUE) {
                modCount.compareAndSwapValue(Long.MIN_VALUE, 0);
            }
            return this;
        });
    }

    /**
     * Acquires the necessary LongValues (maxCycle, minCycle, modCount) from the table store.
     */
    protected void initLongValues() {
        maxCycleValue = tableStore.acquireValueFor(HIGHEST_CREATED_CYCLE);
        minCycleValue = tableStore.acquireValueFor(LOWEST_CREATED_CYCLE);
        modCount = tableStore.acquireValueFor(MOD_COUNT);
    }

    /**
     * Refreshes the directory listing, updating the cycle values if needed. Only refreshes if the force flag is set.
     *
     * @param force Whether to force a refresh of the directory listing.
     */
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

            // Scan directory for cycle files and update min/max filenames
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

            // Convert the min and max filenames to cycle numbers
            int min = UNSET_MIN_CYCLE;
            if (!INITIAL_MIN_FILENAME.equals(minFilename))
                min = fileNameToCycleFunction.applyAsInt(minFilename);

            int max = UNSET_MAX_CYCLE;
            if (!INITIAL_MAX_FILENAME.equals(maxFilename))
                max = fileNameToCycleFunction.applyAsInt(maxFilename);

            // Update cycle values if they have changed
            if (currentMin0 == min && currentMax0 == max) {
                modCount.addAtomicValue(1);
                return;
            }

            minCycleValue.setOrderedValue(min);
            if (maxCycleValue.compareAndSwapValue(currentMax, max)) {
                modCount.addAtomicValue(1);
                break;
            }
            Jvm.nanoPause();
        }
    }

    /**
     * Handles the creation of a new file by updating the cycle metadata.
     *
     * @param file  The file that was created.
     * @param cycle The cycle associated with the file.
     */
    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);
    }

    /**
     * Updates the minimum and maximum cycle values when the queue rolls to a new cycle.
     *
     * @param cycle The new cycle number.
     */
    @Override
    public void onRoll(int cycle) {
        minCycleValue.setMinValue(cycle);
        maxCycleValue.setMaxValue(cycle);
        modCount.addAtomicValue(1);
    }

    /**
     * Returns the timestamp of the last directory listing refresh.
     *
     * @return The last refresh time in milliseconds.
     */
    @Override
    public long lastRefreshTimeMS() {
        return lastRefreshTimeMS;
    }

    /**
     * Returns the highest cycle number created in the queue.
     *
     * @return The highest created cycle.
     */
    @Override
    public int getMaxCreatedCycle() {
        return getMaxCycleValue();
    }

    /**
     * Returns the lowest cycle number created in the queue.
     *
     * @return The lowest created cycle.
     */
    @Override
    public int getMinCreatedCycle() {
        return getMinCycleValue();
    }

    /**
     * Returns the modification count, representing how many times the directory listing has been modified.
     *
     * @return The modification count.
     */
    @Override
    public long modCount() {
        return modCount.getVolatileValue();
    }

    /**
     * Provides a string representation of the table store's content in binary format.
     *
     * @return A string representing the table store's content.
     */
    @Override
    public String toString() {
        return tableStore.dump(WireType.BINARY_LIGHT);
    }

    /**
     * Closes the directory listing by releasing resources associated with the LongValues.
     */
    protected void performClose() {
        Closeable.closeQuietly(minCycleValue, maxCycleValue, modCount);
    }

    /**
     * Returns the volatile value of the maximum cycle.
     *
     * @return The maximum cycle value.
     */
    private int getMaxCycleValue() {
        return (int) maxCycleValue.getVolatileValue();
    }

    /**
     * Returns the volatile value of the minimum cycle.
     *
     * @return The minimum cycle value.
     */
    private int getMinCycleValue() {
        return (int) minCycleValue.getVolatileValue();
    }
}
