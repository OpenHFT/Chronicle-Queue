/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.TimeProvider;
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
    private final TimeProvider time;
    private volatile LongValue maxCycleValue;
    private volatile LongValue minCycleValue;
    private volatile LongValue modCount;
    private volatile int maxCreatedCycle = UNSET_MAX_CYCLE;
    private volatile int minCreatedCycle = UNSET_MIN_CYCLE;
    private volatile long lastSeenModCount;
    private long lastRefreshTimeMS = 0;

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
            final ToIntFunction<String> fileNameToCycleFunction,
            final TimeProvider time) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileNameToCycleFunction = fileNameToCycleFunction;
        this.time = time;

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
            initLongValues();
            maxCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_MAX_CYCLE);
            minCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_MIN_CYCLE);
            if (modCount.getVolatileValue() == Long.MIN_VALUE) {
                modCount.compareAndSwapValue(Long.MIN_VALUE, 0);
            }
            lastSeenModCount = modCount.getVolatileValue();
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
            refreshPublishedBounds();
            return;
        }

        lastRefreshTimeMS = time.currentTimeMillis();

        tableStore.doWithExclusiveLock(ignored -> {
            throwExceptionIfClosed();
            tableStore.throwExceptionIfClosed();

            final String[] fileNamesList = queuePath.toFile().list();
            if (fileNamesList == null)
                return null;

            String minFilename = INITIAL_MIN_FILENAME;
            String maxFilename = INITIAL_MAX_FILENAME;
            for (String fileName : fileNamesList) {
                if (fileName.endsWith(SingleChronicleQueue.SUFFIX)) {
                    if (minFilename.compareTo(fileName) > 0)
                        minFilename = fileName;

                    if (maxFilename.compareTo(fileName) < 0)
                        maxFilename = fileName;
                }
            }

            int min = UNSET_MIN_CYCLE;
            if (!INITIAL_MIN_FILENAME.equals(minFilename))
                min = fileNameToCycleFunction.applyAsInt(minFilename);

            int max = UNSET_MAX_CYCLE;
            if (!INITIAL_MAX_FILENAME.equals(maxFilename))
                max = fileNameToCycleFunction.applyAsInt(maxFilename);

            minCreatedCycle = min;
            maxCreatedCycle = max;

            // Both persisted watermarks move only forward. Empty or stale directory listings
            // cannot move ordinary writers back into an earlier generation.
            if (min != UNSET_MIN_CYCLE &&
                    !minCycleValue.compareAndSwapValue(UNSET_MIN_CYCLE, min))
                minCycleValue.setMaxValue(min);
            if (max != UNSET_MAX_CYCLE)
                maxCycleValue.setMaxValue(max);
            modCount.addAtomicValue(1);
            lastSeenModCount = modCount.getVolatileValue();
            return null;
        });
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
        tableStore.doWithExclusiveLock(ignored -> {
            minCreatedCycle = Math.min(minCreatedCycle, cycle);
            maxCreatedCycle = Math.max(maxCreatedCycle, cycle);
            minCycleValue.compareAndSwapValue(UNSET_MIN_CYCLE, cycle);
            maxCycleValue.setMaxValue(cycle);
            modCount.addAtomicValue(1);
            lastSeenModCount = modCount.getVolatileValue();
            return null;
        });
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
        throwExceptionIfClosed();
        refreshPublishedBounds();
        return maxCreatedCycle;
    }

    @Override
    public int getMaxCycleForWrite() {
        return getMaxCycleValue();
    }

    boolean resetWriteFloorIfEmpty() {
        return tableStore.doWithExclusiveLock(ignored -> {
            throwExceptionIfClosed();
            tableStore.throwExceptionIfClosed();

            final String[] fileNames = queuePath.toFile().list();
            if (fileNames == null)
                return false;
            for (String fileName : fileNames) {
                if (fileName.endsWith(SingleChronicleQueue.SUFFIX))
                    return false;
            }

            minCreatedCycle = UNSET_MIN_CYCLE;
            maxCreatedCycle = UNSET_MAX_CYCLE;
            minCycleValue.setOrderedValue(UNSET_MIN_CYCLE);
            maxCycleValue.setOrderedValue(UNSET_MAX_CYCLE);
            modCount.addAtomicValue(1);
            lastSeenModCount = modCount.getVolatileValue();
            return true;
        });
    }

    /**
     * Returns the lowest cycle number created in the queue.
     *
     * @return The lowest created cycle.
     */
    @Override
    public int getMinCreatedCycle() {
        throwExceptionIfClosed();
        refreshPublishedBounds();
        return minCreatedCycle;
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

    private void refreshPublishedBounds() {
        final long publishedModCount = modCount.getVolatileValue();
        if (lastSeenModCount == publishedModCount)
            return;

        // A cooperating writer already published these bounds through the table store. Observe
        // them without scanning the directory from an append or tailer hot path.
        minCreatedCycle = getMinCycleValue();
        maxCreatedCycle = getMaxCycleValue();
        lastSeenModCount = publishedModCount;
    }

    /**
     * Returns the volatile value of the maximum cycle.
     *
     * @return The maximum cycle value.
     */
    protected int getMaxCycleValue() {
        return (int) maxCycleValue.getVolatileValue();
    }

    /**
     * Returns the volatile value of the minimum cycle.
     *
     * @return The minimum cycle value.
     */
    protected int getMinCycleValue() {
        return (int) minCycleValue.getVolatileValue();
    }
}
