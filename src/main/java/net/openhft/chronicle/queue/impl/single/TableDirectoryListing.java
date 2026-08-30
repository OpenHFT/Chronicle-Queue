/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
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
    //! Physical bounds may fall after retention; this separate persisted value is monotonic and is
    //! therefore safe for ordinary writer selection across reopen and rolling upgrades.
    private static final String HIGHEST_CYCLE_WRITE_FLOOR = "listing.highestCycleWriteFloor";
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
    //! This mapped value has the same acquire/close ownership as the existing bound values.
    private volatile LongValue writeFloorValue;
    private volatile LongValue modCount;
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
            //! A newly acquired LongValue contains Long.MIN_VALUE, whose int cast is zero. Normalize
            //! the physical maximum first, then initialize the floor from it, so a fresh Queue consistently reports
            //! UNSET_MAX_CYCLE rather than fabricating cycle zero.
            maxCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_MAX_CYCLE);
            minCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_MIN_CYCLE);
            writeFloorValue.compareAndSwapValue(Long.MIN_VALUE, getMaxCycleValue());
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
        writeFloorValue = acquireWriteFloor();
        modCount = tableStore.acquireValueFor(MOD_COUNT);
    }

    /**
     * Acquires the monotonic ordinary-write floor. The read-only listing returns {@code null}: its
     * table store is mapped read-only, so the key cannot be created in metadata written before
     * QUEUE-146, and a read-only Queue never selects an ordinary-write cycle.
     */
    protected LongValue acquireWriteFloor() {
        //! Do not infer mapping mode from TableStore.readOnly(); not every TableStore implementation
        //! exposes the mode used for this mapping. The subclass hook also lets a read-only listing open older metadata
        //! without attempting to create the write-floor key.
        return tableStore.acquireValueFor(HIGHEST_CYCLE_WRITE_FLOOR);
    }

    /**
     * Refreshes the directory listing, updating the cycle values if needed. Only refreshes if the force flag is set.
     *
     * @param force Whether to force a refresh of the directory listing.
     */
    @Override
    public void refresh(final boolean force) {

        if (!force)
            return;

        //! Older writers publish only the legacy bounds and do not participate in this protocol.
        //! Observe both bounds and modCount around the scan, then CAS both bounds; any change or lost CAS requires a
        //! rescan so stale filesystem state cannot overwrite a concurrent legacy publication.
        throwExceptionIfClosed();
        tableStore.throwExceptionIfClosed();

        while (true) {
            // Writers from before QUEUE-146 do not take a table lock. Observe both legacy publication
            // fields around the filesystem scan and retry if such a writer moves either one while the
            // scan is in progress.
            final long observedModCount = modCount.getVolatileValue();
            final long observedLegacyMin = minCycleValue.getVolatileValue();
            final long observedLegacyMax = maxCycleValue.getVolatileValue();

            final String[] fileNamesList = queuePath.toFile().list();
            //! Null means the directory could not be listed, not that it is empty. Preserve all
            //! published state and leave the refresh timestamp unchanged so a later read can retry.
            if (fileNamesList == null)
                return;

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

            if (observedModCount != modCount.getVolatileValue()
                    || observedLegacyMin != minCycleValue.getVolatileValue()
                    || observedLegacyMax != maxCycleValue.getVolatileValue()) {
                Jvm.nanoPause();
                continue;
            }

            // The CAS closes the remaining check/publication window for a legacy writer. A
            // failed CAS means its publication won and the directory must be scanned again.
            if (!maxCycleValue.compareAndSwapValue(observedLegacyMax, max)) {
                Jvm.nanoPause();
                continue;
            }

            // A legacy writer publishes minimum before maximum. It can therefore race after
            // the maximum CAS above without changing maximum yet. Publish both physical bounds
            // symmetrically; if either observed value moved, rescan rather than overwriting the
            // legacy writer's lower bound with a stale (possibly empty-directory) result.
            if (!minCycleValue.compareAndSwapValue(observedLegacyMin, min)) {
                Jvm.nanoPause();
                continue;
            }

            // Persist the physical bounds independently of the monotonic ordinary-write floor.
            // Historical deletion may move or empty these bounds, but cannot move a writer back.
            //! This CAS sequence may lower the physical maximum after deletion. Preserve the maximum
            //! observed before the scan in the monotonic floor first, including publications made by legacy writers.
            if (observedLegacyMax != UNSET_MAX_CYCLE)
                writeFloorValue.setMaxValue(observedLegacyMax);
            modCount.addAtomicValue(1);
            break;
        }
        lastRefreshTimeMS = time.currentTimeMillis();
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
        // All publications are CAS/atomic operations. refresh(true) observes and CASes both
        // bounds, so ordinary roll publication does not need to block the append path on the
        // table-store file lock.
        minCycleValue.setMinValue(cycle);
        maxCycleValue.setMaxValue(cycle);
        //! Publish the writer floor as part of roll publication. The read-side legacy ratchet is a
        //! rolling-upgrade fallback, not the primary path for cooperating writers.
        writeFloorValue.setMaxValue(cycle);
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

    //! Writer selection reads the monotonic floor, while rolling upgrades also require every read to
    //! ratchet legacy highest-cycle publications into that floor. A null floor belongs only to the read-only listing,
    //! which never selects an ordinary write destination.
    @Override
    public int getMaxCycleForWrite() {
        final int legacyMaximum = getMaxCycleValue();
        if (writeFloorValue == null)
            return legacyMaximum;

        // During a rolling upgrade an older process still publishes only listing.highestCycle.
        // Ratchet that value into the new monotonic key on every selection, rather than only once
        // during init, so a rolled-back clock cannot select below a later legacy publication.
        if (legacyMaximum != UNSET_MAX_CYCLE)
            writeFloorValue.setMaxValue(legacyMaximum);
        return (int) writeFloorValue.getVolatileValue();
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
        //! writeFloorValue owns mapped resources just like the physical-bound values and must be
        //! released in the same lifecycle.
        Closeable.closeQuietly(minCycleValue, maxCycleValue, writeFloorValue, modCount);
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
