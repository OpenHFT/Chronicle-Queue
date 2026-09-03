/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
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

import static net.openhft.chronicle.wire.MarshallableOut.UNSET_CONTEXT;

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
    private static final int LEGACY_UNSET_MAX_CYCLE = Integer.MIN_VALUE;
    static final int INITIAL_MIN_CYCLE = Integer.MAX_VALUE;
    static final String INITIAL_MIN_FILENAME = Character.toString(Character.MAX_VALUE);
    static final String INITIAL_MAX_FILENAME = Character.toString(Character.MIN_VALUE);
    private final TableStore<?> tableStore;
    private final Path queuePath;
    private final ToIntFunction<String> fileNameToCycleFunction;
    private final TimeProvider time;
    private volatile LongValue maxCycleValue;
    private volatile LongValue minCycleValue;
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
            //! freshListingReportsUnsetCycle requires a newly allocated Long.MIN_VALUE to be replaced before another
            //! process can narrow it to cycle zero. New writers persist the domain's UNSET_CONTEXT value; the decoder
            //! separately accepts the Integer.MIN_VALUE representation written by develop's empty refresh.
            maxCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_CONTEXT);
            minCycleValue.compareAndSwapValue(Long.MIN_VALUE, INITIAL_MIN_CYCLE);
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

        if (!force)
            return;

        while (true) {
            throwExceptionIfClosed();
            tableStore.throwExceptionIfClosed();
            Jvm.safepoint();
            //! refreshRetriesWhenLegacyMinimumIsPublishedAfterMaximumCas requires observing min, max and modCount
            //! around the scan and CAS-publishing both bounds. Older writers publish min, max, then modCount without
            //! participating in a new lock protocol, so a partial legacy publication must force another scan.
            // Writers from before QUEUE-146 do not take a table lock. Observe both legacy publication
            // fields around the filesystem scan and retry if such a writer moves either one while the
            // scan is in progress.
            final long observedModCount = modCount.getVolatileValue();
            final long observedStoredMin = minCycleValue.getVolatileValue();
            final long observedStoredMax = maxCycleValue.getVolatileValue();
            //! persistedCyclesOutsideDomainFailClosed requires every mapped long to be decoded and range checked
            //! before it participates in cycle arithmetic.
            final int observedMax = decodeMaxCycle(observedStoredMax);
            decodeMinCycle(observedStoredMin, observedMax);

            final String[] fileNamesList = queuePath.toFile().list();
            //! failedDirectoryListingDoesNotResetPublishedBounds fails if list()==null is treated as an empty Queue:
            //! that would replace valid shared bounds with sentinels and could move ordinary writers backwards.
            // A failed directory read is not evidence that the Queue is empty. Preserve the
            // published bounds and leave the refresh timestamp unchanged so a later call retries.
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

            int min = INITIAL_MIN_CYCLE;
            if (!INITIAL_MIN_FILENAME.equals(minFilename))
                min = fileNameToCycleFunction.applyAsInt(minFilename);

            int max = UNSET_CONTEXT;
            if (!INITIAL_MAX_FILENAME.equals(maxFilename))
                max = fileNameToCycleFunction.applyAsInt(maxFilename);

            if (observedModCount != modCount.getVolatileValue()
                    || observedStoredMin != minCycleValue.getVolatileValue()
                    || observedStoredMax != maxCycleValue.getVolatileValue()) {
                Jvm.nanoPause();
                continue;
            }

            // Supported maintenance retains the highest/current roll. Losing it while metadata
            // survives is an inconsistent Queue, not permission to move ordinary writes backward.
            //! refreshRejectsMissingPublishedMaximum, refreshRejectsMissingLegacyPublication and
            //! publishedCycleZeroMissingItsFileFailsClosed fail if a scan may lower the mapped maximum while
            //! metadata survives. The explicit UNSET_CONTEXT comparison keeps published cycle zero in this check.
            if (observedMax != UNSET_CONTEXT && max < observedMax)
                throw new IllegalStateException("Highest/current roll " + observedMax
                        + " disappeared while Queue metadata remains");

            // The CAS closes the remaining check/publication window for a legacy writer. A
            // failed CAS means its publication won and the directory must be scanned again.
            //! freshListingReportsUnsetCycle also requires an empty refresh to retain the domain sentinel rather than
            //! reintroduce develop's Integer.MIN_VALUE representation.
            if (!maxCycleValue.compareAndSwapValue(observedStoredMax, max)) {
                Jvm.nanoPause();
                continue;
            }

            // A legacy writer publishes minimum before maximum. It can therefore race after
            // the maximum CAS above without changing maximum yet. Publish both physical bounds
            // symmetrically; if either observed value moved, rescan rather than overwriting the
            // legacy writer's lower bound with a stale (possibly empty-directory) result.
            if (!minCycleValue.compareAndSwapValue(observedStoredMin, min)) {
                Jvm.nanoPause();
                continue;
            }

            modCount.addAtomicValue(1);
            break;
        }
        //! failedDirectoryListingDoesNotResetPublishedBounds also requires a failed scan to remain retryable.
        //! Assigning the refresh time only after successful publication prevents an automatic refresh from being
        //! suppressed by a directory read that returned no listing.
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
        //! onRollRejectsCyclesOutsideUInt31 ensures invalid in-process values cannot enter the same mapped fields
        //! whose restart path now rejects corrupt persisted values.
        final int validCycle = requireCycle(cycle, "roll cycle");
        minCycleValue.setMinValue(validCycle);
        maxCycleValue.setMaxValue(validCycle);
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
        //! freshListingReportsUnsetCycle, readOnlyListingDecodesLegacyUnsetCycle,
        //! readOnlyListingDecodesRawStorageSentinelAsUnset,
        //! readOnlyListingHidesPartiallyPublishedCycleZero, unsetToCycleZeroPublicationSurvivesReopen and
        //! persistedCyclesOutsideDomainFailClosed require storage sentinels and corrupt values to be handled before
        //! narrowing. Writable initialisation reduces the observation window but is not a prerequisite for reads.
        return decodeMaxCycle(maxCycleValue.getVolatileValue());
    }

    /**
     * Returns the volatile value of the minimum cycle.
     *
     * @return The minimum cycle value.
     */
    private int getMinCycleValue() {
        //! maximumCycleRoundTripsAsAValidMinimum requires the decoded maximum to carry empty/non-empty state:
        //! Integer.MAX_VALUE remains both the legacy stored minimum sentinel and a valid UInt31 cycle.
        while (true) {
            final long storedMax = maxCycleValue.getVolatileValue();
            final int maximum = decodeMaxCycle(storedMax);
            final long storedMin = minCycleValue.getVolatileValue();
            if (storedMax == maxCycleValue.getVolatileValue())
                return decodeMinCycle(storedMin, maximum);
            Jvm.nanoPause();
        }
    }

    private static int decodeMaxCycle(final long storedCycle) {
        if (storedCycle == Long.MIN_VALUE
                || storedCycle == LEGACY_UNSET_MAX_CYCLE
                || storedCycle == UNSET_CONTEXT)
            return UNSET_CONTEXT;
        return requireCycle(storedCycle, HIGHEST_CREATED_CYCLE);
    }

    private static int decodeMinCycle(final long storedCycle, final int maximumCycle) {
        if (maximumCycle == UNSET_CONTEXT) {
            if (storedCycle != Long.MIN_VALUE && storedCycle != UNSET_CONTEXT)
                requireCycle(storedCycle, LOWEST_CREATED_CYCLE);
            return UNSET_CONTEXT;
        }
        return requireCycle(storedCycle, LOWEST_CREATED_CYCLE);
    }

    static int requireCycle(final long cycle, final String fieldName) {
        try {
            return Maths.toUInt31(cycle);
        } catch (ArithmeticException e) {
            throw new IllegalStateException("Invalid UInt31 cycle in " + fieldName + ": " + cycle, e);
        }
    }
}
