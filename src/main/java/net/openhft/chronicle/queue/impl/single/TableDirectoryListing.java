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
            //! freshListingReportsUnsetCycle checks both the semantic -1 and the legacy persisted empty value.
            //! Old readers narrow this field and recognise only Integer.MIN_VALUE as empty; storing domain -1 would
            //! send their toEnd() down the non-empty path. Use the legacy encoding here and on empty refresh, while
            //! decoding Long.MIN_VALUE before narrowing remains necessary for readers racing this initialisation.
            maxCycleValue.compareAndSwapValue(Long.MIN_VALUE, LEGACY_UNSET_MAX_CYCLE);
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
        //! DirectoryPublicationBoundaryTest#readOnlyRetryClosesEveryReturnedBinding fails if a retry overwrites a
        //! binding returned before a later acquisition failed. Acquire transactionally; callers cannot release a
        //! partially assigned field after it has been lost. Allocation failures before return belong to ValueIn.
        LongValue maximum = null;
        LongValue minimum = null;
        LongValue modifications = null;
        try {
            maximum = tableStore.acquireValueFor(HIGHEST_CREATED_CYCLE);
            minimum = tableStore.acquireValueFor(LOWEST_CREATED_CYCLE);
            modifications = tableStore.acquireValueFor(MOD_COUNT);
        } catch (RuntimeException | Error failure) {
            Closeable.closeQuietly(maximum, minimum, modifications);
            throw failure;
        }
        Closeable.closeQuietly(maxCycleValue, minCycleValue, modCount);
        maxCycleValue = maximum;
        minCycleValue = minimum;
        modCount = modifications;
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

        while (true) {
            throwExceptionIfClosed();
            tableStore.throwExceptionIfClosed();
            Jvm.safepoint();
            //! refreshRetriesWhenLegacyMinimumIsPublishedAfterMaximumCas preserves a late legacy publication;
            //! it is not an independent discriminator for every observation around the scan. Both bounds still need
            //! CAS publication because older writers publish min, max, then modCount without taking a new lock.
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

            //! CycleOverflowTest#extendedYearFilesRetainLogicalBoundsAfterRefreshAndReopen requires numeric bounds:
            //! a built-in daily/hourly filename for a large valid cycle starts with '+', which sorts before 1970.
            //! Parse and validate every candidate before publishing either bound; lexical extrema can falsely report
            //! the present maximum as deleted. Malformed interior names now also fail this explicit refresh.
            int min = INITIAL_MIN_CYCLE;
            int max = UNSET_CONTEXT;
            for (String fileName : fileNamesList) {
                if (fileName.endsWith(SingleChronicleQueue.SUFFIX)) {
                    int cycle = requireCycle(fileNameToCycleFunction.applyAsInt(fileName), "physical cycle");
                    min = Math.min(min, cycle);
                    max = Math.max(max, cycle);
                }
            }

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
            //! TestDeleteQueueFile#deletingOldFilesChaosTest is retention integration evidence, now with live
            //! forward/backward workers and checked progress, not a discriminator for this guard. The old tests
            //! permitting live highest-roll deletion are deliberately replaced: SingleChronicleQueueTest's
            //! testToEndAfterOfflineQueueDeletion retains whole-Queue deletion only after all handles close.
            if (observedMax != UNSET_CONTEXT && max < observedMax)
                throw new IllegalStateException("Highest/current roll " + observedMax
                        + " disappeared while Queue metadata remains");

            //! DirectoryPublicationBoundaryTest#readOnlyTailerSeesMinimumBeforeRefreshedMaximum pauses after the
            //! maximum CAS. Minimum must already be visible: publishing maximum first exposes MAX_VALUE/7 to a
            //! read-only tailer and strands it at a fictitious first cycle. Maximum commits the non-empty state,
            //! matching onRoll and legacy writers; each failed CAS still retries against fresh physical bounds.
            if (!minCycleValue.compareAndSwapValue(observedStoredMin, min)) {
                Jvm.nanoPause();
                continue;
            }
            //! freshListingReportsUnsetCycle also checks empty refresh's old-reader storage encoding. The semantic
            //! UNSET_CONTEXT comparison above remains independent of that persisted representation.
            final long storedMax = max == UNSET_CONTEXT ? LEGACY_UNSET_MAX_CYCLE : max;
            if (!maxCycleValue.compareAndSwapValue(observedStoredMax, storedMax)) {
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
        //! readOnlyListingHidesPartiallyPublishedCycleZero covers empty-to-non-empty visibility, not every volatile
        //! read interleaving here. Recheck maximum so a concurrently committed non-empty state cannot be paired with
        //! the minimum sampled for an earlier empty state; no current test independently discriminates this retry.
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
