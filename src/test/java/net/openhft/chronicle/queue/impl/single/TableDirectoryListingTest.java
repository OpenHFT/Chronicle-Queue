/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.wire.MarshallableOut;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class TableDirectoryListingTest extends QueueTestCommon {
    private DirectoryListing listing;
    private DirectoryListing listingReadOnly;
    private TableStore<Metadata.NoMeta> tablestore;
    private TableStore<Metadata.NoMeta> tablestoreReadOnly;
    private File testDirectory;
    private File tableFile;
    private File tempFile;

    @NotNull
    private File testDirectory() {
        return getTmpDir();
    }

    @Before
    public void setUp() throws IOException {
        testDirectory = testDirectory();
        testDirectory.mkdirs();
        tableFile = new File(testDirectory, "dir-list" + SingleTableStore.SUFFIX);
        tablestore = SingleTableBuilder.
                binary(tableFile, Metadata.NoMeta.INSTANCE).build();
        SystemTimeProvider time = SystemTimeProvider.INSTANCE;
        listing = new TableDirectoryListing(tablestore,
                testDirectory.toPath(),
                f -> Integer.parseInt(f.split("\\.")[0]),
                time);
        listing.init();
        tablestoreReadOnly = SingleTableBuilder.
                binary(tableFile, Metadata.NoMeta.INSTANCE).readOnly(true).build();
        listingReadOnly = new TableDirectoryListingReadOnly(tablestoreReadOnly, time);
        listingReadOnly.init();
        tempFile = File.createTempFile("foo", "bar");
        tempFile.deleteOnExit();
    }

    @Override
    public void preAfter() {
        Closeable.closeQuietly(listing, listingReadOnly, tablestore, tablestoreReadOnly);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldBlowUpIfClosed() {
        listing.close();
        listing.getMaxCreatedCycle();
    }

    @Test
    public void shouldTrackMaxValue() {
        listing.refresh(true);

        listing.onFileCreated(tempFile, 7);

        assertEquals(7, listing.getMaxCreatedCycle());
        assertEquals(7, listing.getMinCreatedCycle());
        assertEquals(7, listingReadOnly.getMaxCreatedCycle());
        assertEquals(7, listingReadOnly.getMinCreatedCycle());

        listing.onFileCreated(tempFile, 8);

        assertEquals(8, listing.getMaxCreatedCycle());
        assertEquals(7, listing.getMinCreatedCycle());
        assertEquals(8, listingReadOnly.getMaxCreatedCycle());
        assertEquals(7, listingReadOnly.getMinCreatedCycle());
    }

    @Test
    public void shouldInitialiseFromFilesystem() throws IOException {
        new File(testDirectory, 1 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 2 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 3 + SingleChronicleQueue.SUFFIX).createNewFile();

        listing.refresh(true);

        assertEquals(3, listing.getMaxCreatedCycle());
        assertEquals(1, listing.getMinCreatedCycle());
        assertEquals(3, listingReadOnly.getMaxCreatedCycle());
        assertEquals(1, listingReadOnly.getMinCreatedCycle());
    }

    @Test
    public void lockShouldTimeOut() {
        listing.onFileCreated(tempFile, 8);

        listing.onFileCreated(tempFile, 9);
        assertEquals(9, listing.getMaxCreatedCycle());
        assertEquals(9, listingReadOnly.getMaxCreatedCycle());
    }

    @Test
    public void failedDirectoryListingDoesNotResetPublishedBounds() {
        listing.onFileCreated(tempFile, 7);

        final TableDirectoryListing failedListing = new TableDirectoryListing(
                tablestore,
                tempFile.toPath(),
                ignored -> 0,
                SystemTimeProvider.INSTANCE);
        try {
            failedListing.init();
            final long refreshTime = failedListing.lastRefreshTimeMS();
            failedListing.refresh(true);
            assertEquals(7, failedListing.getMaxCreatedCycle());
            assertEquals(7, failedListing.getMinCreatedCycle());
            assertEquals(refreshTime, failedListing.lastRefreshTimeMS());
        } finally {
            failedListing.close();
        }
    }

    @Test
    public void legacyPublicationAfterRefreshIsVisibleWithoutAnotherEvent() throws IOException {
        final File cycleSeven = new File(testDirectory, 7 + SingleChronicleQueue.SUFFIX);
        assertTrue(cycleSeven.createNewFile());
        listing.onFileCreated(cycleSeven, 7);
        listing.refresh(true);
        assertEquals(7, listing.getMaxCreatedCycle());

        // Simulate the publication made by a pre-QUEUE-146 process after this instance has
        // completed its filesystem scan. Current physical-bound reads must not depend on a
        // second local refresh event.
        final LongValue legacyHighestCycle = tablestore.acquireValueFor("listing.highestCycle");
        final LongValue legacyModCount = tablestore.acquireValueFor("listing.modCount");
        try {
            legacyHighestCycle.setMaxValue(9);
            legacyModCount.addAtomicValue(1);
        } finally {
            legacyHighestCycle.close();
            legacyModCount.close();
        }

        assertEquals(9, listing.getMaxCreatedCycle());
    }

    @Test
    public void refreshRetriesWhenLegacyMinimumIsPublishedAfterMaximumCas() throws Exception {
        final TableDirectoryListing tableListing = (TableDirectoryListing) listing;
        final LongValue legacyMinimum = tablestore.acquireValueFor("listing.lowestCycle");
        final LongValue legacyMaximum = tablestore.acquireValueFor("listing.highestCycle");
        final LongValue legacyModCount = tablestore.acquireValueFor("listing.modCount");
        final Field maximumField = TableDirectoryListing.class.getDeclaredField("maxCycleValue");
        maximumField.setAccessible(true);
        final LongValue maximumDelegate = (LongValue) maximumField.get(tableListing);
        final AtomicBoolean published = new AtomicBoolean();

        final LongValue interceptedMaximum = (LongValue) Proxy.newProxyInstance(
                LongValue.class.getClassLoader(),
                new Class<?>[]{LongValue.class},
                (proxy, method, args) -> {
                    try {
                        final Object result = method.invoke(maximumDelegate, args);
                        if ("compareAndSwapValue".equals(method.getName())
                                && Boolean.TRUE.equals(result)
                                && published.compareAndSet(false, true)) {
                            // Precisely model a pre-QUEUE-146 publication between the new
                            // refresher's maximum and minimum CAS operations.
                            assertTrue(new File(testDirectory, 9 + SingleChronicleQueue.SUFFIX).createNewFile());
                            legacyMinimum.setMinValue(9);
                            legacyMaximum.setMaxValue(9);
                            legacyModCount.addAtomicValue(1);
                        }
                        return result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

        maximumField.set(tableListing, interceptedMaximum);
        try {
            tableListing.refresh(true);
        } finally {
            maximumField.set(tableListing, maximumDelegate);
            legacyMinimum.close();
            legacyMaximum.close();
            legacyModCount.close();
        }

        assertTrue(published.get());
        assertEquals(9, tableListing.getMinCreatedCycle());
        assertEquals(9, tableListing.getMaxCreatedCycle());
    }

    @Test
    public void historicalDeletionAndLowerRecreationPreservePublishedMaximum() throws IOException {
        final File cycleSeven = new File(testDirectory, 7 + SingleChronicleQueue.SUFFIX);
        final File cycleEight = new File(testDirectory, 8 + SingleChronicleQueue.SUFFIX);
        final File cycleNine = new File(testDirectory, 9 + SingleChronicleQueue.SUFFIX);
        assertTrue(cycleSeven.createNewFile());
        listing.onFileCreated(cycleSeven, 7);
        assertTrue(cycleEight.createNewFile());
        listing.onFileCreated(cycleEight, 8);
        assertTrue(cycleNine.createNewFile());
        listing.onFileCreated(cycleNine, 9);

        assertTrue(cycleSeven.delete());
        listing.refresh(true);
        assertEquals(8, listing.getMinCreatedCycle());
        assertEquals(9, listing.getMaxCreatedCycle());

        final File cycleSix = new File(testDirectory, 6 + SingleChronicleQueue.SUFFIX);
        assertTrue(cycleSix.createNewFile());
        listing.onFileCreated(cycleSix, 6);
        assertEquals(6, listing.getMinCreatedCycle());
        assertEquals(6, persistedCycle("listing.lowestCycle"));
        assertEquals(9, persistedCycle("listing.highestCycle"));
    }

    @Test
    public void publishedModificationRefreshesAnotherListingsCurrentBounds() throws IOException {
        final TableDirectoryListing secondListing = new TableDirectoryListing(
                tablestore,
                testDirectory.toPath(),
                f -> Integer.parseInt(f.split("\\.")[0]),
                SystemTimeProvider.INSTANCE);
        try {
            secondListing.init();
            secondListing.refresh(true);

            final File cycleFile = new File(testDirectory, 7 + SingleChronicleQueue.SUFFIX);
            cycleFile.createNewFile();
            listing.onFileCreated(cycleFile, 7);

            secondListing.refresh(false);
            assertEquals(7, secondListing.getMinCreatedCycle());
            assertEquals(7, secondListing.getMaxCreatedCycle());
        } finally {
            secondListing.close();
        }
    }

    @Test
    public void preOpenedListingsPublishBoundsFromSharedValues() throws IOException {
        final TableDirectoryListing secondListing = new TableDirectoryListing(
                tablestore,
                testDirectory.toPath(),
                f -> Integer.parseInt(f.split("\\.")[0]),
                SystemTimeProvider.INSTANCE);
        try {
            secondListing.init();

            final File cycleFive = new File(testDirectory, 5 + SingleChronicleQueue.SUFFIX);
            final File cycleSix = new File(testDirectory, 6 + SingleChronicleQueue.SUFFIX);
            assertTrue(cycleFive.createNewFile());
            listing.onFileCreated(cycleFive, 5);
            assertTrue(cycleSix.createNewFile());
            secondListing.onFileCreated(cycleSix, 6);

            assertEquals(5, secondListing.getMinCreatedCycle());
            assertEquals(6, secondListing.getMaxCreatedCycle());
            listing.refresh(false);
            assertEquals(5, listing.getMinCreatedCycle());
            assertEquals(6, listing.getMaxCreatedCycle());
        } finally {
            secondListing.close();
        }
    }

    @Test
    public void lowerRecreatedCycleIsPublishedToAnotherOpenListing() throws IOException {
        final TableDirectoryListing secondListing = new TableDirectoryListing(
                tablestore,
                testDirectory.toPath(),
                f -> Integer.parseInt(f.split("\\.")[0]),
                SystemTimeProvider.INSTANCE);
        try {
            secondListing.init();

            final File cycleSix = new File(testDirectory, 6 + SingleChronicleQueue.SUFFIX);
            assertTrue(cycleSix.createNewFile());
            listing.onFileCreated(cycleSix, 6);
            secondListing.refresh(false);
            assertEquals(6, secondListing.getMinCreatedCycle());

            final File cycleFive = new File(testDirectory, 5 + SingleChronicleQueue.SUFFIX);
            assertTrue(cycleFive.createNewFile());
            listing.onFileCreated(cycleFive, 5);

            secondListing.refresh(false);
            assertEquals(5, secondListing.getMinCreatedCycle());
            assertEquals(6, secondListing.getMaxCreatedCycle());
        } finally {
            secondListing.close();
        }
    }

    @Test
    public void refreshRejectsMissingPublishedMaximum() throws IOException {
        final File cycleSeven = new File(testDirectory, 7 + SingleChronicleQueue.SUFFIX);
        assertTrue(cycleSeven.createNewFile());
        listing.onFileCreated(cycleSeven, 7);
        final long modCount = listing.modCount();
        final long refreshTime = listing.lastRefreshTimeMS();

        assertTrue(cycleSeven.delete());
        final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> listing.refresh(true));

        assertTrue(failure.getMessage().contains("Highest/current roll 7 disappeared"));
        assertEquals(7, listing.getMinCreatedCycle());
        assertEquals(7, listing.getMaxCreatedCycle());
        assertEquals(modCount, listing.modCount());
        assertEquals(refreshTime, listing.lastRefreshTimeMS());
    }

    @Test
    public void freshListingReportsUnsetCycle() {
        assertEquals(MarshallableOut.UNSET_CONTEXT, listing.getMaxCreatedCycle());
        assertEquals(MarshallableOut.UNSET_CONTEXT, listing.getMinCreatedCycle());
        assertEquals(MarshallableOut.UNSET_CONTEXT, persistedCycle("listing.highestCycle"));
        assertEquals(Integer.MAX_VALUE, persistedCycle("listing.lowestCycle"));

        listing.refresh(true);
        assertEquals(MarshallableOut.UNSET_CONTEXT, listing.getMaxCreatedCycle());
        assertEquals(MarshallableOut.UNSET_CONTEXT, persistedCycle("listing.highestCycle"));
    }

    @Test
    public void readOnlyListingDecodesLegacyUnsetCycle() {
        setPersistedCycle("listing.highestCycle", Integer.MIN_VALUE);

        assertEquals(MarshallableOut.UNSET_CONTEXT, listingReadOnly.getMaxCreatedCycle());
        assertEquals(MarshallableOut.UNSET_CONTEXT, listingReadOnly.getMinCreatedCycle());
    }

    @Test
    public void readOnlyListingDecodesRawStorageSentinelAsUnset() {
        try (MappedBytes bytes = tablestoreReadOnly.bytes()) {
            assertTrue(bytes.isBackingFileReadOnly());
        }
        setPersistedCycle("listing.highestCycle", Long.MIN_VALUE);

        assertEquals(MarshallableOut.UNSET_CONTEXT, listingReadOnly.getMaxCreatedCycle());
    }

    @Test
    public void readOnlyListingHidesPartiallyPublishedCycleZero() {
        setPersistedCycle("listing.highestCycle", Long.MIN_VALUE);
        setPersistedCycle("listing.lowestCycle", 0);
        reopenReadOnlyListing();

        assertEquals(MarshallableOut.UNSET_CONTEXT, listingReadOnly.getMaxCreatedCycle());
        assertEquals(MarshallableOut.UNSET_CONTEXT, listingReadOnly.getMinCreatedCycle());

        setPersistedCycle("listing.highestCycle", 0);
        assertEquals(0, listingReadOnly.getMaxCreatedCycle());
        assertEquals(0, listingReadOnly.getMinCreatedCycle());
    }

    @Test
    public void unsetToCycleZeroPublicationSurvivesReopen() {
        listing.onRoll(0);
        assertEquals(0, listing.getMaxCreatedCycle());
        assertEquals(0, listing.getMinCreatedCycle());

        reopenReadOnlyListing();
        assertEquals(0, listingReadOnly.getMaxCreatedCycle());
        assertEquals(0, listingReadOnly.getMinCreatedCycle());
    }

    @Test
    public void publishedCycleZeroMissingItsFileFailsClosed() {
        listing.onRoll(0);

        final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> listing.refresh(true));

        assertTrue(failure.getMessage().contains("Highest/current roll 0 disappeared"));
        assertEquals(0, listing.getMaxCreatedCycle());
    }

    @Test
    public void persistedCyclesOutsideDomainFailClosed() {
        setPersistedCycle("listing.highestCycle", -2);
        assertInvalidStoredCycle("listing.highestCycle", -2, listingReadOnly::getMaxCreatedCycle);
        assertInvalidStoredCycle("listing.highestCycle", -2, () -> listing.refresh(true));

        final long aboveUInt31 = (long) Integer.MAX_VALUE + 1;
        setPersistedCycle("listing.highestCycle", aboveUInt31);
        assertInvalidStoredCycle("listing.highestCycle", aboveUInt31, listingReadOnly::getMaxCreatedCycle);
        assertInvalidStoredCycle("listing.highestCycle", aboveUInt31, () -> listing.refresh(true));

        setPersistedCycle("listing.highestCycle", 0);
        setPersistedCycle("listing.lowestCycle", -2);
        assertInvalidStoredCycle("listing.lowestCycle", -2, listingReadOnly::getMinCreatedCycle);
        assertInvalidStoredCycle("listing.lowestCycle", -2, () -> listing.refresh(true));

        setPersistedCycle("listing.lowestCycle", aboveUInt31);
        assertInvalidStoredCycle("listing.lowestCycle", aboveUInt31, listingReadOnly::getMinCreatedCycle);
        assertInvalidStoredCycle("listing.lowestCycle", aboveUInt31, () -> listing.refresh(true));
    }

    @Test
    public void maximumCycleRoundTripsAsAValidMinimum() {
        listing.onRoll(Integer.MAX_VALUE);

        assertEquals(Integer.MAX_VALUE, listing.getMaxCreatedCycle());
        assertEquals(Integer.MAX_VALUE, listing.getMinCreatedCycle());
        reopenReadOnlyListing();
        assertEquals(Integer.MAX_VALUE, listingReadOnly.getMaxCreatedCycle());
        assertEquals(Integer.MAX_VALUE, listingReadOnly.getMinCreatedCycle());
    }

    @Test
    public void onRollRejectsCyclesOutsideUInt31() {
        assertThrows(IllegalStateException.class, () -> listing.onRoll(MarshallableOut.UNSET_CONTEXT));
    }

    @Test
    public void fileSystemListingDistinguishesMaximumCycleFromUnset() {
        final DirectoryListing fileSystemListing = new FileSystemDirectoryListing(
                testDirectory,
                f -> Integer.parseInt(f.split("\\.")[0]),
                SystemTimeProvider.INSTANCE);
        try {
            fileSystemListing.refresh(true);
            assertEquals(MarshallableOut.UNSET_CONTEXT, fileSystemListing.getMaxCreatedCycle());
            assertEquals(MarshallableOut.UNSET_CONTEXT, fileSystemListing.getMinCreatedCycle());

            fileSystemListing.onRoll(Integer.MAX_VALUE);
            assertEquals(Integer.MAX_VALUE, fileSystemListing.getMaxCreatedCycle());
            assertEquals(Integer.MAX_VALUE, fileSystemListing.getMinCreatedCycle());
            assertThrows(IllegalStateException.class,
                    () -> fileSystemListing.onRoll(MarshallableOut.UNSET_CONTEXT));
        } finally {
            fileSystemListing.close();
        }
    }

    @Test
    public void refreshRejectsMissingLegacyPublication() throws IOException {
        final File cycleSeven = new File(testDirectory, 7 + SingleChronicleQueue.SUFFIX);
        assertTrue(cycleSeven.createNewFile());
        listing.onFileCreated(cycleSeven, 7);

        final LongValue legacyHighestCycle = tablestore.acquireValueFor("listing.highestCycle");
        try {
            legacyHighestCycle.setMaxValue(9);
        } finally {
            legacyHighestCycle.close();
        }
        assertThrows(IllegalStateException.class, () -> listing.refresh(true));
        assertEquals(9, listing.getMaxCreatedCycle());
    }

    @Test
    public void metadataContainsNoSeparateWriteFloor() {
        assertFalse(tablestore.dump(net.openhft.chronicle.wire.WireType.BINARY_LIGHT)
                .contains("listing.highestCycleWriteFloor"));
    }

    private int persistedCycle(final String key) {
        final LongValue value = tablestore.acquireValueFor(key);
        try {
            return (int) value.getVolatileValue();
        } finally {
            value.close();
        }
    }

    private void setPersistedCycle(final String key, final long cycle) {
        final LongValue value = tablestore.acquireValueFor(key);
        try {
            value.setVolatileValue(cycle);
        } finally {
            value.close();
        }
    }

    private void reopenReadOnlyListing() {
        Closeable.closeQuietly(listingReadOnly, tablestoreReadOnly);
        tablestoreReadOnly = SingleTableBuilder.
                binary(tableFile, Metadata.NoMeta.INSTANCE).readOnly(true).build();
        listingReadOnly = new TableDirectoryListingReadOnly(tablestoreReadOnly, SystemTimeProvider.INSTANCE);
        listingReadOnly.init();
    }

    private static void assertInvalidStoredCycle(final String fieldName,
                                                 final long cycle,
                                                 final Runnable read) {
        final IllegalStateException failure = assertThrows(IllegalStateException.class, read::run);
        assertTrue(failure.getMessage().contains(fieldName));
        assertTrue(failure.getMessage().contains(Long.toString(cycle)));
    }
}
