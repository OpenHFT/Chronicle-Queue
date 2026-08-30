/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.wire.WireType;
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
import static org.junit.Assume.assumeFalse;

public class TableDirectoryListingTest extends QueueTestCommon {
    private DirectoryListing listing;
    private DirectoryListing listingReadOnly;
    private TableStore<Metadata.NoMeta> tablestore;
    private TableStore<Metadata.NoMeta> tablestoreReadOnly;
    private File testDirectory;
    private File tempFile;

    @NotNull
    private File testDirectory() {
        return getTmpDir();
    }

    @Before
    public void setUp() throws IOException {
        testDirectory = testDirectory();
        testDirectory.mkdirs();
        File tableFile = new File(testDirectory, "dir-list" + SingleTableStore.SUFFIX);
        tablestore = SingleTableBuilder.
                binary(tableFile, Metadata.NoMeta.INSTANCE).build();
        tablestoreReadOnly = SingleTableBuilder.
                binary(tableFile, Metadata.NoMeta.INSTANCE).readOnly(true).build();
        SystemTimeProvider time = SystemTimeProvider.INSTANCE;
        listing = new TableDirectoryListing(tablestore,
                testDirectory.toPath(),
                f -> Integer.parseInt(f.split("\\.")[0]),
                time);
        listingReadOnly = new TableDirectoryListingReadOnly(tablestore, time);
        listing.init();
        listingReadOnly.init();
        tempFile = File.createTempFile("foo", "bar");
        tempFile.deleteOnExit();
    }

    @Override
    public void preAfter() {
        Closeable.closeQuietly(tablestore, tablestoreReadOnly, listing, listingReadOnly);
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
    public void failedDirectoryListingDoesNotResetWriteFloor() {
        listing.onFileCreated(tempFile, 7);

        final TableDirectoryListing failedListing = new TableDirectoryListing(
                tablestore,
                tempFile.toPath(),
                ignored -> 0,
                SystemTimeProvider.INSTANCE);
        try {
            failedListing.init();
            failedListing.refresh(true);
            assertEquals(7, failedListing.getMaxCycleForWrite());
            // A failed listing must not be published as an empty directory either.
            assertEquals(7, failedListing.getMaxCreatedCycle());
            assertEquals(7, failedListing.getMinCreatedCycle());
        } finally {
            failedListing.close();
        }
    }

    @Test
    public void publishedHighestCycleIsTheWriteFloor() {
        listing.onFileCreated(tempFile, 7);

        final LongValue publishedHighestCycle = tablestore.acquireValueFor("listing.highestCycleWriteFloor");
        try {
            publishedHighestCycle.setOrderedValue(9);
        } finally {
            publishedHighestCycle.close();
        }

        assertEquals(9, listing.getMaxCycleForWrite());
    }

    @Test
    public void legacyHighestCycleRatchetsAnAlreadyInitialisedWriteFloor() {
        listing.onFileCreated(tempFile, 7);
        assertEquals(7, listing.getMaxCycleForWrite());

        // Simulate a pre-QUEUE-146 process, which publishes only the legacy physical maximum.
        final LongValue legacyHighestCycle = tablestore.acquireValueFor("listing.highestCycle");
        try {
            legacyHighestCycle.setMaxValue(9);
        } finally {
            legacyHighestCycle.close();
        }

        assertEquals(9, listing.getMaxCycleForWrite());
        assertEquals(9, persistedCycle("listing.highestCycleWriteFloor"));
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
        assertEquals(9, listing.getMaxCycleForWrite());
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
        assertEquals(9, tableListing.getMaxCycleForWrite());
    }

    @Test
    public void refreshNeverLowersPersistedWatermarks() throws IOException {
        listing.onFileCreated(tempFile, 7);
        listing.onFileCreated(tempFile, 9);

        new File(testDirectory, 8 + SingleChronicleQueue.SUFFIX).createNewFile();
        listing.refresh(true);
        assertEquals(8, listing.getMinCreatedCycle());
        assertEquals(8, listing.getMaxCreatedCycle());
        assertEquals(9, listing.getMaxCycleForWrite());

        new File(testDirectory, 6 + SingleChronicleQueue.SUFFIX).createNewFile();
        listing.refresh(true);
        assertEquals(6, listing.getMinCreatedCycle());
        assertEquals(6, persistedCycle("listing.lowestCycle"));
        assertEquals(8, persistedCycle("listing.highestCycle"));
        assertEquals(9, listing.getMaxCycleForWrite());
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
            assertEquals(6, secondListing.getMaxCycleForWrite());
        } finally {
            secondListing.close();
        }
    }

    @Test
    public void emptyRefreshPreservesPublishedWriteFloor() throws IOException {
        final File cycleFile = new File(testDirectory, 7 + SingleChronicleQueue.SUFFIX);
        cycleFile.createNewFile();
        listing.onFileCreated(cycleFile, 7);

        final TableDirectoryListing secondListing = new TableDirectoryListing(
                tablestore,
                testDirectory.toPath(),
                f -> Integer.parseInt(f.split("\\.")[0]),
                SystemTimeProvider.INSTANCE);
        try {
            secondListing.init();
            secondListing.refresh(false);
            assertEquals(7, secondListing.getMaxCycleForWrite());

            assertTrue(cycleFile.delete());
            listing.refresh(true);

            secondListing.refresh(false);
            assertEquals(TableDirectoryListing.UNSET_MIN_CYCLE, secondListing.getMinCreatedCycle());
            assertEquals(TableDirectoryListing.UNSET_MAX_CYCLE, secondListing.getMaxCreatedCycle());
            assertEquals(7, secondListing.getMaxCycleForWrite());
        } finally {
            secondListing.close();
        }
    }

    @Test
    public void freshListingReportsUnsetWriteFloor() {
        // Before any publication the floor is the same sentinel lastCycle() uses, not (int) Long.MIN_VALUE == 0.
        assertEquals(TableDirectoryListing.UNSET_MAX_CYCLE, listing.getMaxCycleForWrite());
        assertEquals(TableDirectoryListing.UNSET_MAX_CYCLE, listing.getMaxCreatedCycle());
    }

    @Test
    public void refreshRatchetsLegacyPublicationDeletedBeforeAnyWriteFloorRead() throws IOException {
        final File cycleSeven = new File(testDirectory, 7 + SingleChronicleQueue.SUFFIX);
        assertTrue(cycleSeven.createNewFile());
        listing.onFileCreated(cycleSeven, 7);

        // A pre-QUEUE-146 writer publishes cycle 9, its file is removed again, and nothing reads the write
        // floor before this listing rescans the directory and lowers the physical maximum back to 7.
        final LongValue legacyHighestCycle = tablestore.acquireValueFor("listing.highestCycle");
        try {
            legacyHighestCycle.setMaxValue(9);
        } finally {
            legacyHighestCycle.close();
        }
        listing.refresh(true);

        assertEquals(7, listing.getMaxCreatedCycle());
        assertEquals(9, listing.getMaxCycleForWrite());
    }

    @Test
    public void readOnlyListingOpensMetadataWrittenBeforeTheWriteFloorKey() {
        final File legacyFile = new File(testDirectory, "legacy-list" + SingleTableStore.SUFFIX);
        // A pre-QUEUE-146 writer persists only the physical bounds and the modification count.
        try (TableStore<Metadata.NoMeta> legacyStore = SingleTableBuilder.binary(legacyFile, Metadata.NoMeta.INSTANCE).build()) {
            for (String key : new String[]{"listing.highestCycle", "listing.lowestCycle", "listing.modCount"}) {
                final LongValue value = legacyStore.acquireValueFor(key);
                try {
                    value.setOrderedValue("listing.modCount".equals(key) ? 1 : 7);
                } finally {
                    value.close();
                }
            }
        }

        try (TableStore<Metadata.NoMeta> readOnlyStore = SingleTableBuilder.binary(legacyFile, Metadata.NoMeta.INSTANCE).readOnly(true).build();
             TableDirectoryListingReadOnly readOnlyListing = new TableDirectoryListingReadOnly(readOnlyStore, SystemTimeProvider.INSTANCE)) {
            readOnlyListing.init();
            assertEquals(7, readOnlyListing.getMaxCreatedCycle());
            assertEquals(7, readOnlyListing.getMinCreatedCycle());
            assertEquals(7, readOnlyListing.getMaxCycleForWrite());
        }
    }

    @Test
    public void readOnlyQueueOpensMetadataWrittenBeforeTheWriteFloorKey() {
        assumeFalse("read-only queues are not supported on Windows", OS.isWindows());

        final File legacyQueueDirectory = new File(testDirectory, "legacy-queue");
        final int legacyCycle;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(legacyQueueDirectory).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("legacy payload");
            legacyCycle = queue.rollCycle().toCycle(appender.lastIndexAppended());
        }

        // Replace only the metadata table with its pre-QUEUE-146 shape. The genuine roll file above keeps this an
        // end-to-end Queue open rather than another direct TableDirectoryListing construction.
        final File metadataFile = new File(legacyQueueDirectory, SingleChronicleQueue.QUEUE_METADATA_FILE);
        assertTrue("failed to replace metadata with its legacy shape", metadataFile.delete());
        final SCQMeta legacyMetadata = new SCQMeta(new SCQRoll(RollCycles.DEFAULT, 0L, null, null), 0);
        try (TableStore<SCQMeta> legacyStore = SingleTableBuilder.binary(metadataFile, legacyMetadata).build()) {
            for (String key : new String[]{"listing.highestCycle", "listing.lowestCycle", "listing.modCount"}) {
                final LongValue value = legacyStore.acquireValueFor(key);
                try {
                    value.setOrderedValue("listing.modCount".equals(key) ? 1 : legacyCycle);
                } finally {
                    value.close();
                }
            }
            assertFalse(legacyStore.dump(WireType.BINARY_LIGHT).contains("listing.highestCycleWriteFloor"));
        }

        try (SingleChronicleQueue readOnlyQueue = SingleChronicleQueueBuilder.binary(legacyQueueDirectory)
                .testBlockSize()
                .readOnly(true)
                .build();
             ExcerptTailer tailer = readOnlyQueue.createTailer()) {
            assertEquals(legacyCycle, readOnlyQueue.firstCycle());
            assertEquals(legacyCycle, readOnlyQueue.lastCycle());
            assertEquals("legacy payload", tailer.readText());
        }
    }

    private int persistedCycle(final String key) {
        final LongValue value = tablestore.acquireValueFor(key);
        try {
            return (int) value.getVolatileValue();
        } finally {
            value.close();
        }
    }
}
