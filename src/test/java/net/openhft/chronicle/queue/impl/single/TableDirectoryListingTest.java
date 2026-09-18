/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void shouldBlowUpIfClosed() {
        listing.close();
        assertThrows("Reading the maximum cycle must be rejected after the listing is closed",
                IllegalStateException.class, listing::getMaxCreatedCycle);
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
    public void closesPartialBindingsBeforeRetryingMissingLowestCycle() {
        assertPartialBindingsClosedBeforeRetry(1);
    }

    @Test
    public void closesPartialBindingsBeforeRetryingMissingModCount() {
        assertPartialBindingsClosedBeforeRetry(2);
    }

    @SuppressWarnings("unchecked")
    private void assertPartialBindingsClosedBeforeRetry(int missingKey) {
        String[] keys = {"listing.highestCycle", "listing.lowestCycle", "listing.modCount"};
        TableStore<Metadata.NoMeta> incompleteStore = createStrictMock(TableStore.class);
        List<LongValue> partialBindings = new ArrayList<>();
        List<LongValue> successfulBindings = new ArrayList<>();
        listing.onFileCreated(tempFile, 7);

        TableDirectoryListingReadOnly readOnly =
                new TableDirectoryListingReadOnly(incompleteStore, SystemTimeProvider.INSTANCE);
        try {
            // Use real mapped bindings; the store only controls when a key becomes visible.
            for (int i = 0; i < missingKey; i++) {
                LongValue value = tablestoreReadOnly.acquireValueFor(keys[i]);
                partialBindings.add(value);
                expect(incompleteStore.acquireValueFor(keys[i])).andReturn(value);
            }
            expect(incompleteStore.acquireValueFor(keys[missingKey]))
                    .andThrow(new IllegalStateException("Metadata key not yet published"));
            for (String key : keys) {
                LongValue value = tablestoreReadOnly.acquireValueFor(key);
                successfulBindings.add(value);
                expect(incompleteStore.acquireValueFor(key)).andAnswer(() -> {
                    for (LongValue partial : partialBindings)
                        assertTrue("Failed attempt must release bindings before retry", partial.isClosed());
                    return value;
                });
            }
            replay(incompleteStore);

            readOnly.init();

            assertEquals(7, readOnly.getMinCreatedCycle());
            assertEquals(7, readOnly.getMaxCreatedCycle());
            assertEquals(listing.modCount(), readOnly.modCount());
            for (LongValue value : successfulBindings)
                assertFalse("Successful bindings belong to the open listing", value.isClosed());
            verify(incompleteStore);
            readOnly.close();
            for (LongValue value : successfulBindings)
                assertTrue("Closing the listing releases successful bindings", value.isClosed());
        } finally {
            readOnly.close();
            // A failed control must not leak the real references it deliberately tracks.
            partialBindings.forEach(Closeable::closeQuietly);
            successfulBindings.forEach(Closeable::closeQuietly);
        }
    }
}
