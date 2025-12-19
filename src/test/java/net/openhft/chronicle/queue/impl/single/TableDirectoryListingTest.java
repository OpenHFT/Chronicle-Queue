/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @BeforeEach
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

    public void shouldBlowUpIfClosed() {
        listing.close();
        assertThrows(IllegalStateException.class, listing::getMaxCreatedCycle, "directory listing: closed");
    }

    @Test
    public void shouldTrackMaxValue() {
        listing.refresh(true);

        listing.onFileCreated(tempFile, 7);

        assertEquals(7, listing.getMaxCreatedCycle(), "Expected max created cycle to be 7 after creating first file with cycle 7");
        assertEquals(7, listing.getMinCreatedCycle(), "Expected min created cycle to be 7 after creating first file with cycle 7");
        assertEquals(7, listingReadOnly.getMaxCreatedCycle(), "Expected read-only max created cycle to be 7 after first file creation");
        assertEquals(7, listingReadOnly.getMinCreatedCycle(), "Expected read-only min created cycle to be 7 after first file creation");

        listing.onFileCreated(tempFile, 8);

        assertEquals(8, listing.getMaxCreatedCycle(), "Expected max created cycle to be 8 after creating second file with cycle 8");
        assertEquals(7, listing.getMinCreatedCycle(), "Expected min created cycle to remain 7 after creating second file with cycle 8");
        assertEquals(8, listingReadOnly.getMaxCreatedCycle(), "Expected read-only max created cycle to be 8 after second file creation");
        assertEquals(7, listingReadOnly.getMinCreatedCycle(), "Expected read-only min created cycle to remain 7 after second file creation");
    }

    @Test
    public void shouldInitialiseFromFilesystem() throws IOException {
        new File(testDirectory, 1 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 2 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 3 + SingleChronicleQueue.SUFFIX).createNewFile();

        listing.refresh(true);

        assertEquals(3, listing.getMaxCreatedCycle(), "Expected max created cycle to be 3 after initializing from filesystem with cycles 1, 2, 3");
        assertEquals(1, listing.getMinCreatedCycle(), "Expected min created cycle to be 1 after initializing from filesystem with cycles 1, 2, 3");
        assertEquals(3, listingReadOnly.getMaxCreatedCycle(), "Expected read-only max created cycle to be 3 after filesystem initialization");
        assertEquals(1, listingReadOnly.getMinCreatedCycle(), "Expected read-only min created cycle to be 1 after filesystem initialization");
    }

    @Test
    public void lockShouldTimeOut() {
        listing.onFileCreated(tempFile, 8);

        listing.onFileCreated(tempFile, 9);
        assertEquals(9, listing.getMaxCreatedCycle(), "Expected max created cycle to be 9 after creating file with cycle 9, demonstrating lock acquisition succeeds");
        assertEquals(9, listingReadOnly.getMaxCreatedCycle(), "Expected read-only max created cycle to be 9 after lock operation completes");
    }
}
