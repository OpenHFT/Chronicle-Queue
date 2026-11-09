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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
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

    /**
     * Simulates a read-only queue opening while a writer is still populating the metadata file: acquiring
     * listing.modCount fails on the first attempt, so init() retries. Values acquired by the failed attempt
     * must be closed rather than leaked.
     */
    @Test
    public void readOnlyInitRetryShouldNotLeakPartiallyAcquiredValues() {
        final List<LongValue> acquired = new ArrayList<>();
        final AtomicBoolean failedOnce = new AtomicBoolean();
        @SuppressWarnings("unchecked")
        final TableStore<Metadata.NoMeta> racingStore = (TableStore<Metadata.NoMeta>) Proxy.newProxyInstance(
                TableStore.class.getClassLoader(),
                new Class<?>[]{TableStore.class},
                (proxy, method, args) -> {
                    if ("acquireValueFor".equals(method.getName())
                            && "listing.modCount".contentEquals((CharSequence) args[0])
                            && failedOnce.compareAndSet(false, true))
                        throw new IllegalStateException("simulated: key not yet written by the writer");
                    final Object result;
                    try {
                        result = method.invoke(tablestore, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if ("acquireValueFor".equals(method.getName()))
                        acquired.add((LongValue) result);
                    return result;
                });

        final TableDirectoryListingReadOnly racingListing =
                new TableDirectoryListingReadOnly(racingStore, SystemTimeProvider.INSTANCE);
        racingListing.init();
        assertTrue("modCount acquisition should have failed once", failedOnce.get());
        assertEquals("2 values from the failed attempt + 3 from the retry", 5, acquired.size());

        racingListing.close();
        for (LongValue value : acquired)
            assertTrue("Not closed: " + value, ((Closeable) value).isClosed());
    }
}
