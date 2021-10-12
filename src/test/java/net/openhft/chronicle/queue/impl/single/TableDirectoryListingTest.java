package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TableDirectoryListingTest extends ChronicleQueueTestBase {
    private TableDirectoryListing listing;
    private TableStore<Metadata.NoMeta> tablestore;
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
        listing = new TableDirectoryListing(tablestore,
                testDirectory.toPath(),
                f -> Integer.parseInt(f.split("\\.")[0]));
        listing.init();
        tempFile = File.createTempFile("foo", "bar");
        tempFile.deleteOnExit();
    }

    @Override
    public void preAfter() {
        tablestore.close();
        listing.close();
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

        listing.onFileCreated(tempFile, 8);

        assertEquals(8, listing.getMaxCreatedCycle());
        assertEquals(7, listing.getMinCreatedCycle());
    }

    @Test
    public void shouldInitialiseFromFilesystem() throws IOException {
        new File(testDirectory, 1 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 2 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 3 + SingleChronicleQueue.SUFFIX).createNewFile();

        listing.refresh(true);

        assertEquals(3, listing.getMaxCreatedCycle());
        assertEquals(1, listing.getMinCreatedCycle());
    }

    @Test
    public void lockShouldTimeOut() {
        listing.onFileCreated(tempFile, 8);

        listing.onFileCreated(tempFile, 9);
        assertEquals(9, listing.getMaxCreatedCycle());
    }
}