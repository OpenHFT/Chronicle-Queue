package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TableDirectoryListingTest {
    private TableDirectoryListing listing;
    private File testDirectory;
    private File tempFile;

    @NotNull
    private static File testDirectory() {
        return DirectoryUtils.tempDir(TableDirectoryListingTest.class.getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        testDirectory = testDirectory();
        testDirectory.mkdirs();
        File tableFile = new File(testDirectory, "dir-list" + SingleTableBuilder.SUFFIX);
        listing = new TableDirectoryListing(SingleTableBuilder.
                binary(tableFile).build(),
                testDirectory.toPath(),
                f -> Integer.parseInt(f.getName().split("\\.")[0]),
                false);
        listing.init();
        tempFile = File.createTempFile("foo", "bar");
        tempFile.deleteOnExit();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldBlowUpIfClosed() {
        listing.close();
        listing.getMaxCreatedCycle();
    }

    @Test
    public void shouldTrackMaxValue() throws Exception {
        listing.refresh();

        listing.onFileCreated(tempFile, 7);

        assertThat(listing.getMaxCreatedCycle(), is(7));
        assertThat(listing.getMinCreatedCycle(), is(7));

        listing.onFileCreated(tempFile, 8);

        assertThat(listing.getMaxCreatedCycle(), is(8));
        assertThat(listing.getMinCreatedCycle(), is(7));
    }

    @Test
    public void shouldInitialiseFromFilesystem() throws Exception {
        new File(testDirectory, 1 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 2 + SingleChronicleQueue.SUFFIX).createNewFile();
        new File(testDirectory, 3 + SingleChronicleQueue.SUFFIX).createNewFile();

        listing.refresh();

        assertThat(listing.getMaxCreatedCycle(), is(3));
        assertThat(listing.getMinCreatedCycle(), is(1));
    }

    @Test
    public void lockShouldTimeOut() {
        listing.onFileCreated(tempFile, 8);

        listing.onFileCreated(tempFile, 9);
        assertThat(listing.getMaxCreatedCycle(), is(9));
    }
}