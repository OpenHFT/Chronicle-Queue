package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class TestDeleteQueueFile extends ChronicleQueueTestBase {

    private final Path tempQueueDir = getTmpDir().toPath();

    @Test
    public void testQueueFileDeletionWhileInUse() throws IOException {
        assumeFalse(OS.isWindows());

        SetTimeProvider timeProvider = new SetTimeProvider();
        QueueStoreFileListener listener = new QueueStoreFileListener();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempQueueDir.resolve("unitTestQueue"))
                .timeProvider(timeProvider)
                .storeFileListener(listener)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

            assertEquals(Long.MAX_VALUE, queue.firstIndex());

            // write 10 records should go to first day file
            writeTextAndReturnFirstIndex(appender, 10, "test");

            // roll to next day file
            timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));

            // write 5 records to second roll cycle
            long firstIndexOfSecondCycle = writeTextAndReturnFirstIndex(appender, 5, "test2");

            BackgroundResourceReleaser.releasePendingResources();

            // There should be a single released file and a single acquired-but-not-released file
            String firstFile = getOnlyItem(listener.getReleasedFiles());
            String secondFile = getOnlyItem(listener.getAcquiredButNotReleasedFiles());
            Assert.assertNotEquals(firstFile, secondFile);

            long indexAfter15Records = appender.lastIndexAppended();

            // now let's create one tailer which will read all content
            ExcerptTailer excerptTailer = queue.createTailer();
            readText(excerptTailer, 10, "test");
            readText(excerptTailer, 5, "test2");

            assertEquals(indexAfter15Records, excerptTailer.index() - 1);

            // lets delete first file
            Files.delete(Paths.get(firstFile));

            queue.refreshDirectoryListing();

            assertEquals(queue.firstIndex(), firstIndexOfSecondCycle);

            // and create a tailer it should only read data in second file
            ExcerptTailer excerptTailer2 = queue.createTailer();
            assertEquals(firstIndexOfSecondCycle, excerptTailer2.index());
            readText(excerptTailer2, 5, "test2");
        }
    }

    @Test
    public void firstAndLastIndicesAreRefreshedAfterForceDirectoryListingRefreshInterval() throws IOException {
        assumeFalse(OS.isWindows());

        SetTimeProvider timeProvider = new SetTimeProvider();
        QueueStoreFileListener listener = new QueueStoreFileListener();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempQueueDir.resolve("unitTestQueue"))
                .timeProvider(timeProvider)
                .storeFileListener(listener)
                .forceDirectoryListingRefreshIntervalMs(200)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

            assertEquals(Long.MAX_VALUE, queue.firstIndex());

            // write 10 records should go to first day file
            writeTextAndReturnFirstIndex(appender, 10, "test");

            // roll to next day file
            timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));

            // write 5 records to second roll cycle
            writeTextAndReturnFirstIndex(appender, 5, "test2");

            BackgroundResourceReleaser.releasePendingResources();

            // There should be a single released file and a single acquired-but-not-released file
            String firstFile = getOnlyItem(listener.getReleasedFiles());
            String secondFile = getOnlyItem(listener.getAcquiredButNotReleasedFiles());
            Assert.assertNotEquals(firstFile, secondFile);

            final ExcerptTailer tailer = queue.createTailer();

            // delete the roll cycle files
            Files.delete(Paths.get(firstFile));
            Files.delete(Paths.get(secondFile));

            // this will throw
            try {
                tailer.toEnd();
                fail("tailer.toEnd() used to fail when files were deleted under it: " + tailer.index());
            } catch (IllegalStateException expected) {
                // do nothing
            }

            Jvm.pause(250);

            // this will succeed
            assertEquals(0, tailer.toEnd().index());
            assertEquals(0, tailer.toStart().index());
        }
    }

    /**
     * Write the specified text the specified number of times, return the index of the first entry written
     */
    private long writeTextAndReturnFirstIndex(ExcerptAppender appender, int times, String text) {
        long firstIndex = -1;
        for (int i = 0; i < times; i++) {
            appender.writeText(text);
            if (firstIndex < 0) {
                firstIndex = appender.lastIndexAppended();
            }
        }
        return firstIndex;
    }

    /**
     * Read the specified text the specified number of times
     */
    private void readText(ExcerptTailer tailer, int times, String text) {
        for (int i = 0; i < times; i++) {
            assertEquals(text, tailer.readText());
        }
    }

    /**
     * Assert that a set contains a single item and retrieve it
     */
    private <T> T getOnlyItem(Set<T> files) {
        if (files.size() != 1) {
            throw new AssertionError("Expected a single entry, got: " + files);
        }
        return files.stream().findFirst().orElseThrow(() -> new AssertionError("This should never happen"));
    }

    static final class QueueStoreFileListener implements StoreFileListener {

        private final Set<String> releasedFiles = new HashSet<>();
        private final Set<String> acquiredButNotReleasedFiles = new HashSet<>();

        @Override
        public void onReleased(int cycle, File file) {
            System.out.println("onReleased called cycle: " + cycle + ", file: " + file);
            releasedFiles.add(file.getAbsolutePath());

            //update acquire file map
            acquiredButNotReleasedFiles.remove(file.getAbsolutePath());
        }

        @Override
        public void onAcquired(int cycle, File file) {
            System.out.println("onAcquired called cycle: " + cycle + ", file: " + file);
            acquiredButNotReleasedFiles.add(file.getAbsolutePath());
        }

        public Set<String> getAcquiredButNotReleasedFiles() {
            return acquiredButNotReleasedFiles;
        }

        public Set<String> getReleasedFiles() {
            return releasedFiles;
        }
    }
}