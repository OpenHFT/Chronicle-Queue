package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class TestDeleteQueueFile extends ChronicleQueueTestBase {

    private static final int NUM_REPEATS = 10;
    private final Path tempQueueDir = getTmpDir().toPath();

    @Test
    public void testRefreshDirectoryListingWillUpdateFirstAndLastIndicesCorrectly() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            // delete the first and last files
            Files.delete(Paths.get(queueWithCycleDetails.rollCycles.get(0).filename));
            Files.delete(Paths.get(queueWithCycleDetails.rollCycles.get(2).filename));

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            queue.refreshDirectoryListing();

            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            assertEquals(Long.toHexString(secondCycle.firstIndex), Long.toHexString(queue.firstIndex()));
            assertEquals(Long.toHexString(secondCycle.lastIndex), Long.toHexString(queue.lastIndex()));

            // and create a tailer it should only read data in second file
            ExcerptTailer excerptTailer2 = queue.createTailer();
            assertEquals(Long.toHexString(secondCycle.firstIndex), Long.toHexString(excerptTailer2.index()));
            readText(excerptTailer2, "test2");
        }
    }

    @Test
    public void tailerToStartWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(Long.toHexString(firstCycle.firstIndex), Long.toHexString(tailer.toStart().index()));
            assertEquals(Long.toHexString(thirdCycle.lastIndex + 1), Long.toHexString(tailer.toEnd().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // should be at correct index
            assertEquals(Long.toHexString(secondCycle.firstIndex), Long.toHexString(tailer.toStart().index()));
        }
    }

    @Ignore("still doesn't work")
    @Test
    public void tailerToStartFromStartWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(Long.toHexString(thirdCycle.lastIndex + 1), Long.toHexString(tailer.toEnd().index()));
            assertEquals(Long.toHexString(firstCycle.firstIndex), Long.toHexString(tailer.toStart().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // should be at correct index
            assertEquals(Long.toHexString(secondCycle.firstIndex), Long.toHexString(tailer.toStart().index()));
        }
    }

    @Test
    public void tailerToEndWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(Long.toHexString(thirdCycle.lastIndex + 1), Long.toHexString(tailer.toEnd().index()));
            assertEquals(Long.toHexString(firstCycle.firstIndex), Long.toHexString(tailer.toStart().index()));

            // delete the last store
            Files.delete(Paths.get(thirdCycle.filename));

            // should be at correct index
            assertEquals(Long.toHexString(secondCycle.lastIndex + 1), Long.toHexString(tailer.toEnd().index()));
        }
    }

    @Test
    public void tailerToEndFromEndWorksInFaceOfDeletedStoreFile() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null)) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(Long.toHexString(firstCycle.firstIndex), Long.toHexString(tailer.toStart().index()));
            assertEquals(Long.toHexString(thirdCycle.lastIndex + 1), Long.toHexString(tailer.toEnd().index()));

            // delete the last store
            Files.delete(Paths.get(thirdCycle.filename));

            // should be at correct index
            assertEquals(Long.toHexString(secondCycle.lastIndex + 1), Long.toHexString(tailer.toEnd().index()));
        }
    }

    @Test
    public void firstAndLastIndexAreRefreshedAfterForceRefreshInterval() throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, builder -> builder.forceDirectoryListingRefreshIntervalMs(250))) {

            final SingleChronicleQueue queue = queueWithCycleDetails.queue;
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // while the queue is intact
            assertEquals(Long.toHexString(firstCycle.firstIndex), Long.toHexString(tailer.toStart().index()));
            assertEquals(Long.toHexString(thirdCycle.lastIndex + 1), Long.toHexString(tailer.toEnd().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // using old cached value
            assertEquals(Long.toHexString(firstCycle.firstIndex), Long.toHexString(queue.firstIndex()));
            assertEquals(Long.toHexString(firstCycle.firstIndex), Long.toHexString(queue.firstIndex()));

            // wait for cache to expire
            Jvm.pause(260);

            // using correct value
            assertEquals(Long.toHexString(secondCycle.firstIndex), Long.toHexString(queue.firstIndex()));
        }
    }

    @Test
    public void tailingThroughDeletedCyclesWillRefreshThenRetry_Writable() throws IOException {
        tailingThroughDeletedCyclesWillRefreshThenRetry(qwcd -> qwcd.queue);
    }

    @Test
    public void tailingThroughDeletedCyclesWillRefreshThenRetry_ReadOnly() throws IOException {
        tailingThroughDeletedCyclesWillRefreshThenRetry(qwcd -> SingleChronicleQueueBuilder.binary(qwcd.queue.fileAbsolutePath())
                .rollCycle(RollCycles.FAST_DAILY)
                .readOnly(true)
                .build());
    }

    public void tailingThroughDeletedCyclesWillRefreshThenRetry(Function<QueueWithCycleDetails, SingleChronicleQueue> queueCreator) throws IOException {
        assumeFalse(OS.isWindows());

        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(3, null);
             SingleChronicleQueue queue = queueCreator.apply(queueWithCycleDetails)
        ) {
            RollCycleDetails firstCycle = queueWithCycleDetails.rollCycles.get(0);
            RollCycleDetails secondCycle = queueWithCycleDetails.rollCycles.get(1);
            RollCycleDetails thirdCycle = queueWithCycleDetails.rollCycles.get(2);

            ExcerptTailer tailer = queue.createTailer();

            // delete the store files
            Files.delete(Paths.get(firstCycle.filename));
            Files.delete(Paths.get(secondCycle.filename));
            Files.delete(Paths.get(thirdCycle.filename));

            int counter = 0;
            while (true) {
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    if (!documentContext.isPresent()) {
                        break;
                    }
                    counter++;
                }
            }
            assertEquals(10, counter); // we still get 10 because the current store is in memory
        }
    }

    /**
     * Create a queue with N roll cycles
     *
     * @param numberOfCycles  The number of cycles to create
     * @param builderConsumer A consumer that can optionally modify the queue settings
     * @return The queue and the details of the roll cycles created
     */
    private QueueWithCycleDetails createQueueWithNRollCycles(int numberOfCycles, Consumer<SingleChronicleQueueBuilder> builderConsumer) {
        SetTimeProvider timeProvider = new SetTimeProvider();
        QueueStoreFileListener listener = new QueueStoreFileListener();
        final SingleChronicleQueueBuilder queueBuilder = SingleChronicleQueueBuilder.binary(tempQueueDir.resolve("unitTestQueue"))
                .rollCycle(RollCycles.FAST_DAILY)
                .timeProvider(timeProvider)
                .storeFileListener(listener);
        if (builderConsumer != null) {
            builderConsumer.accept(queueBuilder);
        }
        SingleChronicleQueue queue = queueBuilder
                .build();

        ExcerptAppender appender = queue.acquireAppender();

        assertEquals(Long.MAX_VALUE, queue.firstIndex());

        final List<RollCycleDetails> rollCycleDetails = IntStream.range(0, numberOfCycles)
                .mapToObj(i -> {
                    long firstIndexInCycle = writeTextAndReturnFirstIndex(appender, "test" + (i + 1));
                    long lastIndexInCycle = appender.lastIndexAppended();
                    timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));
                    BackgroundResourceReleaser.releasePendingResources();
                    return new RollCycleDetails(firstIndexInCycle, lastIndexInCycle, listener.lastFileAcquired.getAbsolutePath());
                }).collect(Collectors.toList());

        // There should be 3 acquired files, for roll cycles 1, 2, 3
        Assert.assertEquals(numberOfCycles, rollCycleDetails.size());

        // now let's create one tailer which will read all content
        ExcerptTailer excerptTailer = queue.createTailer();
        for (int i = 0; i < numberOfCycles; i++) {
            readText(excerptTailer, "test" + (i + 1));
        }

        return new QueueWithCycleDetails(queue, rollCycleDetails);
    }

    static class QueueWithCycleDetails extends AbstractCloseable {
        final SingleChronicleQueue queue;
        final List<RollCycleDetails> rollCycles;

        QueueWithCycleDetails(SingleChronicleQueue queue, List<RollCycleDetails> rollCycles) {
            this.queue = queue;
            this.rollCycles = rollCycles;
        }

        @Override
        protected void performClose() throws IllegalStateException {
            Closeable.closeQuietly(queue);
        }
    }

    static class RollCycleDetails {
        final long firstIndex;
        final long lastIndex;
        final String filename;

        RollCycleDetails(long firstIndex, long lastIndex, String filename) {
            this.firstIndex = firstIndex;
            this.lastIndex = lastIndex;
            this.filename = filename;
        }
    }

    /**
     * Write the specified text the specified number of times, return the index of the first entry written
     */
    private long writeTextAndReturnFirstIndex(ExcerptAppender appender, String text) {
        long firstIndex = -1;
        for (int i = 0; i < NUM_REPEATS; i++) {
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
    private void readText(ExcerptTailer tailer, String text) {
        for (int i = 0; i < NUM_REPEATS; i++) {
            assertEquals(text, tailer.readText());
        }
    }

    static final class QueueStoreFileListener implements StoreFileListener {

        private File lastFileAcquired;

        @Override
        public void onReleased(int cycle, File file) {
        }

        @Override
        public void onAcquired(int cycle, File file) {
            System.out.println("onAcquired called cycle: " + cycle + ", file: " + file);
            lastFileAcquired = file;
        }
    }
}