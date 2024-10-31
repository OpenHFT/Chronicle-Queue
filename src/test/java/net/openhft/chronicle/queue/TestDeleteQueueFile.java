/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Long.toHexString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@SuppressWarnings("this-escape")
public class TestDeleteQueueFile extends QueueTestCommon {

    private static final int NUM_REPEATS = 10;
    private static final int CYCLES_TO_DELETE_PER_ITERATION = 20;
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
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(queue.firstIndex()));
            assertEquals(toHexString(secondCycle.lastIndex), toHexString(queue.lastIndex()));

            // and create a tailer it should only read data in second file
            ExcerptTailer excerptTailer2 = queue.createTailer();
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(excerptTailer2.index()));
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
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(tailer.toStart().index()));
        }
    }

    @Ignore("https://github.com/OpenHFT/Chronicle-Queue/issues/1151")
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
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(tailer.toStart().index()));
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
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));

            // delete the last store
            Files.delete(Paths.get(thirdCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
        }
    }

    @Ignore("https://github.com/OpenHFT/Chronicle-Queue/issues/1151")
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
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));

            // delete the last store
            Files.delete(Paths.get(thirdCycle.filename));

            // should be at correct index
            assertEquals(toHexString(secondCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));
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
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(tailer.toStart().index()));
            assertEquals(toHexString(thirdCycle.lastIndex + 1), toHexString(tailer.toEnd().index()));

            // delete the first store
            Files.delete(Paths.get(firstCycle.filename));

            // using old cached value
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(queue.firstIndex()));
            assertEquals(toHexString(firstCycle.firstIndex), toHexString(queue.firstIndex()));

            // wait for cache to expire
            ((SetTimeProvider) queue.time()).advanceMillis(260);

            // using correct value
            assertEquals(toHexString(secondCycle.firstIndex), toHexString(queue.firstIndex()));
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

    @Test
    public void deletingOldFilesChaosTest() throws InterruptedException {
        ignoreException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");
        final int numberOfCycles = 300;
        final AtomicBoolean running = new AtomicBoolean(true);
        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(numberOfCycles, null)) {
            Thread backwardTailerThread = new Thread(() -> new QueueTailer(running, queueWithCycleDetails, TailerDirection.BACKWARD));
            Thread forwardTailerThread = new Thread(() -> new QueueTailer(running, queueWithCycleDetails, TailerDirection.FORWARD));
            Thread deleterThread = new Thread(() -> progressivelyTruncateOldRollCycles(queueWithCycleDetails));

            backwardTailerThread.start();
            forwardTailerThread.start();
            deleterThread.start();
            deleterThread.join();
            running.set(false);
            forwardTailerThread.join();
            backwardTailerThread.join();
        }
    }

    @Test
    public void deleteFileFromUnderTailerTest_StartOfRange() throws IOException {
        deleteFileFromUnderTailerTest(10, 0);
    }

    @Test
    public void deleteFileFromUnderTailerTest_MiddleOfRange() throws IOException {
        deleteFileFromUnderTailerTest(10, 5);
    }

    @Test
    public void deleteFileFromUnderTailerTest_EndOfRange() throws IOException {
        deleteFileFromUnderTailerTest(10, 8);
    }

    public void deleteFileFromUnderTailerTest(int numberOfCycles, int currentCycleIndex) throws IOException {
        assumeFalse(OS.isWindows());
        ignoreException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");
        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(numberOfCycles, null)) {
            try (final ExcerptTailer tailer = queueWithCycleDetails.queue.createTailer()) {
                final RollCycleDetails rollCycleBeingRead = queueWithCycleDetails.rollCycles.get(currentCycleIndex);
                final RollCycleDetails nextRollCycleToBeRead = queueWithCycleDetails.rollCycles.get(currentCycleIndex + 1);
                final long expectedLastIndexRead = queueWithCycleDetails.rollCycles.stream()
                        .filter(rc -> rc != nextRollCycleToBeRead)
                        .mapToLong(rc -> rc.lastIndex)
                        .reduce(-1, Long::max);
                tailer.moveToIndex(rollCycleBeingRead.firstIndex);
                long lastIndexRead;
                try (final DocumentContext documentContext = tailer.readingDocument()) {
                    // just read the record to ensure we've loaded the store
                    lastIndexRead = documentContext.index();
                }
                Jvm.startup().on(TestDeleteQueueFile.class, "First index read was " + toHexString(lastIndexRead));
                // Need to delete current and next cycle to trigger failure
                Files.delete(Paths.get(rollCycleBeingRead.filename));
                Files.delete(Paths.get(nextRollCycleToBeRead.filename));
                queueWithCycleDetails.queue.refreshDirectoryListing();
                // Now read through the rest of the queue
                while (true) {
                    try (final DocumentContext documentContext = tailer.readingDocument()) {
                        if (!documentContext.isPresent()) {
                            break;
                        }
                        lastIndexRead = documentContext.index();
                        // Do nothing
                    }
                }
                Jvm.startup().on(TestDeleteQueueFile.class, "Last index read was " + toHexString(lastIndexRead));
                assertEquals(toHexString(expectedLastIndexRead), toHexString(lastIndexRead));
            }
        }
    }

    private void progressivelyTruncateOldRollCycles(QueueWithCycleDetails queueWithCycleDetails) {
        try {
            int deletedUpTo = 0;
            // int numberOfCycles = queueWithCycleDetails.rollCycles.size();
            while (!queueWithCycleDetails.rollCycles.isEmpty()) {
                Jvm.startup().on(TestDeleteQueueFile.class, "Deleting from " + deletedUpTo + " to " + (deletedUpTo + CYCLES_TO_DELETE_PER_ITERATION));
                for (int i = 0; i < CYCLES_TO_DELETE_PER_ITERATION; i++) {
                    final RollCycleDetails rollCycleDetails = queueWithCycleDetails.rollCycles.remove(0);
                    // Jvm.startup().on(TestDeleteQueueFile.class, "Deleting " + rollCycleDetails.filename + " (" + deletedUpTo + "/" + numberOfCycles + "), firstIndex=" + toHexString(rollCycleDetails.firstIndex) + ", lastIndex=" + toHexString(rollCycleDetails.lastIndex));
                    Files.delete(Paths.get(rollCycleDetails.filename));
                    deletedUpTo++;
                }
                queueWithCycleDetails.queue.refreshDirectoryListing();
                Jvm.pause(1_000);
            }
        } catch (IOException e) {
            Jvm.error().on(TestDeleteQueueFile.class, "Error occurred", e);
        }
    }

    private void deleteAllRollCyclesInRandomOrder(QueueWithCycleDetails queueWithCycleDetails) {
        try {
            int numberOfCycles = queueWithCycleDetails.rollCycles.size();
            int deleted = 0;
            while (queueWithCycleDetails.rollCycles.size() > 1) {
                // Don't delete the first cycle, we can't deal with that yet
                final int index = ThreadLocalRandom.current().nextInt(1, queueWithCycleDetails.rollCycles.size());
                final RollCycleDetails rollCycleDetails = queueWithCycleDetails.rollCycles.remove(index);
                deleted++;
                Jvm.startup().on(TestDeleteQueueFile.class, "Deleting " + rollCycleDetails.rollCycle + ": " + rollCycleDetails.filename + " (" + deleted + "/" + numberOfCycles + "), firstIndex=" + toHexString(rollCycleDetails.firstIndex) + ", lastIndex=" + toHexString(rollCycleDetails.lastIndex));
                Files.delete(Paths.get(rollCycleDetails.filename));
                queueWithCycleDetails.queue.refreshDirectoryListing();
                Jvm.pause(20);
            }
        } catch (IOException e) {
            Jvm.error().on(TestDeleteQueueFile.class, "Error occurred", e);
        }
    }

    @Test
    public void deletingRandomRollCyclesChaosTest() throws InterruptedException {
        assumeFalse(OS.isWindows());
        ignoreException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");
        final int numberOfCycles = 300;
        final AtomicBoolean running = new AtomicBoolean(true);
        try (QueueWithCycleDetails queueWithCycleDetails = createQueueWithNRollCycles(numberOfCycles, null)) {

            Thread forwardTailerThread = new Thread(new QueueTailer(running, queueWithCycleDetails, TailerDirection.FORWARD));
            Thread backwardTailerThread = new Thread(new QueueTailer(running, queueWithCycleDetails, TailerDirection.BACKWARD));

            Thread deleteRandomCyclesThread = new Thread(() -> deleteAllRollCyclesInRandomOrder(queueWithCycleDetails));

            backwardTailerThread.start();
            forwardTailerThread.start();
            deleteRandomCyclesThread.start();
            deleteRandomCyclesThread.join();
            running.set(false);
            backwardTailerThread.join();
            forwardTailerThread.join();
        }
    }

    private static class QueueTailer implements Runnable {

        private final AtomicBoolean running;
        private final QueueWithCycleDetails queueWithCycleDetails;
        private final TailerDirection direction;

        QueueTailer(AtomicBoolean running, QueueWithCycleDetails queueWithCycleDetails, TailerDirection direction) {
            this.running = running;
            this.queueWithCycleDetails = queueWithCycleDetails;
            this.direction = direction;
        }

        @Override
        public void run() {
            try {
                while (running.get()) {
                    try (final ExcerptTailer tailer = queueWithCycleDetails.queue.createTailer().direction(direction)) {
                        if (direction == TailerDirection.BACKWARD) {
                            tailer.toEnd();
                        } else {
                            tailer.toStart();
                        }
                        Jvm.startup().on(TestDeleteQueueFile.class, direction + " Tailer starting at index=" + toHexString(tailer.index()) + ", cycle=" + queueWithCycleDetails.queue.rollCycle().toCycle(tailer.index()));
                        int cyclesRead = 0;
                        long lastReadIndex = -5;
                        int currentCycle = -1;
                        while (running.get()) {
                            try (final DocumentContext documentContext = readingDocumentWithRetries(tailer)) {
                                if (!documentContext.isPresent()) {
                                    logIterationResult(direction, tailer, cyclesRead, lastReadIndex);
                                    break;
                                }
                                lastReadIndex = documentContext.index();
                                final int cycle = queueWithCycleDetails.queue.rollCycle().toCycle(lastReadIndex);
                                if (cycle != currentCycle) {
                                    Jvm.startup().on(TestDeleteQueueFile.class, direction + " reading cycle " + cycle);
                                    currentCycle = cycle;
                                    cyclesRead++;
                                }
                            } catch (RuntimeException e) {
                                Jvm.error().on(TestDeleteQueueFile.class, "Failed after reading " + lastReadIndex);
                                throw e;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                Jvm.error().on(TestDeleteQueueFile.class, "Error occurred", e);
            }
            Jvm.startup().on(TestDeleteQueueFile.class, "Tailer thread terminated: " + direction);
        }

        private DocumentContext readingDocumentWithRetries(ExcerptTailer excerptTailer) {
            DocumentContext documentContext = null;
            for (int i = 0; i < 2; i++) {
                documentContext = excerptTailer.readingDocument();
                if (documentContext.isPresent()) {
                    break;
                }
            }
            return documentContext;
        }

        private OptionalLong lastAvailableIndex() {
            return queueWithCycleDetails.rollCycles.stream().mapToLong(rc -> rc.lastIndex).reduce(Long::max);
        }

        private OptionalLong firstAvailableIndex() {
            return queueWithCycleDetails.rollCycles.stream().mapToLong(rc -> rc.firstIndex).findFirst();
        }

        private int remainingCycles() {
            return queueWithCycleDetails.rollCycles.size();
        }

        private void logIterationResult(TailerDirection direction, ExcerptTailer tailer, int cyclesRead, long lastReadIndex) {
            final int remainingCycles = remainingCycles();
            // Check we read at least the number of cycles remaining now
            if (cyclesRead < remainingCycles) {
                Jvm.error().on(TestDeleteQueueFile.class, direction + " didn't read all remaining cycles cyclesRead=" + cyclesRead + ", cyclesRemaining=" + remainingCycles);
            }
            // Check we got to the end if we're moving forward
            if (direction == TailerDirection.FORWARD) {
                lastAvailableIndex().ifPresent(lastIndex -> {
                    if (lastReadIndex < lastIndex) {
                        logError(tailer, lastIndex, cyclesRead);
                    }
                });
                return;
            }
            // Check we got to the start if we're moving backwards
            if (direction == TailerDirection.BACKWARD) {
                firstAvailableIndex().ifPresent(firstIndex -> {
                    if (lastReadIndex > firstIndex) {
                        logError(tailer, firstIndex, cyclesRead);
                    }
                });
                return;
            }
            // Otherwise report what we read
            Jvm.startup().on(TestDeleteQueueFile.class, direction + " Tailer read " + cyclesRead + " cycles of " + remainingCycles + " remaining (read should always be >= remaining)");
        }

        private void logError(ExcerptTailer tailer, long lastIndex, int cyclesRead) {
            String firstLast = direction == TailerDirection.BACKWARD ? "first" : "last";
            String error = String.format("Didn't get to %s. lastReadIndex=%x, lastReadCycle=%d, %sIndex=%x, %sCycle=%d, cyclesRead=%d",
                    direction == TailerDirection.BACKWARD ? "start" : "end",
                    tailer.lastReadIndex(),
                    queueWithCycleDetails.queue.rollCycle().toCycle(tailer.lastReadIndex()),
                    firstLast,
                    lastIndex,
                    firstLast,
                    queueWithCycleDetails.queue.rollCycle().toCycle(lastIndex),
                    cyclesRead);
            Jvm.error().on(TestDeleteQueueFile.class, error);
        }
    }

    public void tailingThroughDeletedCyclesWillRefreshThenRetry(Function<QueueWithCycleDetails, SingleChronicleQueue> queueCreator) throws IOException {
        assumeFalse(OS.isWindows());
        expectException("The current cycle seems to have been deleted from under the queue, scanning to find the next remaining cycle");

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
                .testBlockSize()
                .storeFileListener(listener);
        if (builderConsumer != null) {
            builderConsumer.accept(queueBuilder);
        }
        SingleChronicleQueue queue = queueBuilder
                .build();
        List<RollCycleDetails> rollCycleDetails;
        try (ExcerptAppender appender = queue.createAppender()) {

            assertEquals(Long.MAX_VALUE, queue.firstIndex());

            rollCycleDetails = IntStream.range(0, numberOfCycles)
                    .mapToObj(i -> {
                        long firstIndexInCycle = writeTextAndReturnFirstIndex(appender, "test" + (i + 1));
                        long lastIndexInCycle = appender.lastIndexAppended();
                        timeProvider.advanceMillis(TimeUnit.DAYS.toMillis(1));
                        BackgroundResourceReleaser.releasePendingResources();
                        return new RollCycleDetails(
                                queue.rollCycle().toCycle(firstIndexInCycle),
                                firstIndexInCycle,
                                lastIndexInCycle,
                                listener.lastFileAcquired.getAbsolutePath());
                    }).collect(Collectors.toList());
        }

        // There should be 3 acquired files, for roll cycles 1, 2, 3
        assertEquals(numberOfCycles, rollCycleDetails.size());

        // now let's create one tailer which will read all content
        try (ExcerptTailer excerptTailer = queue.createTailer()) {
            for (int i = 0; i < numberOfCycles; i++) {
                readText(excerptTailer, "test" + (i + 1));
            }
        }

        return new QueueWithCycleDetails(queue, new CopyOnWriteArrayList<>(rollCycleDetails));
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
        final int rollCycle;
        final long firstIndex;
        final long lastIndex;
        final String filename;

        RollCycleDetails(int rollCycle, long firstIndex, long lastIndex, String filename) {
            this.rollCycle = rollCycle;
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
            Jvm.debug().on(TestDeleteQueueFile.class, "onAcquired called cycle: " + cycle + ", file: " + file);
            lastFileAcquired = file;
        }
    }
}
