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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.onoes.ExceptionHandler;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public final class AppenderFileHandleLeakTest extends ChronicleQueueTestBase {
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private static final int MESSAGES_PER_THREAD = 50;
    private static final SystemTimeProvider SYSTEM_TIME_PROVIDER = SystemTimeProvider.INSTANCE;
    private static final RollCycle ROLL_CYCLE = RollCycles.TEST_SECONDLY;
    private static final DateTimeFormatter ROLL_CYCLE_FORMATTER = DateTimeFormatter.ofPattern(ROLL_CYCLE.format()).withZone(ZoneId.of("UTC"));

    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_COUNT,
            new NamedThreadFactory("test"));
    private final TrackingStoreFileListener storeFileListener = new TrackingStoreFileListener();
    private final AtomicLong currentTime = new AtomicLong(System.currentTimeMillis());
    private File queuePath;

    private static void readMessage(final ChronicleQueue queue,
                                    final boolean manuallyReleaseResources,
                                    final Consumer<ExcerptTailer> refHolder) {
        final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try (final ExcerptTailer tailer = queue.createTailer()) {
            while (bytes.isEmpty()) {
                tailer.toStart().readBytes(bytes);
            }
            refHolder.accept(tailer);
            assertTrue(Math.signum(bytes.readInt()) >= 0);

            if (manuallyReleaseResources) {
                tailer.close();
            }
        } finally {
            bytes.releaseLast();
        }
    }

    private static void writeMessage(final int j, final ChronicleQueue queue) {
        final ExcerptAppender appender = queue.acquireAppender();
        appender.writeBytes(b -> b.writeInt(j));
    }

    @Before
    public void setUp() {
        System.setProperty("chronicle.queue.disableFileShrinking", "true");
        queuePath = getTmpDir();
    }

    @Test
    public void appenderAndTailerResourcesShouldBeCleanedUpByGarbageCollection() throws InterruptedException, TimeoutException, ExecutionException {

        File file;

        assumeTrue(OS.isLinux() || OS.isMacOSX());

        try (ChronicleQueue queue = createQueue(SYSTEM_TIME_PROVIDER)) {

            file = queue.file();

            GcControls.requestGcCycle();
            Thread.sleep(100);
            final List<ExcerptTailer> gcGuard = new LinkedList<>();

            final List<Future<Boolean>> futures = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < MESSAGES_PER_THREAD; j++) {
                        writeMessage(j, queue);
                        readMessage(queue, false, gcGuard::add);
                    }
                    GcControls.requestGcCycle();
                    return Boolean.TRUE;
                }));
            }

            for (Future<Boolean> future : futures) {
                assertTrue(future.get(1, TimeUnit.MINUTES));
            }
            assertFalse(gcGuard.isEmpty());
            gcGuard.clear();

        }
        GcControls.waitForGcCycle();
        GcControls.waitForGcCycle();

        Assert.assertTrue(isFileHandleClosed(file));
    }

    @Test
    public void tailerResourcesCanBeReleasedManually() throws Exception {
        FlakyTestRunner.builder(this::tailerResourcesCanBeReleasedManually0).build().run();
    }

    public void tailerResourcesCanBeReleasedManually0() throws InterruptedException, TimeoutException, ExecutionException {

        File file;

        assumeTrue(OS.isLinux() || OS.isMacOSX());

        GcControls.requestGcCycle();
        Thread.sleep(100);
        try (ChronicleQueue queue = createQueue(SYSTEM_TIME_PROVIDER)) {
            file = queue.file();
            final List<Future<Boolean>> futures = new LinkedList<>();
            final List<ExcerptTailer> gcGuard = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < MESSAGES_PER_THREAD; j++) {
                        writeMessage(j, queue);
                        readMessage(queue, true, gcGuard::add);
                    }
                    return Boolean.TRUE;
                }));
            }

            for (Future<Boolean> future : futures) {
                assertTrue(future.get(1, TimeUnit.MINUTES));
            }

            assertFalse(gcGuard.isEmpty());
        }

        Assert.assertTrue(isFileHandleClosed(file));

    }

    @Test
    public void tailerShouldReleaseFileHandlesAsQueueRolls() throws InterruptedException {
        assumeTrue(OS.isLinux() || OS.isMacOSX());

        File file;

        System.gc();
        Thread.sleep(100);
        final int messagesPerThread = 10;
        try (ChronicleQueue queue = createQueue(currentTime::get)) {
            file = queue.file();

            for (int j = 0; j < messagesPerThread; j++) {
                writeMessage(j, queue);
                currentTime.addAndGet(500);
            }

            // StoreFileListener#onAcquired() is called on the background resource releaser thread
            BackgroundResourceReleaser.releasePendingResources();
            int acquiredBefore = storeFileListener.acquiredCounts.size();
            storeFileListener.reset();

            final ExcerptTailer tailer = queue.createTailer();
            tailer.toStart();
            int messageCount = 0;
            int notFoundAttempts = 5;
            while (true) {
                try (final DocumentContext ctx = tailer.readingDocument()) {
                    if (!ctx.isPresent()) {
                        if (--notFoundAttempts > 0)
                            continue;
                        break;
                    }

                    messageCount++;
                }
            }

            assertEquals(messagesPerThread, messageCount);

            // StoreFileListener#onAcquired() is called on the background resource releaser thread
            BackgroundResourceReleaser.releasePendingResources();
            Jvm.debug().on(getClass(), "storeFileListener " + storeFileListener);

            assertEquals(acquiredBefore, storeFileListener.acquiredCounts.size());

        }

        Assert.assertTrue(isFileHandleClosed(file));
    }

    @Test
    public void appenderShouldOnlyKeepCurrentRollCycleOpen() {
        assumeTrue(OS.isLinux() || OS.isMacOSX());

        AtomicLong timeProvider = new AtomicLong(1661323015000L);
        try (ChronicleQueue queue = createQueue(timeProvider::get)) {

            for (int j = 0; j < 10; j++) {
                writeMessage(j, queue);
                assertOnlyCurrentRollCycleIsOpen(timeProvider.get());
                timeProvider.addAndGet(1_000);
            }
        }
    }

    @Test
    public void tailerShouldOnlyKeepCurrentRollCycleOpen() {
        assumeTrue(OS.isLinux() || OS.isMacOSX());

        final long startTime = 1661323015000L;
        AtomicLong timeProvider = new AtomicLong(startTime);
        final int messageCount = 10;

        // populate the queue
        try (ChronicleQueue queue = createQueue(timeProvider::get)) {
            for (int j = 0; j < messageCount; j++) {
                writeMessage(j, queue);
                timeProvider.addAndGet(1_000);
            }
        }
        // there should be no file handles open now
        assertEquals(0, countMatchingLsofLines(line -> line.contains(queuePath.getAbsolutePath()), Jvm.warn()));

        timeProvider.set(startTime);

        // iterate through messages checking roll cycles are closed
        try (ChronicleQueue queue = createQueue(timeProvider::get);
             final ExcerptTailer tailer = queue.createTailer()) {
            IntStream.range(0, messageCount).forEach(i -> {
                tailer.readBytes(b -> assertEquals(i, b.readInt()));
                assertOnlyCurrentRollCycleIsOpen(timeProvider.get());
                timeProvider.addAndGet(1_000);
            });
        }
    }

    private void assertOnlyCurrentRollCycleIsOpen(long timestamp) {
        BackgroundResourceReleaser.releasePendingResources();
        final String currentRollCycleName = ROLL_CYCLE_FORMATTER.format(Instant.ofEpochMilli(timestamp)) + ".cq4";
        assertEquals("Found open files that are not the table store or the current roll cycle (" + currentRollCycleName + ")", 0,
                countMatchingLsofLines(
                        lsofLine -> lsofLine.contains(queuePath.getAbsolutePath())
                                && !(lsofLine.endsWith("metadata.cq4t") || lsofLine.endsWith(currentRollCycleName)), Jvm.warn()));
    }

    @Override
    public void assertReferencesReleased() {
        threadPool.shutdownNow();
        try {
            assertTrue(threadPool.awaitTermination(5L, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        super.assertReferencesReleased();
    }

    private static boolean isFileHandleClosed(File file) {
        return countMatchingLsofLines(str -> str.contains(file.getAbsolutePath()), Jvm.error()) == 0;
    }

    private static int countMatchingLsofLines(@NotNull Predicate<String> lsofLineMatcher, @NotNull ExceptionHandler logLevel) {
        Process plsof = null;
        try {
            plsof = new ProcessBuilder("lsof", "-p", String.valueOf(Jvm.getProcessId())).start();
            int matchingLines = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(plsof.getInputStream()))) {
                String line;
                while (plsof.isAlive()) {
                    line = reader.readLine();
                    if (line != null && lsofLineMatcher.test(line)) {
                        matchingLines++;
                        logLevel.on(AppenderFileHandleLeakTest.class, "Found matching line:\n" + line);
                    }
                }
                assertEquals("lsof call terminated with error", 0, plsof.exitValue());
            }
            return matchingLines;
        } catch (IOException e) {
            throw new RuntimeException("An error occurred reading command output", e);
        } finally {
            if (plsof != null)
                plsof.destroy();
        }
    }

    private ChronicleQueue createQueue(final TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.
                binary(queuePath).
                rollCycle(RollCycles.TEST_SECONDLY).
                wireType(WireType.BINARY_LIGHT).
                storeFileListener(storeFileListener).
                timeProvider(timeProvider).
                build();
    }

    private static final class TrackingStoreFileListener implements StoreFileListener {
        private final Map<String, Integer> acquiredCounts = new HashMap<>();
        private final Map<String, Integer> releasedCounts = new HashMap<>();

        @Override
        public void onAcquired(final int cycle, final File file) {
            acquiredCounts.put(file.getName(), acquiredCounts.getOrDefault(file.getName(), 0) + 1);
        }

        @Override
        public void onReleased(final int cycle, final File file) {
            releasedCounts.put(file.getName(), releasedCounts.getOrDefault(file.getName(), 0) + 1);
        }

        void reset() {
            acquiredCounts.clear();
            releasedCounts.clear();
        }

        @Override
        public String toString() {
            return String.format("%nacquired: %d%nreleased: %d%ndiffs:%n%s%n",
                    acquiredCounts.size(), releasedCounts.size(), buildDiffs());
        }

        private String buildDiffs() {
            final StringBuilder builder = new StringBuilder();
            builder.append("acquired but not released:\n");
            HashSet<String> keyDiff = new HashSet<>(acquiredCounts.keySet());
            keyDiff.removeAll(releasedCounts.keySet());
            keyDiff.forEach(k -> {
                builder.append(k).append("(").append(acquiredCounts.get(k)).append(")\n");
            });
            builder.append("released but not acquired:\n");
            keyDiff.clear();
            keyDiff.addAll(releasedCounts.keySet());
            keyDiff.removeAll(acquiredCounts.keySet());
            keyDiff.forEach(k -> {
                builder.append(k).append("(").append(releasedCounts.get(k)).append(")\n");
            });

            return builder.toString();
        }
    }
}