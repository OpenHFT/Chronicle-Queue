package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import net.openhft.chronicle.testframework.Waiters;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
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

    /**
     * These only run on Linux because {@link MappedFileUtil#getAllMappedFiles()} only works
     * on Linux
     */
    @Before
    public void setUp() {
        assumeTrue(OS.isLinux());

        System.setProperty("chronicle.queue.disableFileShrinking", "true");
        queuePath = getTmpDir();
    }

    @Test
    public void appenderAndTailerResourcesShouldBeCleanedUpByGarbageCollection() throws InterruptedException, TimeoutException, ExecutionException {
        try (ChronicleQueue queue = createQueue(SYSTEM_TIME_PROVIDER)) {

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

        Assert.assertTrue(queueFilesAreAllClosed());
    }

    @Test
    public void tailerResourcesCanBeReleasedManually() throws Exception {
        FlakyTestRunner.builder(this::tailerResourcesCanBeReleasedManually0)
                .build()
                .run();
    }

    public void tailerResourcesCanBeReleasedManually0() throws InterruptedException, TimeoutException, ExecutionException {
        GcControls.requestGcCycle();
        Thread.sleep(100);
        try (ChronicleQueue queue = createQueue(SYSTEM_TIME_PROVIDER)) {
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

        Assert.assertTrue(queueFilesAreAllClosed());

    }

    @Test
    public void tailerShouldReleaseFileHandlesAsQueueRolls() throws InterruptedException {

        System.gc();
        Thread.sleep(100);
        final int messagesPerThread = 10;
        try (ChronicleQueue queue = createQueue(currentTime::get)) {

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

        Assert.assertTrue(queueFilesAreAllClosed());
    }

    @Test
    public void appenderShouldOnlyKeepCurrentRollCycleOpen_deflaked() {
        FlakyTestRunner.<RuntimeException>builder(this::appenderShouldOnlyKeepCurrentRollCycleOpen)
                .withMaxIterations(3)
                .build()
                .run();
    }

    public void appenderShouldOnlyKeepCurrentRollCycleOpen() {
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
    public void tailerShouldOnlyKeepCurrentRollCycleOpen_deflaked() {
        FlakyTestRunner.<RuntimeException>builder(this::tailerShouldOnlyKeepCurrentRollCycleOpen)
                .withMaxIterations(3)
                .build()
                .run();
    }

    public void tailerShouldOnlyKeepCurrentRollCycleOpen() {
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
        assertTrue(queueFilesAreAllClosed());

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
        /*
         * "A mapped byte buffer and the file mapping that it represents remain valid until the buffer itself is garbage-collected."
         *
         * Given we can't guarantee a GC happens, I wonder if this test can ever not be flaky
         *
         * See https://docs.oracle.com/javase/8/docs/api/java/nio/MappedByteBuffer.html
         */
        GcControls.waitForGcCycle();
        final String currentRollCycleName = ROLL_CYCLE_FORMATTER.format(Instant.ofEpochMilli(timestamp)) + ".cq4";
        final String absolutePathToCurrentRollCycle = queuePath.toPath().toAbsolutePath().resolve(currentRollCycleName).toString();
        Waiters.builder(() -> onlyCurrentRollCycleIsOpen(absolutePathToCurrentRollCycle))
                .message("Files that are not the table store or the current roll cycle (" + currentRollCycleName + ") remain open")
                .maxTimeToWaitMs(5_500)
                .checkIntervalMs(1_000)
                .run();
    }

    private boolean onlyCurrentRollCycleIsOpen(String absolutePathToCurrentRollCycle) {
        Set<String> mappedFiles = MappedFileUtil.getAllMappedFiles();
        List<String> rollCyclesOpen = mappedFiles.stream().filter(
                        lsofLine -> lsofLine.contains(queuePath.getAbsolutePath())
                                && !(lsofLine.endsWith("metadata.cq4t")))
                .collect(Collectors.toList());
        boolean onlyCurrentFileIsOpen = rollCyclesOpen.contains(absolutePathToCurrentRollCycle) && rollCyclesOpen.size() == 1;
        if (!onlyCurrentFileIsOpen) {
            rollCyclesOpen.forEach(line -> Jvm.warn().on(AppenderFileHandleLeakTest.class, "Found file open:\n" + line));
        }
        return onlyCurrentFileIsOpen;
    }

    @Override
    public void assertReferencesReleased() {
        threadPool.shutdownNow();
        try {
            assertTrue(threadPool.awaitTermination(5L, SECONDS));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        super.assertReferencesReleased();
    }

    private boolean queueFilesAreAllClosed() {
        GcControls.waitForGcCycle();
        final List<String> openQueueFiles = MappedFileUtil.getAllMappedFiles().stream()
                .filter(str -> str.contains(queuePath.getAbsolutePath()))
                .collect(Collectors.toList());
        openQueueFiles.forEach(qf -> Jvm.error().on(AppenderFileHandleLeakTest.class, "Found open queue file: " + qf));
        return openQueueFiles.isEmpty();
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