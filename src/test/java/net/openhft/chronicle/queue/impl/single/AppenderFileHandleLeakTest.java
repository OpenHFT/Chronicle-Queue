package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;

public final class AppenderFileHandleLeakTest {
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private static final int MESSAGES_PER_THREAD = 50;
    private static final SystemTimeProvider SYSTEM_TIME_PROVIDER = SystemTimeProvider.INSTANCE;

    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
    private final List<Path> lastFileHandles = new ArrayList<>();
    private TrackingStoreFileListener storeFileListener = new TrackingStoreFileListener();
    private AtomicLong currentTime = new AtomicLong(System.currentTimeMillis());
    private File queuePath;

    private static Matcher<Integer> withinDelta(final int expected, final int delta) {
        return new TypeSafeMatcher<Integer>() {
            private int actual;

            @Override
            protected boolean matchesSafely(final Integer actual) {
                this.actual = actual;
                return Math.abs(actual - expected) < delta;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText(String.format("actual %d was not within %d of %d",
                        actual, delta, expected));
            }
        };
    }

    private static void readMessage(final SingleChronicleQueue queue,
                                    final boolean manuallyReleaseResources,
                                    final Consumer<ExcerptTailer> refHolder) {
        final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try {
            final ExcerptTailer tailer = queue.createTailer();
            while (bytes.isEmpty()) {
                tailer.toStart().readBytes(bytes);
            }
            refHolder.accept(tailer);
            assertThat(Math.signum(bytes.readInt()) >= 0, is(true));

            if (manuallyReleaseResources) {
                try {
                    ((SingleChronicleQueueExcerpts.StoreTailer) tailer).releaseResources();
                } catch (RuntimeException e) {
                    // ignore
                }
            }
        } finally {
            bytes.release();
        }
    }

    private static void writeMessage(final int j, final SingleChronicleQueue queue) {
        final ExcerptAppender appender = queue.acquireAppender();
        appender.writeBytes(b -> {
            b.writeInt(j);
        });
    }

    @Before
    public void setUp() throws Exception {
        queuePath = DirectoryUtils.tempDir(AppenderFileHandleLeakTest.class.getSimpleName());
    }

    @Test
    public void appenderAndTailerResourcesShouldBeCleanedUpByGarbageCollection() throws Exception {
        // this might help the test be more stable when there is multiple tests.
        System.gc();
        Thread.sleep(100);
        assumeThat(OS.isLinux(), is(true));
        final List<ExcerptTailer> gcGuard = new LinkedList<>();
        long openFileHandleCount = countFileHandlesOfCurrentProcess();
        List<Path> fileHandlesAtStart = new ArrayList<>(lastFileHandles);
        try (SingleChronicleQueue queue = createQueue(SYSTEM_TIME_PROVIDER)) {
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
                assertThat(future.get(1, TimeUnit.MINUTES), is(true));
            }
            assertFalse(gcGuard.isEmpty());
            gcGuard.clear();

        }
        GcControls.waitForGcCycle();
        GcControls.waitForGcCycle();

        waitForFileHandleCountToDrop(openFileHandleCount, fileHandlesAtStart);
    }

    @Test
    public void tailerResourcesCanBeReleasedManually() throws Exception {
        System.gc();
        Thread.sleep(100);
        assumeThat(OS.isLinux(), is(true));
        try (SingleChronicleQueue queue = createQueue(SYSTEM_TIME_PROVIDER)) {
            final long openFileHandleCount = countFileHandlesOfCurrentProcess();
            final List<Path> fileHandlesAtStart = new ArrayList<>(lastFileHandles);
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
                assertThat(future.get(1, TimeUnit.MINUTES), is(true));
            }

            waitForFileHandleCountToDrop(openFileHandleCount, fileHandlesAtStart);

            assertFalse(gcGuard.isEmpty());
        }
    }

    @Test
    public void tailerShouldReleaseFileHandlesAsQueueRolls() throws Exception {
        System.gc();
        Thread.sleep(100);
        assumeThat(OS.isLinux(), is(true));
        final int messagesPerThread = 10;
        try (SingleChronicleQueue queue = createQueue(currentTime::get)) {

            final long openFileHandleCount = countFileHandlesOfCurrentProcess();
            final List<Path> fileHandlesAtStart = new ArrayList<>(lastFileHandles);
            final List<Future<Boolean>> futures = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < messagesPerThread; j++) {
                        writeMessage(j, queue);
                        currentTime.addAndGet(100);
                    }
                    return Boolean.TRUE;
                }));
            }

            for (Future<Boolean> future : futures) {
                assertThat(future.get(1, TimeUnit.MINUTES), is(true));
            }

            waitForFileHandleCountToDrop(openFileHandleCount, fileHandlesAtStart);

            fileHandlesAtStart.clear();
            final long tailerOpenFileHandleCount = countFileHandlesOfCurrentProcess();

            final ExcerptTailer tailer = queue.createTailer();
            tailer.toStart();
            final int expectedMessageCount = THREAD_COUNT * messagesPerThread;
            int messageCount = 0;
            storeFileListener.reset();
            while (true) {
                try (final DocumentContext ctx = tailer.readingDocument()) {
                    if (!ctx.isPresent()) {
                        break;
                    }

                    messageCount++;
                }
            }

            assertThat(messageCount, is(expectedMessageCount));
            assertThat(storeFileListener.toString(),
                    storeFileListener.releasedCount,
                    is(withinDelta(storeFileListener.acquiredCount, 3)));

            waitForFileHandleCountToDrop(tailerOpenFileHandleCount, fileHandlesAtStart);
        }
    }

    @After
    public void checkRegisteredBytes() throws Exception {
        threadPool.shutdownNow();
        assertTrue(threadPool.awaitTermination(5L, TimeUnit.SECONDS));
        BytesUtil.checkRegisteredBytes();
    }

    private void waitForFileHandleCountToDrop(
            final long startFileHandleCount,
            final List<Path> fileHandlesAtStart) throws IOException {
        final long failAt = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < failAt) {
            if (countFileHandlesOfCurrentProcess() < startFileHandleCount + 5) {
                return;
            }
        }

        final List<Path> fileHandlesAtEnd = new ArrayList<>(lastFileHandles);
        fileHandlesAtEnd.removeAll(fileHandlesAtStart);

        fail("File handle count did not drop for queue in directory " + queuePath.getAbsolutePath() +
                ", remaining handles:\n" + fileHandlesAtEnd);
    }

    private long countFileHandlesOfCurrentProcess() throws IOException {
        lastFileHandles.clear();
        try (final Stream<Path> fileHandles = Files.list(Paths.get("/proc/self/fd"))) {
            fileHandles.map(p -> {
                try {
                    return p.toRealPath();
                } catch (IOException e) {
                    return p;
                }
            }).filter(p -> p.toString().contains(queuePath.getName())).forEach(lastFileHandles::add);
        }
        try (final Stream<Path> fileHandles = Files.list(Paths.get("/proc/self/fd"))) {
            return fileHandles.count();
        }
    }

    private SingleChronicleQueue createQueue(final TimeProvider timeProvider) {
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
        private int acquiredCount = 0;
        private int releasedCount = 0;

        @Override
        public void onAcquired(final int cycle, final File file) {
            acquiredCounts.put(file.getName(), acquiredCounts.getOrDefault(file.getName(), 0) + 1);
            acquiredCount++;
        }

        @Override
        public void onReleased(final int cycle, final File file) {
            releasedCounts.put(file.getName(), releasedCounts.getOrDefault(file.getName(), 0) + 1);
            releasedCount++;
        }

        void reset() {
            acquiredCounts.clear();
            releasedCounts.clear();
            acquiredCount = 0;
            releasedCount = 0;
        }

        @Override
        public String toString() {
            return String.format("%nacquired: %d%nreleased: %d%ndiffs:%n%s%n",
                    acquiredCount, releasedCount, buildDiffs());
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