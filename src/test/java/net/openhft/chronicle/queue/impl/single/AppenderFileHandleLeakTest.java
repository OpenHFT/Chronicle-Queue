package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

public final class AppenderFileHandleLeakTest {
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
    private final List<Path> lastFileHandles = new ArrayList<>();

    @Test
    public void appenderAndTailerResourcesShouldBeCleanedUpByGarbageCollection() throws Exception {
        // this might help the test be more stable when there is multiple tests.
        System.gc();
        Thread.sleep(100);
        assumeThat(OS.isLinux(), is(true));
        final List<ExcerptTailer> gcGuard = new LinkedList<>();
        try (SingleChronicleQueue queue = createQueue()) {
            final long openFileHandleCount = countFileHandlesOfCurrentProcess();
            final List<Path> fileHandlesAtStart = new ArrayList<>(lastFileHandles);
            final List<Future<Boolean>> futures = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 50; j++) {
                        writeMessage(j, queue);
                        GcControls.requestGcCycle();
                        readMessage(queue, false, gcGuard::add);
                    }
                    return Boolean.TRUE;
                }));
            }

            for (Future<Boolean> future : futures) {
                assertThat(future.get(1, TimeUnit.MINUTES), is(true));
            }
            assertFalse(gcGuard.isEmpty());
            gcGuard.clear();

            GcControls.waitForGcCycle();
            GcControls.waitForGcCycle();

            waitForFileHandleCountToDrop(openFileHandleCount, fileHandlesAtStart);
        }
    }

    @Test
    public void tailerResourcesCanBeReleasedManually() throws Exception {
        System.gc();
        Thread.sleep(100);
        assumeThat(OS.isLinux(), is(true));
        try (SingleChronicleQueue queue = createQueue()) {
            final long openFileHandleCount = countFileHandlesOfCurrentProcess();
            final List<Path> fileHandlesAtStart = new ArrayList<>(lastFileHandles);
            final List<Future<Boolean>> futures = new LinkedList<>();
            final List<ExcerptTailer> gcGuard = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 50; j++) {
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

    @After
    public void checkRegisteredBytes() throws Exception {
        threadPool.shutdownNow();
        assertTrue(threadPool.awaitTermination(5L, TimeUnit.SECONDS));
        BytesUtil.checkRegisteredBytes();
    }

    private void waitForFileHandleCountToDrop(
            final long startFileHandleCount,
            final List<Path> fileHandlesAtStart) throws IOException {
        final long failAt = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < failAt) {
            if (countFileHandlesOfCurrentProcess() < startFileHandleCount + 5) {
                return;
            }
        }

        final List<Path> fileHandlesAtEnd = new ArrayList<>(lastFileHandles);
        fileHandlesAtEnd.removeAll(fileHandlesAtStart);

        fail("File handle count did not drop, remaining handles:\n" + fileHandlesAtEnd);
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

    private long countFileHandlesOfCurrentProcess() throws IOException {
        lastFileHandles.clear();
        try (final Stream<Path> fileHandles = Files.list(Paths.get("/proc/self/fd"))) {
            fileHandles.map(p -> {
                try {
                    return p.toRealPath();
                } catch (IOException e) {
                    return p;
                }
            }).forEach(lastFileHandles::add);
        }
        try (final Stream<Path> fileHandles = Files.list(Paths.get("/proc/self/fd"))) {
            return fileHandles.count();
        }
    }

    private static SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.
                binary(DirectoryUtils.tempDir(AppenderFileHandleLeakTest.class.getSimpleName())).
                rollCycle(RollCycles.TEST_SECONDLY).
                wireType(WireType.BINARY_LIGHT).
                build();
    }
}