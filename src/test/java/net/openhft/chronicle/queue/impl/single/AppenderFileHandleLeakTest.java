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
import org.junit.Ignore;
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
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

public final class AppenderFileHandleLeakTest {
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
    private final List<ExcerptTailer> gcGuard = new LinkedList<>();

    @Test
    public void appenderAndTailerResourcesShouldBeCleanedUpByGarbageCollection() throws Exception {
        // this might help the test be more stable when there is multiple tests.
        System.gc();
        Thread.sleep(100);
        assumeThat(OS.isLinux(), is(true));
        try (SingleChronicleQueue queue = createQueue()) {
            final long openFileHandleCount = countFileHandlesOfCurrentProcess();
            final List<Future<Boolean>> futures = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 50; j++) {
                        writeMessage(j, queue);
                        GcControls.requestGcCycle();
                        readMessage(queue, gcGuard, false);
                    }
                    return Boolean.TRUE;
                }));
            }

            for (Future<Boolean> future : futures) {
                assertThat(future.get(1, TimeUnit.MINUTES), is(true));
            }

            gcGuard.clear();

            GcControls.waitForGcCycle();
            GcControls.waitForGcCycle();

            waitForFileHandleCountToDrop(openFileHandleCount);
        }
    }

    @Ignore
    @Test
    public void tailerResourcesCanBeReleasedManually() throws Exception {
        System.gc();
        Thread.sleep(100);
        assumeThat(OS.isLinux(), is(true));
        try (SingleChronicleQueue queue = createQueue()) {
            final long openFileHandleCount = countFileHandlesOfCurrentProcess();
            final List<Future<Boolean>> futures = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 50; j++) {
                        writeMessage(j, queue);
                        GcControls.requestGcCycle();
                        readMessage(queue, gcGuard, true);
                    }
                    return Boolean.TRUE;
                }));
            }

            for (Future<Boolean> future : futures) {
                assertThat(future.get(1, TimeUnit.MINUTES), is(true));
            }

            waitForFileHandleCountToDrop(openFileHandleCount);
        }
    }

    @After
    public void checkRegisteredBytes() {
        BytesUtil.checkRegisteredBytes();
    }

    private void waitForFileHandleCountToDrop(final long startFileHandleCount) throws IOException {
        final long failAt = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < failAt) {
            if (countFileHandlesOfCurrentProcess() < startFileHandleCount + 5) {
                return;
            }
        }

        fail("File handle count did not drop");
    }

    private static void readMessage(final SingleChronicleQueue queue,
                                    final List<ExcerptTailer> gcGuard,
                                    final boolean manuallyReleaseResources) {
        final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try {
            final ExcerptTailer tailer = queue.createTailer();
            while (bytes.isEmpty()) {
                tailer.toStart().readBytes(bytes);
            }
            gcGuard.add(tailer);
            assertThat(Math.signum(bytes.readInt()) >= 0, is(true));

            if (manuallyReleaseResources) {
                try {
                    tailer.getCloserJob().run();
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

    private final List<Path> lastFileHandles = new ArrayList<>();

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