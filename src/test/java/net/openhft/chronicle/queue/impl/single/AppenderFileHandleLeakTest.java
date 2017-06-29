package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.DirectoryUtils;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public final class AppenderFileHandleLeakTest {
    private static final SingleChronicleQueue QUEUE;
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 3;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_COUNT);

    static {
        QUEUE = createQueue();
    }

    @Test
    public void shouldNotLeakFileHandles() throws Exception {
        assumeThat(OS.isLinux(), is(true));

        final long openFileHandleCount = countFileHandlesOfCurrentProcess();
        System.out.printf("start count: %d%n", openFileHandleCount);
        final List<Future<Boolean>> futures = new LinkedList<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(threadPool.submit(() -> {
                for (int j = 0; j < 1000; j++) {
                    writeMessage(j);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
                }
                return Boolean.TRUE;
            }));
        }

        for (Future<Boolean> future : futures) {
            assertThat(future.get(1, TimeUnit.DAYS), is(true));
        }

        waitForGcCycle();
        waitForGcCycle();

        System.out.printf("end count: %d%n", openFileHandleCount);
        assertThat(abs(openFileHandleCount - countFileHandlesOfCurrentProcess()) < 50, is(true));

    }

    private static void writeMessage(final int j) {
        QUEUE.acquireAppender().writeText(Integer.toString(j));
    }

    private void waitForGcCycle() {
        final long gcCount = getGcCount();
        System.gc();
        final long timeoutAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1L);
        while ((getGcCount() == gcCount) && System.currentTimeMillis() < timeoutAt) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
        }

        if (getGcCount() == gcCount) {
            throw new IllegalStateException("GC did not occur within timeout");
        }
    }

    private static long getGcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().
                reduce(0L,
                        (count, gcBean) -> count + gcBean.getCollectionCount(),
                        (a, b) -> a + b);
    }

    private static long countFileHandlesOfCurrentProcess() throws IOException {
        try (final Stream<Path> fileHandles = Files.list(Paths.get("/proc/self/fd"))) {
            return fileHandles.count();
        }
    }

    private static SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.
                binary(DirectoryUtils.tempDir(AppenderFileHandleLeakTest.class.getSimpleName())).build();
    }
}