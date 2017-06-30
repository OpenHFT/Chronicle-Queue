package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public final class AppenderFileHandleLeakTest {
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() / 2;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_COUNT);

    @Test
    public void shouldNotLeakFileHandles() throws Exception {
        assumeThat(OS.isLinux(), is(true));
        try (SingleChronicleQueue queue = createQueue()) {
            final long openFileHandleCount = countFileHandlesOfCurrentProcess();
            System.out.printf("start count: %d%n", openFileHandleCount);
            final List<Future<Boolean>> futures = new LinkedList<>();

            for (int i = 0; i < THREAD_COUNT; i++) {
                futures.add(threadPool.submit(() -> {
                    for (int j = 0; j < 50; j++) {
                        writeMessage(j, queue);
                        GcControls.requestGcCycle();
                    }
                    return Boolean.TRUE;
                }));
            }

            for (Future<Boolean> future : futures) {
                assertThat(future.get(1, TimeUnit.MINUTES), is(true));
            }

            GcControls.waitForGcCycle();

            System.out.printf("end count: %d%n", countFileHandlesOfCurrentProcess());
            assertThat(abs(openFileHandleCount - countFileHandlesOfCurrentProcess()) < 5, is(true));
        }
    }

    private static void writeMessage(final int j, final SingleChronicleQueue queue) {
        final ExcerptAppender appender = queue.acquireAppender();
        appender.writeBytes(b -> {
            b.writeInt(j);
        });
    }

    private static long countFileHandlesOfCurrentProcess() throws IOException {
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