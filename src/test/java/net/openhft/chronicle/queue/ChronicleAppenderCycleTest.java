package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.assertNull;

/**
 * This test case replicates the assertion error in Chronicle StoreAppender's checkWritePositionHeaderNumber() method. see
 * https://github.com/OpenHFT/Chronicle-Queue/issues/611
 */
public class ChronicleAppenderCycleTest extends QueueTestCommon {

    private static final long LATCH_TIMEOUT_MS = 5000;

    @Test
    public void testAppenderCycle() throws IOException {
        String id = "testAppenderCycle";
        Bytes<?> msg = Bytes.allocateDirect(64);
        try {
            int n = 20;
            for (int i = 0; i < n; ++i)
                runTest(id + '-' + i, msg);
        } finally {
            msg.releaseLast();
        }

    }

    private void runTest(String id, Bytes<?> msg) throws IOException {
        Path path = IOTools.createTempDirectory(id);
        try {
            CountDownLatch steady = new CountDownLatch(2);
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            int n = 468;

            AtomicReference<Throwable> thr1 = useAppender(path, appender -> {
                appender.cycle();
                for (int i = 0; i < n; ++i)
                    appender.writeBytes(msg);
                steady.countDown();
                await(go, "go");
                for (int i = 0; i < n; ++i)
                    appender.writeBytes(msg);
            }, done);

            AtomicReference<Throwable> thr2 = useAppender(path, appender -> {
                steady.countDown();
                await(go, "go");
                int m = 2 * n;
                for (int i = 0; i < m; ++i)
                    appender.cycle();
            }, done);

            await(steady, "steady");
            go.countDown();
            await(done, "done");

            assertNull(thr1.get());
            assertNull(thr2.get());
        } finally {
            DirectoryUtils.deleteDir(path.toFile());
        }
    }

    private void await(CountDownLatch latch, String name) {
        try {
            latch.await(LATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Problem acquiring the \"" + name + "\" latch",
                    e);
        }
    }

    private AtomicReference<Throwable> useAppender(Path path,
                                                   Consumer<ExcerptAppender> tester, CountDownLatch done) {
        AtomicReference<Throwable> refThr = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                SingleChronicleQueueBuilder builder = createBuilder(path);
                try (SingleChronicleQueue queue = builder.build()) {
                    try (ExcerptAppender appender = queue.acquireAppender()) {
                        tester.accept(appender);
                    }
                }
            } catch (Throwable e) {
                refThr.set(e);
                e.printStackTrace();
            } finally {
                done.countDown();
            }
        });
        thread.setDaemon(true);
        thread.start();
        return refThr;
    }

    private SingleChronicleQueueBuilder createBuilder(Path path) {
        SingleChronicleQueueBuilder builder =
                SingleChronicleQueueBuilder.builder(path, WireType.FIELDLESS_BINARY);
        builder.rollCycle(RollCycles.DEFAULT);
        return builder;
    }
}