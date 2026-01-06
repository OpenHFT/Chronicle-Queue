/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * This test case replicates the assertion error in Chronicle StoreAppender's checkWritePositionHeaderNumber() method. see
 * https://github.com/OpenHFT/Chronicle-Queue/issues/611
 */
public class ChronicleAppenderCycleTest extends QueueTestCommon {

    private static final long LATCH_TIMEOUT_MS = 5000;

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    @DisplayName("Appenders advance cycles without errors under load")
    public void testAppenderCycle() throws IOException {
        String id = "testAppenderCycle";
        Bytes<?> msg = Bytes.allocateDirect(64);
        try {
            int n = 20;
            for (int i = 0; i < n; ++i) {
                String runId = id + '-' + i;
                Throwable[] errors = runTest(runId, msg);
                assertNull(errors[0], "Writer thread should complete without errors during cycle test run "
                        + runId + " (iteration " + i + ")");
                assertNull(errors[1], "Cycler thread should complete without errors during cycle test run "
                        + runId + " (iteration " + i + ")");
            }
        } finally {
            msg.releaseLast();
        }
    }

    private Throwable[] runTest(String id, Bytes<?> msg) {
        Path path = IOTools.createTempDirectory(id);
        try {
            CountDownLatch steady = new CountDownLatch(2);
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            int n = 468;

            final AtomicReference<Throwable> thr1 = useAppender(path, appender -> {
                appender.cycle();
                for (int i = 0; i < n; ++i)
                    appender.writeBytes(msg);
                steady.countDown();
                await(go, "go");
                for (int i = 0; i < n; ++i)
                    appender.writeBytes(msg);
            }, done);

            final AtomicReference<Throwable> thr2 = useAppender(path, appender -> {
                steady.countDown();
                await(go, "go");
                int m = 2 * n;
                for (int i = 0; i < m; ++i)
                    appender.cycle();
            }, done);

            await(steady, "steady");
            go.countDown();
            await(done, "done");

            return new Throwable[]{thr1.get(), thr2.get()};
        } finally {
            IOTools.deleteDirWithFiles(path.toFile());
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
                    try (ExcerptAppender appender = queue.createAppender()) {
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
        builder.testBlockSize();
        builder.rollCycle(RollCycles.DEFAULT);
        return builder;
    }
}
