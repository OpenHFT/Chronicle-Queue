/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("long running contention benchmark for manual performance checks")
@TestMethodOrder(MethodOrderer.MethodName.class)
@RequiredForClient
public class ContendedWriterTest extends QueueTestCommon {
    private static final long NUMBER_OF_LONGS = 3;
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    @DisplayName("Single writer thread runs without contention errors")
    public void oneThread() {
        long totalCount = test("oneThread", new Config(false, 1, 0));
        assertTrue(totalCount > 0, "totalCount should be > 0 for one-thread run, count=" + totalCount);
    }

    @Test
    @DisplayName("Single writer thread with deferred contention")
    public void oneThreadDeferred() {
        long totalCount = test("oneThreadDeferred", new Config(true, 1, 0));
        assertTrue(totalCount > 0, "totalCount should be > 0 for deferred one-thread run, count=" + totalCount);
    }

    @Test
    @DisplayName("Six writer threads run with expected contention")
    public void sixThreads() {
        Config config15 = new Config(false, 1, 5);
        long totalCount = test("sixThreads", config15, config15, config15, config15, config15, config15);
        assertTrue(totalCount > 0, "totalCount should be > 0 for six-thread run, count=" + totalCount);
    }

    @Test
    @DisplayName("Six writer threads with deferred contention")
    public void sixThreadsDeferred() {
        Config config15 = new Config(true, 1, 5);
        long totalCount = test("sixThreadsDeferred", config15, config15, config15, config15, config15, config15);
        assertTrue(totalCount > 0, "totalCount should be > 0 for deferred six-thread run, count=" + totalCount);
    }

    @Test
    @DisplayName("Two threads writing large messages at same slow rate")
    public void twoThreadsWritingLargeMessagesAtSameSlowRate() {
        long totalCount = test("twoThreadsWritingLargeMessagesAtSameSlowRate",
                new Config(false, 1, 5),
                new Config(false, 1, 5));
        assertTrue(totalCount > 0, "totalCount should be > 0 for slow large-message run, count=" + totalCount);
    }

    @Test
    @DisplayName("Two threads writing large messages at same slow rate with deferred contention")
    public void twoThreadsWritingLargeMessagesAtSameSlowRateBothDeferred() {
        long totalCount = test("twoThreadsWritingLargeMessagesAtSameSlowRateBothDeferred",
                new Config(true, 1, 5),
                new Config(true, 1, 5));
        assertTrue(totalCount > 0, "totalCount should be > 0 for deferred slow large-message run, count=" + totalCount);
    }

    @Test
    @DisplayName("Two threads writing large messages with fast and slow rates")
    public void twoThreadsWritingLargeMessagesOneFastOneSlow() {
        long totalCount = test("twoThreadsWritingLargeMessagesOneFastOneSlow",
                new Config(false, 1, 0),
                new Config(false, 1, 5));
        assertTrue(totalCount > 0, "totalCount should be > 0 for fast/slow large-message run, count=" + totalCount);
    }

    @Test
    @DisplayName("Two threads writing large messages with fast/slow rates and deferred contention")
    public void twoThreadsWritingLargeMessagesOneFastOneSlowAndDeferred() {
        long totalCount = test("twoThreadsWritingLargeMessagesOneFastOneSlowAndDeferred",
                new Config(false, 1, 0),
                new Config(true, 1, 5));
        assertTrue(totalCount > 0, "totalCount should be > 0 for deferred fast/slow large-message run, count=" + totalCount);
    }

    @Test
    @DisplayName("Two threads writing large messages fast and small messages slow")
    public void twoThreadsWritingLargeMessagesFastAndSmallMessagesSlow() {
        long totalCount = test("twoThreadsWritingLargeMessagesFastAndSmallMessagesSlow",
                new Config(false, 1, 0),
                new Config(false, 0, 5));
        assertTrue(totalCount > 0, "totalCount should be > 0 for large/fast small/slow run, count=" + totalCount);
    }

    @Test
    @DisplayName("Two threads writing large messages fast and small messages slow with deferred contention")
    public void twoThreadsWritingLargeMessagesFastAndSmallMessagesSlowAndDeferred() {
        long totalCount = test("twoThreadsWritingLargeMessagesFastAndSmallMessagesSlowAndDeferred",
                new Config(false, 1, 0),
                new Config(true, 0, 5));
        assertTrue(totalCount > 0, "totalCount should be > 0 for deferred large/fast small/slow run, count=" + totalCount);
    }

    private long test(String name, Config... configs) {
        File path = getTmpDir();
        SingleChronicleQueue[] queues = new SingleChronicleQueue[configs.length];
        StartAndMonitor[] startAndMonitors = new StartAndMonitor[configs.length];

        try {
            for (int i = 0; i < configs.length; i++) {
                queues[i] = SingleChronicleQueueBuilder
                        .binary(path)
                        .testBlockSize()
                        // .progressOnContention(configs[i].progressOnContention)
                        .build();
                startAndMonitors[i] = new StartAndMonitor(queues[i], Integer.toString(i), configs[i].writePause, configs[i].pauseBetweenWrites);
            }

            //warmup
            Jvm.pause(5_000);
            running.set(false);
            Jvm.pause(50);

            running.set(true);
            for (int i = 0; i < configs.length; i++) {
                startAndMonitors[i] = new StartAndMonitor(queues[i], Integer.toString(i), configs[i].writePause, configs[i].pauseBetweenWrites);
            }

            Jvm.pause(Jvm.isDebug() ? 30_000 : 15_000);
            running.set(false);
            Jvm.pause(50);

            for (int i = 0; i < configs.length; i++) {
                System.out.println("thread" + i + " progress=" + configs[i].progressOnContention + " writePause=" +
                        configs[i].writePause + " between=" + configs[i].pauseBetweenWrites + ": " +
                        startAndMonitors[i].histo.toMicrosFormat());
            }
            long totalCount = 0;
            for (int i = 0; i < configs.length; i++) {
                totalCount += startAndMonitors[i].histo.totalCount();
            }
            return totalCount;
        } finally {
            Closeable.closeQuietly((Object[]) queues);
        }
    }

    private static class Config {
        final boolean progressOnContention;
        final int writePause; // how long to keep writingContext open
        final int pauseBetweenWrites;

        private Config(boolean progressOnContention, int writePause, int pauseBetweenWrites) {
            this.progressOnContention = progressOnContention;
            this.writePause = writePause;
            this.pauseBetweenWrites = pauseBetweenWrites;
        }
    }

    private static class SlowToSerialiseAndDeserialise implements Marshallable {
        @SuppressWarnings("unused")
        private final StringBuilder sb = new StringBuilder();
        private final long writePauseMs;

        private SlowToSerialiseAndDeserialise(long writePauseMs) {
            this.writePauseMs = writePauseMs;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            ValueIn valueIn = wire.getValueIn();
            for (int i = 0; i < NUMBER_OF_LONGS; i++)
                assertEquals(i, valueIn.int64(), "marshallable value should equal index " + i);
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            ValueOut valueOut = wire.getValueOut();
            for (int i = 0; i < NUMBER_OF_LONGS; i++)
                valueOut.int64(i);
            Jvm.pause(writePauseMs);
        }
    }

    private class StartAndMonitor {
        final Histogram histo = new Histogram();

        StartAndMonitor(ChronicleQueue queue, String name, int writePauseMs, int sleepBetweenMillis) {
            final SlowToSerialiseAndDeserialise object = new SlowToSerialiseAndDeserialise(writePauseMs);
            Thread thread = new Thread(() -> {
                try (final ExcerptAppender appender = queue.createAppender()) {
                    while (running.get()) {
                        long loopStart = System.nanoTime();
                        try (final DocumentContext ctx = appender.writingDocument()) {
                            ctx.wire().getValueOut().marshallable(object);
                        }
                        long timeTaken = System.nanoTime() - loopStart;
                        histo.sampleNanos(timeTaken);
                        Jvm.pause(sleepBetweenMillis);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }, name);
            thread.start();
        }
    }
}
