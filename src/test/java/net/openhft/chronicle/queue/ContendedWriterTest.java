/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

@Ignore("long running")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RequiredForClient
public class ContendedWriterTest extends ChronicleQueueTestBase {
    private static final long NUMBER_OF_LONGS = 3;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private ThreadDump threadDump;

@Test
    public void oneThread() {
        test("oneThread", new Config(false, 1, 0));
    }

    @Test
    public void oneThreadDeferred() {
        test("oneThreadDeferred", new Config(true, 1, 0));
    }

    @Test
    public void sixThreads() {
        Config config15 = new Config(false, 1, 5);
        test("sixThreads", config15, config15, config15, config15, config15, config15);
    }

    @Test
    public void sixThreadsDeferred() {
        Config config15 = new Config(true, 1, 5);
        test("sixThreadsDeferred", config15, config15, config15, config15, config15, config15);
    }

    @Test
    public void twoThreadsWritingLargeMessagesAtSameSlowRate() {
        test("twoThreadsWritingLargeMessagesAtSameSlowRate",
                new Config(false, 1, 5),
                new Config(false, 1, 5));
    }

    @Test
    public void twoThreadsWritingLargeMessagesAtSameSlowRateBothDeferred() {
        test("twoThreadsWritingLargeMessagesAtSameSlowRateBothDeferred",
                new Config(true, 1, 5),
                new Config(true, 1, 5));
    }

    @Test
    public void twoThreadsWritingLargeMessagesOneFastOneSlow() {
        test("twoThreadsWritingLargeMessagesOneFastOneSlow",
                new Config(false, 1, 0),
                new Config(false, 1, 5));
    }

    @Test
    public void twoThreadsWritingLargeMessagesOneFastOneSlowAndDeferred() {
        test("twoThreadsWritingLargeMessagesOneFastOneSlowAndDeferred",
                new Config(false, 1, 0),
                new Config(true, 1, 5));
    }

    @Test
    public void twoThreadsWritingLargeMessagesFastAndSmallMessagesSlow() {
        test("twoThreadsWritingLargeMessagesFastAndSmallMessagesSlow",
                new Config(false, 1, 0),
                new Config(false, 0, 5));
    }

    @Test
    public void twoThreadsWritingLargeMessagesFastAndSmallMessagesSlowAndDeferred() {
        test("twoThreadsWritingLargeMessagesFastAndSmallMessagesSlowAndDeferred",
                new Config(false, 1, 0),
                new Config(true, 0, 5));
    }

    private void test(String name, Config... configs) {
//        System.out.println(name);
        File path = getTmpDir();
        SingleChronicleQueue[] queues = new SingleChronicleQueue[configs.length];
        StartAndMonitor[] startAndMonitors = new StartAndMonitor[configs.length];

        try {
            for (int i = 0; i < configs.length; i++) {
                queues[i] = SingleChronicleQueueBuilder
                        .binary(path)
                        .testBlockSize()
//                        .progressOnContention(configs[i].progressOnContention)
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
        } finally {
            Closeable.closeQuietly(queues);
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
                assertEquals(i, valueIn.int64());
            //Jvm.pause(PAUSE_READ_MS);
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
        Histogram histo = new Histogram();

        public StartAndMonitor(ChronicleQueue queue, String name, int writePauseMs, int sleepBetweenMillis) {
            final SlowToSerialiseAndDeserialise object = new SlowToSerialiseAndDeserialise(writePauseMs);
            Thread thread = new Thread(() -> {
                try {
                    while (running.get()) {
                        long loopStart = System.nanoTime();
                        final ExcerptAppender appender = queue.acquireAppender();
//                        System.out.println("about to open");
                        try (final DocumentContext ctx = appender.writingDocument()) {
//                            System.out.println("about to write");
                            ctx.wire().getValueOut().marshallable(object);
//                            System.out.println("about to close");
                        }
//                        System.out.println("closed");
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