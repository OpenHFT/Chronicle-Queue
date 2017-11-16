/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.StoreComponentReferenceHandler;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

/*
 * Created by Jerry Shea 16/11/17
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ContendedWriterTest {
    final String STRATEGY = Boolean.getBoolean("header.write.defer") ? "DEFFERED " : "ORIGINAL ";
    final AtomicBoolean running = new AtomicBoolean(true);
    private static final long NUMBER_OF_LONGS = 3;

    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
        threadDump.ignore(StoreComponentReferenceHandler.THREAD_NAME);
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Test
    public void oneThread() {
        System.out.println("oneThread");
        File path1 = DirectoryUtils.tempDir("testContended");

        try (SingleChronicleQueue queue1 = SingleChronicleQueueBuilder
                .binary(path1)
                .testBlockSize()
                .build()) {

            new StartAndMonitor(queue1, "1", 1, 0);

            //warmup
            Jvm.pause(5_000);
            running.set(false);
            Jvm.pause(50);

            running.set(true);
            StartAndMonitor s1 = new StartAndMonitor(queue1, "1", 1, 0);

            Jvm.pause(Jvm.isDebug() ? 30_000 : 5_000);
            running.set(false);
            Jvm.pause(50);

            System.out.println(STRATEGY + s1.histo.toMicrosFormat());
        }
    }

    @Test
    public void twoThreadsWritingLargeMessagesAtSameSlowRate() {
        testContended("twoThreadsWritingLargeMessagesAtSameSlowRate", 1, 5, 1, 5);
    }

    @Test
    public void twoThreadsWritingLargeMessagesOneFastOneSlow() {
        testContended("twoThreadsWritingLargeMessagesOneFastOneSlow", 1, 0, 1, 5);
    }

    @Test
    public void twoThreadsWritingLargeMessagesFastAndSmallMessagesSlow() {
        testContended("twoThreadsWritingLargeMessagesFastAndSmallMessagesSlow", 1, 0, 0, 5);
    }

    private void testContended(String name, int wp1, int sbm1, int wp2, int sbm2) {
        System.out.println(name);
        File path1 = DirectoryUtils.tempDir(name);

        try (SingleChronicleQueue queue1 = SingleChronicleQueueBuilder
                .binary(path1)
                .testBlockSize()
                .build();
             SingleChronicleQueue queue2 = SingleChronicleQueueBuilder
                     .binary(path1)
                     .testBlockSize()
                     .build()) {

            new StartAndMonitor(queue1, "1", wp1, sbm1);
            new StartAndMonitor(queue2, "2", wp2, sbm2);

            //warmup
            Jvm.pause(5_000);
            running.set(false);
            Jvm.pause(50);

            running.set(true);
            StartAndMonitor s1 = new StartAndMonitor(queue1, "1", wp1, sbm1);
            StartAndMonitor s2 = new StartAndMonitor(queue2, "2", wp2, sbm2);

            Jvm.pause(Jvm.isDebug() ? 30_000 : 5_000);
            running.set(false);
            Jvm.pause(50);

            System.out.println(STRATEGY + s1.histo.toMicrosFormat());
            System.out.println(STRATEGY + s2.histo.toMicrosFormat());
        }
    }

    private class StartAndMonitor {
        Histogram histo = new Histogram();

        public StartAndMonitor(ChronicleQueue queue, String name, int writePauseMs, int sleepBetweenMillis) {
            final ExcerptAppender appender = queue.acquireAppender();
            final SlowToSerialiseAndDeserialise object = new SlowToSerialiseAndDeserialise(writePauseMs);
            Thread thread = new Thread(() -> {
                try {
                    while (running.get()) {
                        long loopStart = System.nanoTime();
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

    private static class SlowToSerialiseAndDeserialise implements Marshallable {
        private final StringBuilder sb = new StringBuilder();
        private final long writePauseMs;

        private SlowToSerialiseAndDeserialise(long writePauseMs) {
            this.writePauseMs = writePauseMs;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            ValueIn valueIn = wire.getValueIn();
            for (int i=0; i<NUMBER_OF_LONGS; i++)
                assertEquals(i, valueIn.int64());
            //Jvm.pause(PAUSE_READ_MS);
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            ValueOut valueOut = wire.getValueOut();
            for (int i=0; i<NUMBER_OF_LONGS; i++)
                valueOut.int64(i);
            Jvm.pause(writePauseMs);
        }
    }
}
