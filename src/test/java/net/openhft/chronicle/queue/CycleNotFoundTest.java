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
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertEquals;
import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;

public class CycleNotFoundTest extends ChronicleQueueTestBase {
    private static final int BLOCK_SIZE = 256 << 20;
    private static final int NUMBER_OF_TAILERS = 2;
    private static final long INTERVAL_US = 25;
    private static final long NUMBER_OF_MSG = 1_000_000;

    @Test()
    public void tailerCycleNotFoundTest() throws IOException, InterruptedException {
        String path = getTmpDir() + "/tailerCycleNotFound.q";
        new File(path).deleteOnExit();

        AtomicLong counter = new AtomicLong();

        Runnable reader = () -> {
            try (RollingChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build()) {

                final ExcerptTailer tailer = rqueue.createTailer();
                long last = -1;
                TailerState lastState = TailerState.UNINTIALISED;

                while (!Thread.interrupted()) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) {
                            lastState = tailer.state();
                        } else {
                            long n = dc.wire().read().int64();
                            if (n <= last) System.out.println("num did not increase! " + n + " last: " + last);
                            else if (n != last + 1)
                                System.out.println("num increased by more than 1! " + n + " last: " + last);

                            last = n;
                            counter.incrementAndGet();
                            lastState = tailer.state();
                        }
                    } catch (UnsupportedOperationException uoe) {
                        uoe.printStackTrace();
                        System.out.println("last state before exception: " + lastState);
                        TailerState state = tailer.state();
                        System.out.println("current state: " + state);
                        lastState = state;
                    }
                }
            } finally {
                System.out.printf("Read %,d messages", counter.intValue());
            }
        };

        List<Thread> tailers = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_TAILERS; i++) {
            Thread tailerThread = new Thread(reader, "tailer-thread-" + i);
            tailers.add(tailerThread);
        }

        Thread appenderThread = new Thread(() -> {
            ChronicleQueue wqueue = SingleChronicleQueueBuilder.binary(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build();

            final ExcerptAppender appender = wqueue.acquireAppender();

            long next = System.nanoTime() + INTERVAL_US * 1000;
            for (int i = 0; i < NUMBER_OF_MSG; i++) {
                while (System.nanoTime() < next)
                    /* busy wait*/ ;
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write().int64(i);
                }
                next += INTERVAL_US * 1000;
            }
            wqueue.close();
        }, "appender-thread");

        tailers.forEach(Thread::start);
        Jvm.pause(100);

        appenderThread.start();
        appenderThread.join();
        System.out.println("appender is done.");

        //Pause to allow tailer to catch up (if needed)
        for (int i = 0; i < 10; i++) {
            if (NUMBER_OF_MSG * NUMBER_OF_TAILERS > counter.get())
                Jvm.pause(Jvm.isDebug() ? 10000 : 1000);
        }

        for (int i = 0; i < 10; i++) {
            for (final Thread tailer : tailers) {
                tailer.interrupt();
                tailer.join(100);
            }
        }
        Thread.sleep(200);

        assertEquals(NUMBER_OF_MSG * NUMBER_OF_TAILERS, counter.get());
    }
}
