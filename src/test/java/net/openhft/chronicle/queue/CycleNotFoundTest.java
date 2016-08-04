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

import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;

public class CycleNotFoundTest extends ChronicleQueueTestBase {
    private static final int BLOCK_SIZE = 256 << 20;
    private static final int NUMBER_OF_TAILERS = 4;
    private static final long INTERVAL_US = 25;
    private static final long NUMBER_OF_MSG = 100_000;

    //  @Ignore("long running test")
    @Test(timeout = 5000)
    public void tailerCycleNotFoundTest() throws IOException, InterruptedException, ExecutionException {
        String path = getTmpDir() + "/tailerCycleNotFound.q";
        new File(path).deleteOnExit();
        ExecutorService executorService = Executors.newFixedThreadPool((int) NUMBER_OF_MSG);
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
                long count = 0;
                while (count < NUMBER_OF_MSG) {


                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) {
                            lastState = tailer.state();
                        } else {
                            long n = dc.wire().read().int64();
                            if (n <= last)
                                System.out.println("num did not increase! " + n + " last: " + last);
                            else if (n != last + 1)
                                System.out.println("num increased by more than 1! " + n + " last: " + last);

                            last = n;
                            counter.incrementAndGet();
                            count++;
                            lastState = tailer.state();
                        }
                    } catch (UnsupportedOperationException uoe) {
                        uoe.printStackTrace();
                        System.out.println("last state before exception: " + lastState);
                        TailerState state = tailer.state();
                        System.out.println("current state: " + state);
                        lastState = state;
                    }
                    if (executorService.isShutdown())
                        return;
                }


            } finally {
                System.out.printf("Read %,d messages", counter.intValue());
            }
        };


        List<Future> tailers = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_TAILERS; i++) {
            tailers.add(executorService.submit(reader));
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


        appenderThread.start();
        appenderThread.join();
        System.out.println("appender is done.");

        // wait for all the tailer to finish
        for (Future f : tailers) {
            f.get();
        }

        assertEquals(NUMBER_OF_MSG * NUMBER_OF_TAILERS, counter.get());
    }
}
