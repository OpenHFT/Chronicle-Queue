/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.util.concurrent.*;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;

@RequiredForClient
public class VisibilityOfMessagesBetweenTailorsAndAppenderTest extends QueueTestCommon {

    volatile long lastWrittenIndex = Long.MIN_VALUE;

    /**
     * check if a message is written with an appender its visible to the tailor, without locks etc.
     *
     */
    @Test
    public void test() throws InterruptedException, ExecutionException {

        try (ChronicleQueue x = SingleChronicleQueueBuilder
                .binary(getTmpDir())
                .rollCycle(MINUTELY)
                .build()) {

            ExecutorService e1 = newSingleThreadExecutor(new NamedThreadFactory("e1"));

            e1.submit(() -> {
                try (ExcerptAppender excerptAppender = x.acquireAppender()) {
                    for (long i = 0; i < 1_000_000; i++) {
                        try (DocumentContext dc = excerptAppender.writingDocument()) {
                            dc.wire().getValueOut().int64(i);
                        }
                        lastWrittenIndex = excerptAppender.lastIndexAppended();
                        if (Thread.currentThread().isInterrupted())
                            return null;
                    }
                }
                return null;
            });

            ExecutorService e2 = newSingleThreadExecutor(new NamedThreadFactory("e2"));
            Future<Void> f2 = e2.submit(() -> {
                try (ExcerptTailer tailer = x.createTailer()) {

                    for (; ; ) {
                        long i = lastWrittenIndex;
                        if (i != Long.MIN_VALUE)
                            if (!tailer.moveToIndex(i))
                                throw new ExecutionException("non atomic, index=" + Long.toHexString(i), null);
                        if (Thread.currentThread().isInterrupted())
                            return null;
                    }
                }
            });

            try {
                f2.get(Jvm.isCodeCoverage() ? 20 : 5, TimeUnit.SECONDS);
            } catch (TimeoutException ignore) {

            }

            e1.shutdown();
            e2.shutdown();

            if (!e1.awaitTermination(1, TimeUnit.SECONDS)) {
                e1.shutdownNow();
            }
            if (!e2.awaitTermination(1, TimeUnit.SECONDS)) {
                e2.shutdownNow();
            }
        }
    }
}
