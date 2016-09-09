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
package net.openhft.chronicle.queue.impl.single.jira;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.WireType;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-30
 */
public class Queue30Test extends ChronicleQueueTestBase {

    @Ignore("Stress test - doesn't finish")
    @Test
    public void testMT() throws IOException, InterruptedException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.TEXT)
                .blockSize(640_000)
                .build();

        ExecutorService exec = Executors.newCachedThreadPool(new NamedThreadFactory("stress"));
        Throwable[] tref = {null};
        Runnable r = () -> {
            try {
                final String name = Thread.currentThread().getName();
                final ExcerptAppender appender = queue.acquireAppender();
                for (int count = 0; !Thread.currentThread().isInterrupted(); count++) {
                    final int c = count;
                    appender.writeDocument(w ->
                            w.write(() -> "thread").text(name)
                                    .write(() -> "count").int32(c)
                    );

                    if (count % 10_000 == 0) {
                        LOGGER.info(name + "> " + count);
                    }
                }
            } catch (Throwable t) {
                tref[0] = t;
                exec.shutdown();
            }
        };
        for (int i = 0; i < 100; i++)
            exec.submit(r);
        exec.awaitTermination(10, TimeUnit.MINUTES);
        exec.shutdownNow();

        if (tref[0] != null)
            throw new AssertionError(tref[0]);

    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Ignore("Stress test - doesn't finish")
    @Test
    public void testST() throws IOException {
        final ChronicleQueue queue = new SingleChronicleQueueBuilder(getTmpDir())
                .wireType(WireType.TEXT)
                .blockSize(640_000)
                .build();

        final String name = Thread.currentThread().getName();
        final ExcerptAppender appender = queue.acquireAppender();
        for (int count = 0; ; count++) {
            final int c = count;
            appender.writeDocument(w ->
                    w.write(() -> "thread").text(name)
                            .write(() -> "count").int32(c)
            );

            if (count % 50_000 == 0) {
                LOGGER.info(name + "> " + count);
            }
        }
    }
}
