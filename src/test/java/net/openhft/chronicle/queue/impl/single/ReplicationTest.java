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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.ChronicleQueueTestBase.getTmpDir;

public class ReplicationTest {

    private static final int TIMES = 100;

    @Test
    // todo rob to change this to check the data
    // @Ignore("TODO FIX Indexes are not in the same place")
    public void testAppendAndRead() throws TimeoutException, ExecutionException, InterruptedException {

        class Data {
            Bytes bs = Bytes.elasticByteBuffer();
            long index;

            public Data(Bytes bytesStore, long index) {
                bs.write(bytesStore);
                this.index = index;
            }

            public Bytes<?> bytes() {
                return bs;
            }
        }

        ConcurrentLinkedQueue<Data> q = new ConcurrentLinkedQueue<>();

        try (final RollingChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir())
                .wireType(WireType.BINARY)
                .build()) {

            final ExcerptAppender appender = queue.createAppender();
            AtomicLong value = new AtomicLong();
            Executors.newSingleThreadExecutor(new NamedThreadFactory("data", true)).execute(() -> {
                for (long i = 0; i < TIMES; ) {
                    appender.writeDocument(w -> w.writeEventName("some-key").int64(value
                            .incrementAndGet()));
                    if (i % 1_000_000 == 0)
                        System.out.println("data=" + i);
                    i++;
                }

            });

            final ExcerptTailer tailer = queue.createTailer();

            Executors.newSingleThreadExecutor(new NamedThreadFactory("source", true)).execute(() -> {
                for (long i = 0; i < TIMES; ) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent())
                            continue;
                        q.add(new Data(dc.wire().bytes(), dc.index()));
                        if (i % 1_000_000 == 0)
                            System.out.println("source=" + i);
                        i++;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            try (final RollingChronicleQueue queue2 = SingleChronicleQueueBuilder.binary(getTmpDir())
                    .wireType(WireType.BINARY)
                    .build()) {

                final ExcerptAppender syncAppender = queue2.createAppender();

                ExecutorService sync = Executors.newSingleThreadExecutor(new NamedThreadFactory("sync", true));
                Future f = sync.submit(()
                        -> {
                    for (long i = 0; i < TIMES; ) {
                        Data poll = q.poll();
                        if (poll == null) {
                            Jvm.pause(1);
                            continue;
                        }
                        syncAppender.writeBytes(poll.index, poll.bytes().bytesForRead());
                        if (i % 1_000_000 == 0)
                            System.out.println("sync=" + i);
                        i++;
                    }
                    return null;

                });
                f.get();

                Assert.assertEquals(queue.dump(), queue2.dump());
//                System.out.println(queue2.dump());
            }
        }
    }
}

