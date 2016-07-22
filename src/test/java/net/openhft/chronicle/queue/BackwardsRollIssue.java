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

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.RollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;

public class BackwardsRollIssue extends ChronicleQueueTestBase {
    private static final int BYTES_LENGTH = 256;
    private static final int BLOCK_SIZE = 256 << 20;
    private static final long INTERVAL_US = 10;

    @Test()
    public void doTest() throws IOException, InterruptedException {
        String path = getTmpDir() + "/backRoll.q";
        new File(path).deleteOnExit();

        AtomicLong counter = new AtomicLong();

        Runnable reader = () -> {
            final String name = Thread.currentThread().getName();
            final AffinityLock rlock = AffinityLock.acquireLock();
            final Bytes bytes = NativeBytes.nativeBytes(BYTES_LENGTH).unchecked(true);
            try (ChronicleQueue rqueue = new SingleChronicleQueueBuilder(path)
                    .wireType(WireType.FIELDLESS_BINARY)
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .blockSize(BLOCK_SIZE)
                    .build()) {

                final ExcerptTailer tailer = rqueue.createTailer();

                long current;
                long last = tailer.index();
                System.out.println("first index is: " + last);
                while (!Thread.interrupted()) {
                    bytes.clear();
                    if (tailer.readBytes(bytes)) {
                        current = tailer.index();
                        if (last == 0)
                            System.out.println(name + " first index is: " + current);

                        if (current < last)
                            System.out.println(name + " index went backwards from " + last + " to " + current);

                        last = current;
                        counter.incrementAndGet();
                    }
                }
            } finally {
                if (rlock != null) {
                    rlock.release();
                }
                System.out.printf("Read %,d messages", counter.intValue());
            }
        };
        Thread tailerThread = new Thread(reader, "tailer-thread1");
        Thread tailerThread2 = new Thread(reader, "tailer-thread2");

        long runs = 2_000_000;

        Thread appenderThread = new Thread(() -> {
            final AffinityLock wlock = AffinityLock.acquireLock();
            try {
                ChronicleQueue wqueue = new SingleChronicleQueueBuilder(path)
                        .wireType(WireType.FIELDLESS_BINARY)
                        .rollCycle(TEST_SECONDLY)
                        .blockSize(BLOCK_SIZE)
                        .build();

                final ExcerptAppender appender = wqueue.acquireAppender();
                final Bytes bytes = Bytes.allocateDirect(BYTES_LENGTH).unchecked(true);

                long next = System.nanoTime() + INTERVAL_US * 1000;
                for (int i = 0; i < runs; i++) {
                    while (System.nanoTime() < next)
                        /* busy wait*/ ;
                    long start = next;
                    bytes.readPositionRemaining(0, BYTES_LENGTH);
                    bytes.writeLong(0L, start);

                    appender.writeBytes(bytes);
                    next += INTERVAL_US * 1000;
                }
                wqueue.close();
            } finally {
                if (wlock != null) {
                    wlock.release();
                }
            }
        }, "appender-thread");

        tailerThread.start();
        tailerThread2.start();
        Jvm.pause(100);

        appenderThread.start();
        appenderThread.join();
        System.out.println("appender is done.");

        //Pause to allow tailer to catch up (if needed)
        for (int i = 0; i < 10; i++) {
            if (runs != counter.get())
                Jvm.pause(Jvm.isDebug() ? 10000 : 100);
        }

        for (int i = 0; i < 10; i++) {
            tailerThread.interrupt();
            tailerThread.join(100);
            tailerThread2.interrupt();
            tailerThread2.join(100);
        }

        assertEquals(runs * 2, counter.get());
    }
}
