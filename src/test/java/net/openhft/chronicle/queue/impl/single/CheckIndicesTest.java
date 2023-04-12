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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class CheckIndicesTest extends QueueTestCommon {

    private static final int BATCH_SIZE = 10;
    private ChronicleQueue queue0;

    @Ignore("stress test to run manually")
    @Test
    public void test() throws ExecutionException, InterruptedException {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).epoch(System.currentTimeMillis()).build()) {
            queue0 = queue;
            newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::appendToQueue, 0, 1, TimeUnit.MICROSECONDS);
            Future f = newSingleThreadScheduledExecutor().submit(this::checkIndices);
            Future f2 = newSingleThreadScheduledExecutor().submit(this::checkIndices);

            for (; ; ) {
                if (f.isDone())
                    f.get();
                if (f2.isDone())
                    f2.get();
                Thread.sleep(500);
            }
 }
    }

    private Callable<Void> checkIndices() {
        ExcerptTailer tailer = queue0.createTailer();

        long index = 0;

        boolean movetoIndex = true;
        for (int i = 0; i < 10_000_000; i++) {

            if (movetoIndex)
                if (!tailer.moveToIndex(index))
                    continue;
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    index += ((long) (Math.random() * 10) - 2);
                    movetoIndex = true;
                    continue;
                }
                if (index != dc.index())
                    throw new AssertionError();
                 // System.out.println("reading index=" + Long.toHexString(index));
                if (queue0.rollCycle().toSequenceNumber(index) != dc.wire().read("value").readLong())
                    throw new AssertionError();
            }
            movetoIndex = false;
            index += 1;

        }
        return null;

    }

    private void appendToQueue() {
        ExcerptAppender appender = queue0.acquireAppender();
        try {

            for (int i = 0; i < BATCH_SIZE; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    long seq = appender.queue().rollCycle().toSequenceNumber(dc.index());
                      // System.out.println("write=" + Long.toHexString(dc.index()));
                    dc.wire().write("value").writeLong(seq);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
