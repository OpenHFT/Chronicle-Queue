/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ChronicleRollingIssueTest {

    @Ignore
    @Test
    public void test() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors() - 1;
        int messages = 50;

        String path = OS.TARGET + "/ChronicleRollingIssueTest-" + System.nanoTime();
        AtomicInteger count = new AtomicInteger();

        Runnable appendRunnable = () -> {
            try (final ChronicleQueue writeQueue = ChronicleQueueBuilder
                    .single(path)
                    .rollCycle(RollCycles.TEST_SECONDLY).build()) {
                for (int i = 0; i < messages; i++) {
                    long millis = System.currentTimeMillis() % 1000;
                    if (millis > 1 && millis < 999) {
                        Jvm.pause(999 - millis);
                    }
                    ExcerptAppender appender = writeQueue.acquireAppender();
                    Map<String, Object> map = new HashMap<>();
                    map.put("key", Thread.currentThread().getName() + " - " + i);
                    appender.writeMap(map);
                    count.incrementAndGet();
                }
            }
        };

        for (int i = 0; i < threads; i++) {
            new Thread(appendRunnable, "appender-" + i).start();
        }
        long start = System.currentTimeMillis();
        long lastIndex = 0;
        try (final ChronicleQueue queue = ChronicleQueueBuilder
                .single(path)
                .rollCycle(RollCycles.TEST_SECONDLY).build()) {
            ExcerptTailer tailer = queue.createTailer();
            int count2 = 0;
            while (count2 < threads * messages) {
                Map<String, Object> map = tailer.readMap();
                long index = tailer.index();
                if (map != null) {
                    count2++;
                } else if (index >= 0) {
                    if (RollCycles.TEST_SECONDLY.toCycle(lastIndex) != RollCycles.TEST_SECONDLY.toCycle(index)) {
                        System.out.println("Wrote: " + count
                                + " read: " + count2
                                + " index: " + Long.toHexString(index));
                        lastIndex = index;
                    }
                }
                final int i = count.get();
                if (System.currentTimeMillis() > start + 60000) {
//                    System.out.println(queue.dump());
                    throw new AssertionError("Wrote: " + count
                            + " read: " + count2
                            + " index: " + Long.toHexString(index));
                }
            }
        } finally {
            try {
                IOTools.deleteDirWithFiles(path, 2);
            } catch (IORuntimeException todoFixOnWindows) {

            }
        }
    }
}