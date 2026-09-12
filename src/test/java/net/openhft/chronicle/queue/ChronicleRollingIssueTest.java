/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;

@RequiredForClient
public class ChronicleRollingIssueTest extends QueueTestCommon {

    private String path;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
        path = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
    }

    @After
    public void tearDown() {
        IOTools.deleteDirWithFiles(path);
    }

    @Test
    public void test() throws InterruptedException {
        int threads = Math.min(64, Runtime.getRuntime().availableProcessors() * 4) - 1;
        int messages = 100;

        AtomicInteger count = new AtomicInteger();
        StoreFileListener storeFileListener = (cycle, file) -> {
        };
        Runnable appendRunnable = () -> {
            try (final ChronicleQueue writeQueue = ChronicleQueue
                    .singleBuilder(path)
                    .testBlockSize()
                    .storeFileListener(storeFileListener)
                    .rollCycle(TEST_SECONDLY).build();
                 ExcerptAppender appender = writeQueue.createAppender()) {
                for (int i = 0; i < messages; i++) {
                    long millis = System.currentTimeMillis() % 100;
                    if (millis > 1 && millis < 99) {
                        Jvm.pause(99 - millis);
                    }
                    Map<String, Object> map = new HashMap<>();
                    map.put("key", Thread.currentThread().getName() + " - " + i);
                    appender.writeMap(map);
                    count.incrementAndGet();
                }
            }
        };

        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread(appendRunnable, "appender-" + i);
            thread.start();
            threadList.add(thread);
        }
        long start = System.currentTimeMillis();
        long lastIndex = 0;
        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(path)
                .testBlockSize()
                .storeFileListener(storeFileListener)
                .rollCycle(TEST_SECONDLY).build()) {
            ExcerptTailer tailer = queue.createTailer();
            int count2 = 0;
            while (count2 < threads * messages) {
                Map<String, Object> map = tailer.readMap();
                long index = tailer.index();
                if (map != null) {
                    count2++;
                } else if (index >= 0) {
                    if (TEST_SECONDLY.toCycle(lastIndex) != TEST_SECONDLY.toCycle(index)) {
/*
                       // System.out.println("Wrote: " + count
                                + " read: " + count2
                                + " index: " + Long.toHexString(index));
*/
                        lastIndex = index;
                    }
                }
                if (System.currentTimeMillis() > start + 60000) {
                    throw new AssertionError("Wrote: " + count
                            + " read: " + count2
                            + " index: " + Long.toHexString(index));
                }
            }
        } finally {
            for (Thread thread : threadList) {
                thread.interrupt();
                thread.join(1000);
            }
            try {
                IOTools.deleteDirWithFiles(path, 2);
            } catch (IORuntimeException e) {
                Jvm.debug().on(ChronicleRollingIssueTest.class, "Failed to clean up test directory", e);
            }
        }
    }
}
