/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertFalse;
import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY;

/*
 * Created by skidder on 8/2/16.
 *
 * Targeting the problem of tailers in different threads where one writes very rarely, and the other
 * nearly constantly.
 *
 * The rare appender will have very bad latency proportional to the number of messages written since
 * it last appended.
 */
public class RareAppenderLatencyTest extends QueueTestCommon {
    private final static int HEAVY_MSGS = 1_000_000;
    private final static int RARE_MSGS = 50;

    private ExecutorService appenderES;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Before
    public void before() {
        appenderES = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("Appender", false));
    }

    @Override
    public void preAfter() {
        appenderES.shutdownNow();
    }

    @Test
    public void testRareAppenderLatency() throws InterruptedException, ExecutionException {
        System.setProperty("ignoreHeaderCountIfNumberOfExcerptsBehindExceeds", "" + (1 << 12));

        if (Jvm.isDebug())
            // this is a performance test so should not be run in debug mode
            return;

        if (Jvm.isAssertEnabled())
            // this is a performance test so should not be run with assertions turned on
            return;

        // System.out.println("starting test");
        String pathname = OS.getTarget() + "/testRareAppenderLatency-" + Time.uniqueId();
        new File(pathname).deleteOnExit();

        // Shared queue between two threads appending. One appends very rarely, another heavily.
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(pathname)
                .rollCycle(HOURLY)
                .build();
             final ExcerptAppender rareAppender = queue.createAppender()) {

            String text = getText();

            // Write a some messages with an appender from Main thread.
            for (int i = 0; i < RARE_MSGS; i++) {
                try (DocumentContext ctx = rareAppender.writingDocument()) {
                    ctx.wire()
                            .write("ts").int64(System.currentTimeMillis())
                            .write("msg").text(text);
                }
            }

            // Write a bunch of messages from another thread.
            Future<?> f = appenderES.submit(() -> {
                try (ExcerptAppender appender = queue.createAppender()) {
                    long start = System.currentTimeMillis();
                    for (int i = 0; i < HEAVY_MSGS; i++) {
                        try (DocumentContext ctx = appender.writingDocument()) {
                            ctx.wire()
                                    .write("ts").int64(System.currentTimeMillis())
                                    .write("msg").text(text);
                        }
                        if (appenderES.isShutdown())
                            return;
                    }
                }

                // System.out.println("Wrote heavy " + HEAVY_MSGS + " msgs in " + (System.currentTimeMillis() - start) + " ms");
            });

            f.get();

            // Write a message from the Main thread again (this will have unacceptable latency!)
            long now = System.currentTimeMillis();
            try (DocumentContext ctx = rareAppender.writingDocument()) {
                ctx.wire()
                        .write("ts").int64(System.currentTimeMillis())
                        .write("msg").text(text);
            }
            long l = System.currentTimeMillis() - now;

            // Write another message from the Main thread (this will be fast since we are caught up)
            now = System.currentTimeMillis();
            try (DocumentContext ctx = rareAppender.writingDocument()) {
                ctx.wire()
                        .write("ts").int64(System.currentTimeMillis())
                        .write("msg").text(text);
            }
            // System.out.println("Wrote first rare one in " + l + " ms");
            // System.out.println("Wrote another rare one in " + (System.currentTimeMillis() - now) + " ms");

            assertFalse("Appending from rare thread latency too high!", l > 150);
        }
    }

    @NotNull
    protected String getText() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) sb.append(UUID.randomUUID());
        return sb.toString();
    }
}
