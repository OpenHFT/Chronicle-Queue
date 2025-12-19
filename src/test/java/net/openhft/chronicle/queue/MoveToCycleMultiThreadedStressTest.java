/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.Threads;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

public class MoveToCycleMultiThreadedStressTest extends QueueTestCommon {

    private ThreadLocal<ExcerptTailer> tailer;
    private final AtomicLong last = new AtomicLong();
    private long firstCycle;

    private static final int READ_THREADS = Math.min(Runtime.getRuntime().availableProcessors(), 10);
    private ChronicleQueue queue;

    private final AtomicBoolean shutDown = new AtomicBoolean();
    private boolean resourceTracing;

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @BeforeEach
    public void disableResourceTracing() {
        // with this enabled, and a 32 GB heap this fails with flight recorder
        // with this disabled, and a 32 *MB* heap this passes with flight recorder on
        resourceTracing = Jvm.isResourceTracing();
        Jvm.setResourceTracing(false);
    }

    @AfterEach
    public void resetResourceTracing() {
        Jvm.setResourceTracing(resourceTracing);
    }

    @Test

    @Timeout(value = 60000, unit = TimeUnit.MILLISECONDS)
    public void test() throws ExecutionException, InterruptedException {
        final String path = OS.getTarget() + "/stressMoveToCycle-" + Time.uniqueId();
        final ExecutorService es = Executors.newCachedThreadPool();

        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .rollCycle(net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY)
                .build();
             ExcerptAppender excerptAppender = q.createAppender()) {
            this.queue = q;
            tailer = ThreadLocal.withInitial(q::createTailer);
            excerptAppender.writeText("first");
            updateLast(excerptAppender);

            firstCycle = excerptAppender.queue().rollCycle().toCycle(q.firstIndex());

            final Future<Void> appender = es.submit(this::append);
            final List<Future<Void>> f = new ArrayList<>();

            for (int i = 0; i < READ_THREADS; i++) {
                f.add(es.submit(this::randomMove));
            }

            appender.get();
            shutDown.set(true);
            Thread.sleep(100);

            for (Future<Void> future : f) {
                try {
                    future.get(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                    Assertions.fail("moveToCycle: reader task failed");
                }
            }
            Assertions.assertTrue(last.get() >= firstCycle, "moveToCycle: last cycle updated");
        }

        Threads.shutdown(es);
        IOTools.deleteDirWithFiles(path);
    }

    private Void append() {

        try (final ExcerptAppender excerptAppender = queue.createAppender()) {

            for (int i = 0; i < 50; i++) {
                excerptAppender.writeText("hello");
                updateLast(excerptAppender);
                Jvm.pause(100);
            }
            return null;
        }
    }

    private void updateLast(ExcerptAppender excerptAppender) {
        long lastIndex = excerptAppender.lastIndexAppended();
        long lastCycle = excerptAppender.queue().rollCycle().toCycle(lastIndex);
        long expect;
        do {
            expect = this.last.get();
        } while (!this.last.compareAndSet(expect, lastCycle));
    }

    private Void randomMove() {
        final ExcerptTailer tailer = this.tailer.get();
        while (!shutDown.get()) {

            long span = last.get() - firstCycle;
            int cycle = (int) ((Math.random() * span) + firstCycle);
            tailer.moveToCycle(cycle);
        }
        tailer.close();
        return null;
    }
}
