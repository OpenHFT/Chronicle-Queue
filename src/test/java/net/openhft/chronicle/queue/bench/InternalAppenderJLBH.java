/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class InternalAppenderJLBH implements JLBHTask {

    private static final String QUEUE_PATH = "internalAppend";

    static {
        System.setProperty("jvm.resource.tracing", "false");
    }

    private static final int ITERATIONS = 200_000;
    private static final int WARMUP_ITERATIONS = 50_000;
    private final RollCycle rollCycle;
    private SingleChronicleQueue queue;
    private SetTimeProvider timeProvider;
    private InternalAppender appender;
    private int sequenceNumber;
    private Bytes<?> payload;
    private JLBH jlbh;

    public InternalAppenderJLBH(RollCycle rollCycle) {
        this.rollCycle = rollCycle;
    }

    public static void main(String... args) {
        new JLBH(new JLBHOptions()
                .jlbhTask(new InternalAppenderJLBH(RollCycles.DEFAULT))
                .iterations(ITERATIONS)
                .warmUpIterations(WARMUP_ITERATIONS)
                .accountForCoordinatedOmission(false)
                .runs(3)
        ).start();
    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFiles(QUEUE_PATH);
        timeProvider = new SetTimeProvider();
        payload = Bytes.from("hello world");
        queue = SingleChronicleQueueBuilder
                .binary(QUEUE_PATH)
                .rollCycle(rollCycle)
                .timeProvider(timeProvider)
                .build();
        appender = (InternalAppender) queue.createAppender();
        this.jlbh = jlbh;
    }

    @Override
    public void run(long startTimeNS) {
        long index = rollCycle.toIndex(0, sequenceNumber);
        appender.writeBytes(index, payload);
        jlbh.sample(System.nanoTime() - startTimeNS);
        sequenceNumber++;
    }

    @Override
    public void complete() {
        // The `-SAFE` suffix is a hangover from when there was a safe and unsafe version of this benchmark. Keep it for continuity in the charts.
        TeamCityHelper.teamCityStatsLastRun(InternalAppenderJLBH.class.getSimpleName() + "-SAFE", jlbh, ITERATIONS, System.out);
        Closeable.closeQuietly(appender, queue);
        BackgroundResourceReleaser.releasePendingResources();
        IOTools.deleteDirWithFiles(QUEUE_PATH);
    }
}
