/*
 * Copyright 2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.queue.rollcycles.SparseRollCycles;
import net.openhft.chronicle.wire.DocumentContext;

/**
 * Measures the cost of the tail sequence lookup, SCQIndexing.lastSequenceNumber, through two
 * public paths that call it on every invocation.
 * <p>
 * The end-to-end sample is one write by an appender that finds the write position moved by the
 * other appender. StoreAppender.writeHeader then calls lastSequenceNumber to resync its header
 * number. Two appenders alternate, so every write pays the lookup. The probe "excerptsInCycle"
 * samples ExcerptTailer.excerptsInCycle, which calls lastSequenceNumber directly.
 * <p>
 * The queue starts with one filler record that spans the alias period of the roll cycle,
 * 2^(64 - cycleShift) bytes, so that every later write position is above the period. Then it
 * holds "records" small records. Each iteration adds one record, so the number of records
 * since the last index entry grows during the run.
 * <p>
 * System properties: rollCycle (default HUGE_DAILY_XSPARSE), records (default 10000),
 * iterations (default 2000), throughput (default 200), runs (default 3), path.
 */
public class LastSequenceNumberJLBHBenchmark implements JLBHTask {
    private static final String PATH = System.getProperty("path", "last-sequence-number-bench");
    private static final String ROLL_CYCLE = System.getProperty("rollCycle", "HUGE_DAILY_XSPARSE");
    private static final int RECORDS = Integer.getInteger("records", 10_000);
    private static final int ITERATIONS = Integer.getInteger("iterations", 2_000);
    private static final int THROUGHPUT = Integer.getInteger("throughput", 200);
    private static final int RUNS = Integer.getInteger("runs", 3);
    private static final int WARM_UP = Integer.getInteger("warmUp", 500);

    private SingleChronicleQueue queue;
    private ExcerptAppender appenderA;
    private ExcerptAppender appenderB;
    private ExcerptTailer tailer;
    private NanoSampler excerptsInCycleProbe;
    private JLBH jlbh;
    private long count;

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
    }

    public static void main(String[] args) {
        JLBHOptions options = new JLBHOptions()
                .warmUpIterations(WARM_UP)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                .recordOSJitter(false)
                .accountForCoordinatedOmission(false)
                .skipFirstRun(RUNS >= 3)
                .runs(RUNS)
                .jlbhTask(new LastSequenceNumberJLBHBenchmark());
        new JLBH(options).start();
    }

    static RollCycle rollCycle(String name) {
        for (RollCycle rc : SparseRollCycles.values())
            if (rc.toString().equals(name))
                return rc;
        for (RollCycle rc : LargeRollCycles.values())
            if (rc.toString().equals(name))
                return rc;
        return RollCycles.valueOf(name);
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        System.out.println("-Dpath=" + PATH + " -DrollCycle=" + ROLL_CYCLE + " -Drecords=" + RECORDS);
        IOTools.deleteDirWithFiles(PATH, 10);

        RollCycle rollCycle = rollCycle(ROLL_CYCLE);
        queue = SingleChronicleQueueBuilder.binary(PATH).rollCycle(rollCycle).build();
        appenderA = queue.createAppender();
        appenderB = queue.createAppender();
        tailer = queue.createTailer();

        int cycleShift = Math.max(32, Maths.intLog2(queue.indexCount()) * 2 + Maths.intLog2(queue.indexSpacing()));
        long aliasPeriod = 1L << (64 - cycleShift);
        if (aliasPeriod <= (256L << 20)) {
            appenderA.writeBytes(bytes -> bytes.writeSkip(aliasPeriod));
            System.out.println("filler of " + aliasPeriod + " bytes written, alias period " + aliasPeriod);
        } else {
            System.out.println("alias period " + aliasPeriod + " not filled, all positions stay below it");
        }
        for (int i = 0; i < RECORDS; i++)
            write(appenderA, i);
        count = RECORDS;
        excerptsInCycleProbe = jlbh.addProbe("excerptsInCycle");
    }

    private void write(ExcerptAppender appender, long value) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().bytes().writeLong(value);
        }
    }

    @Override
    public void run(long startTimeNS) {
        // Alternate the appenders so that each write finds the position moved by the other.
        ExcerptAppender appender = (count & 1) == 0 ? appenderA : appenderB;
        long start = System.nanoTime();
        write(appender, count++);
        long afterWrite = System.nanoTime();
        jlbh.sample(afterWrite - start);

        tailer.excerptsInCycle(queue.cycle());
        excerptsInCycleProbe.sampleNanos(System.nanoTime() - afterWrite);
    }

    @Override
    public void complete() {
        tailer.close();
        appenderB.close();
        appenderA.close();
        queue.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, ITERATIONS, System.out);
    }
}
