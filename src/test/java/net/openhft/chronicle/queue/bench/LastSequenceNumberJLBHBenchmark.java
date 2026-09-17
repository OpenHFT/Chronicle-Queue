/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.io.Closeable;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Measures document opening/writing and excerptsInCycle with one or two appenders.
 * The ordinary writingDocument path calls resetPosition/lastSequenceNumber even
 * with one appender; alternating appenders is a separate control, not a prerequisite.
 * Samples are recorded after both independently timed regions.
 * <p>
 * The initial population is one optional alias-period filler plus "records" small records.
 * Warmup and every measured invocation append another record. Population and distance
 * from the last sparse-index anchor are printed at each run boundary.
 * <p>
 * Properties: rollCycle (HUGE_DAILY_XSPARSE), records (10000), appenders (1 or 2,
 * default 2), iterations (2000), throughput (200), runs (3), warmUp (500), revision,
 * and path (an existing parent directory for a new benchmark-owned child).
 */
public class LastSequenceNumberJLBHBenchmark implements JLBHTask {
    private static final String PATH = System.getProperty("path");
    private static final String ROLL_CYCLE = System.getProperty("rollCycle", "HUGE_DAILY_XSPARSE");
    private static final int APPENDERS = Integer.getInteger("appenders", 2);
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
    private long fillerRecords;
    private Path directory;
    private int completedRuns;

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
        if (APPENDERS != 1 && APPENDERS != 2)
            throw new IllegalArgumentException("appenders must be 1 or 2");
        try {
            directory = PATH == null ? Files.createTempDirectory("last-sequence-number-") :
                    Files.createTempDirectory(Paths.get(PATH), "last-sequence-number-");
            System.out.println("revision=" + System.getProperty("revision", "unspecified")
                    + " directory=" + directory + " filesystem=" + Files.getFileStore(directory).type());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        System.out.println("java=" + System.getProperty("java.runtime.version") + " vm="
                + System.getProperty("java.vm.name") + " os=" + System.getProperty("os.name")
                + " arch=" + System.getProperty("os.arch") + " processors=" + Runtime.getRuntime().availableProcessors());
        System.out.println("jvmOptions=" + ManagementFactory.getRuntimeMXBean().getInputArguments());
        System.out.println("rollCycle=" + ROLL_CYCLE + " initiallySmallRecords=" + RECORDS
                + " appenders=" + APPENDERS + " iterations=" + ITERATIONS + " throughput=" + THROUGHPUT
                + " runs=" + RUNS + " warmUp=" + WARM_UP);

        RollCycle rollCycle = rollCycle(ROLL_CYCLE);
        queue = SingleChronicleQueueBuilder.binary(directory).rollCycle(rollCycle).timeProvider(() -> 0L).build();
        appenderA = queue.createAppender();
        appenderB = APPENDERS == 2 ? queue.createAppender() : null;
        tailer = queue.createTailer();

        int cycleShift = Math.max(32, Maths.intLog2(queue.indexCount()) * 2 + Maths.intLog2(queue.indexSpacing()));
        long aliasPeriod = 1L << (64 - cycleShift);
        if (aliasPeriod <= (256L << 20)) {
            appenderA.writeBytes(bytes -> bytes.writeSkip(aliasPeriod));
            fillerRecords = 1;
            System.out.println("filler of " + aliasPeriod + " bytes written, alias period " + aliasPeriod);
        } else {
            System.out.println("alias period " + aliasPeriod + " not filled, all positions stay below it");
        }
        for (int i = 0; i < RECORDS; i++)
            write(appenderA, i);
        count = RECORDS;
        excerptsInCycleProbe = jlbh.addProbe("excerptsInCycle");
        printPopulation("before warmup");
    }

    private void write(ExcerptAppender appender, long value) {
        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().bytes().writeLong(value);
        }
    }

    @Override
    public void run(long startTimeNS) {
        ExcerptAppender appender = APPENDERS == 1 || (count & 1) == 0 ? appenderA : appenderB;
        long start = System.nanoTime();
        write(appender, count++);
        long afterWrite = System.nanoTime();
        long beforeCount = System.nanoTime();
        long population = tailer.excerptsInCycle(queue.cycle());
        long afterCount = System.nanoTime();

        if (population != count + fillerRecords)
            throw new AssertionError("Expected population " + (count + fillerRecords) + ", was " + population);
        jlbh.sample(afterWrite - start);
        excerptsInCycleProbe.sampleNanos(afterCount - beforeCount);
    }

    @Override
    public void warmedUp() {
        printPopulation("after warmup / before run 1");
    }

    @Override
    public void runComplete() {
        printPopulation("after run " + (++completedRuns));
    }

    private void printPopulation(String phase) {
        long population = count + fillerRecords;
        long lastSequence = population - 1;
        long distance = lastSequence < 0 ? 0 : lastSequence % queue.indexSpacing();
        System.out.println(phase + ": population=" + population + " lastSequence=" + lastSequence
                + " indexSpacing=" + queue.indexSpacing() + " lastSparseAnchor=" + (lastSequence - distance)
                + " recordsAfterAnchor=" + distance);
    }

    @Override
    public void complete() {
        try {
            TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, ITERATIONS, System.out);
        } finally {
            Closeable.closeQuietly(tailer, appenderB, appenderA, queue);
            // Only the child created by this benchmark is owned; never delete the supplied parent.
            if (directory != null)
                IOTools.deleteDirWithFiles(directory.toFile());
        }
    }
}
