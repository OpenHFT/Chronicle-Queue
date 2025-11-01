/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueLargeMessageJLBHBenchmark implements JLBHTask {
    private static final int MSG_THROUGHPUT = Integer.getInteger("throughput", 50_000_000);
    private static final int ITERATIONS = Integer.getInteger("iterations", 1_000);
    private static final int MSG_LENGTH = Integer.getInteger("length", 1_000_000);
    private static final String PATH = System.getProperty("path", "replica");
    private static final boolean MSG_DIRECT = Jvm.getBoolean("direct");
    static byte[] bytesArr = new byte[MSG_LENGTH];
    static Bytes<?> bytesArr2 = Bytes.allocateDirect(MSG_LENGTH);
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private JLBH jlbh;
    private NanoSampler writeTime;

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
    }

    public static void main(String[] args) {
        int throughput = MSG_THROUGHPUT / MSG_LENGTH;

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(12_000)
                .iterations(ITERATIONS)
                .throughput(throughput)
                .recordOSJitter(false)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new QueueLargeMessageJLBHBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFilesOrThrow(PATH);

        sourceQueue = single(PATH).blockSize(1L << 30).build();
        sinkQueue = single(PATH).blockSize(1L << 30).build();
        appender = sourceQueue.createAppender();
        tailer = sinkQueue.createTailer();
        tailer.singleThreadedCheckDisabled(true);
        this.jlbh = jlbh;
        writeTime = jlbh.addProbe("writeTime");
    }

    @Override
    public void run(long startTimeNS) {

        if (MSG_DIRECT)
            bytesArr2.readLimit(MSG_LENGTH);
        try (DocumentContext dc = appender.writingDocument()) {
            Bytes<?> bytes = dc.wire().bytes();
            bytes.writeLong(startTimeNS);
            bytes.writeInt(bytes.length());
            if (MSG_DIRECT)
                bytes.write(bytesArr2);
            else
                bytes.write(bytesArr);
        }
        writeTime.sampleNanos(System.nanoTime() - startTimeNS);

        try (DocumentContext dc = tailer.readingDocument()) {
            if (dc.wire() != null) {
                Bytes<?> bytes = dc.wire().bytes();
                long start = bytes.readLong();
                int length = bytes.readInt();
                assert length == MSG_LENGTH;
                if (MSG_DIRECT)
                    bytes.read(bytesArr2.clear(), length);
                else
                    bytes.read(bytesArr);
                jlbh.sample(System.nanoTime() - start);
            }
        }
    }

    @Override
    public void complete() {
        sinkQueue.close();
        sourceQueue.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, ITERATIONS, System.out);
    }
}
