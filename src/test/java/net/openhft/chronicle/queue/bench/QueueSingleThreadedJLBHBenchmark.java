/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.FacadeInterfaces.IFacade;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueSingleThreadedJLBHBenchmark implements JLBHTask {
    private static final String PATH = System.getProperty("path", "replica");
    private static final int ITERATIONS = 1_000_000;
    private final IFacade datum = Values.newNativeReference(IFacade.class);
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private JLBH jlbh;
    private BytesStore<?, ?> datumBytes;
    private Bytes<?> datumWrite;

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
    }

    public static void main(String[] args) {
        // disable as otherwise single GC event skews results heavily
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50000)
                .iterations(ITERATIONS)
                .throughput(100_000)
                .recordOSJitter(false)
                .accountForCoordinatedOmission(false)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new QueueSingleThreadedJLBHBenchmark());
        new JLBH(lth).start();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void init(JLBH jlbh) {
        System.out.println("-Dpath=" + PATH);
        IOTools.deleteDirWithFiles(PATH, 10);

        Byteable byteable = (Byteable) datum;
        long capacity = byteable.maxSize();
        byteable.bytesStore(BytesStore.nativeStore(capacity), 0, capacity);
        datumBytes = ((Byteable) datum).bytesStore();
        datumWrite = datumBytes.bytesForWrite();

        sourceQueue = single(PATH).build();
        sinkQueue = single(PATH).build();
        appender = sourceQueue.createAppender();
        tailer = sinkQueue.createTailer();
        tailer.singleThreadedCheckDisabled(true);
        this.jlbh = jlbh;
    }

    @Override
    public void run(long startTimeNS) {
        datum.setValue10(startTimeNS);

        try (DocumentContext dc = appender.writingDocument()) {
            dc.wire().bytes().write(datumBytes);
        }

        try (DocumentContext dc = tailer.readingDocument()) {
            if (dc.wire() != null) {
                datumWrite.writePosition(0);
                dc.wire().readBytes(datumWrite);
                jlbh.sample(System.nanoTime() - datum.getValue10());
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
