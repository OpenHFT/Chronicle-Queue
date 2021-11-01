/*
 * Copyright 2014-2020 chronicle.software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.bench;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.jlbh.TeamCityHelper;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.queue.bench.BenchmarkUtils.join;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueContendedWritesJLBHBenchmark implements JLBHTask {
    public static final int ITERATIONS = 100_000;
    private SingleChronicleQueue queue;
    private ExcerptTailer tailer;
    private JLBH jlbh;
    private NanoSampler concurrent;
    private NanoSampler concurrent2;
    private volatile boolean stopped = false;
    private AtomicInteger write = new AtomicInteger(0);
    private final Datum datum = new Datum();
    private final Datum datum2 = new Datum();
    private Thread writerThread1;
    private Thread writerThread2;

    public static void main(String[] args) {
        // disable as otherwise single GC event skews results heavily
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50_000)
                .iterations(ITERATIONS)
                .throughput(10_000)
                .recordOSJitter(false).accountForCoordinatedOmission(false)
                .skipFirstRun(true)
                .runs(3)
                .jlbhTask(new QueueContendedWritesJLBHBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFiles("replica", 10);

        this.jlbh = jlbh;
        concurrent = jlbh.addProbe("Concurrent");
        concurrent2 = jlbh.addProbe("Concurrent2");
        queue = single("replica").rollCycle(RollCycles.LARGE_DAILY).doubleBuffer(false).build();
        tailer = queue.createTailer().disableThreadSafetyCheck(true);
        tailer.toStart();
        writerThread1 = new Thread(() -> {
            try (final AffinityLock affinityLock = AffinityLock.acquireCore()) {
                final ExcerptAppender app = queue.acquireAppender();

                while (!stopped) {
                    if (write.get() <= 0)
                        continue;
                    write.decrementAndGet();
                    final long start = System.nanoTime();
                    datum2.ts = start;
                    datum2.username = "" + start;
                    try (DocumentContext dc = app.writingDocument()) {
                        dc.wire().write("datum").marshallable(datum2);
                    }
                    this.concurrent2.sampleNanos(System.nanoTime() - start);
                }
                queue.close();
            }
        });
        writerThread1.start();

        writerThread2 = new Thread(() -> {
            try (final AffinityLock affinityLock = AffinityLock.acquireCore()) {
                final ExcerptAppender app = queue.acquireAppender();

                while (!stopped) {
                    if (write.get() <= 0)
                        continue;
                    write.decrementAndGet();
                    final long start = System.nanoTime();
                    datum.ts = start;
                    datum.username = "" + start;
                    try (DocumentContext dc = app.writingDocument()) {
                        dc.wire().write("datum").marshallable(datum);
                    }
                    concurrent.sampleNanos(System.nanoTime() - start);
                }
                queue.close();
            }
        });
        writerThread2.start();
    }

    long written = 0;

    @Override
    public void run(long startTimeNS) {
        write.set(2);
        int read = 0;
        while (read < 2) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.wire() == null)
                    continue;
                if (dc.wire().read("datum").marshallable(datum))
                    read++;
            }
        }

        jlbh.sampleNanos(System.nanoTime() - startTimeNS);
        written++;
        if (written % 10_000 == 0)
            System.err.println("Written: " + written);
    }

    @Override
    public void complete() {
        stopped = true;
        join(writerThread1);
        join(writerThread2);
        queue.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, ITERATIONS, System.out);
    }

    private static class Datum extends SelfDescribingMarshallable {
        public long ts = 0;
        public String username;
        public byte[] filler0 = new byte[128];
        public byte[] filler1 = new byte[128];
        public byte[] filler2 = new byte[128];
        public byte[] filler3 = new byte[128];
        public byte[] filler4 = new byte[128];
        public byte[] filler5 = new byte[128];
        public byte[] filler6 = new byte[128];
        public byte[] filler7 = new byte[128];
        public byte[] filler8 = new byte[128];
        public byte[] filler9 = new byte[128];

        public byte[] filler10 = new byte[128];
        public byte[] filler11 = new byte[128];
        public byte[] filler12 = new byte[128];
        public byte[] filler13 = new byte[128];
        public byte[] filler14 = new byte[128];
        public byte[] filler15 = new byte[128];
        public byte[] filler16 = new byte[128];
        public byte[] filler17 = new byte[128];
        public byte[] filler18 = new byte[128];
        public byte[] filler19 = new byte[128];

        public byte[] filler20 = new byte[128];
        public byte[] filler21 = new byte[128];
        public byte[] filler22 = new byte[128];
        public byte[] filler23 = new byte[128];
        public byte[] filler24 = new byte[128];
        public byte[] filler25 = new byte[128];
        public byte[] filler26 = new byte[128];
        public byte[] filler27 = new byte[128];
        public byte[] filler28 = new byte[128];
        public byte[] filler29 = new byte[128];

        public byte[] filler30 = new byte[128];
        public byte[] filler31 = new byte[128];
        public byte[] filler32 = new byte[128];
        public byte[] filler33 = new byte[128];
        public byte[] filler34 = new byte[128];
        public byte[] filler35 = new byte[128];
        public byte[] filler36 = new byte[128];
        public byte[] filler37 = new byte[128];
        public byte[] filler38 = new byte[128];
        public byte[] filler39 = new byte[128];
    }
}
