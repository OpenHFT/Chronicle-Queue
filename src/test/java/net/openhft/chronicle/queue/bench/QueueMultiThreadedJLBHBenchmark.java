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
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
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

import static net.openhft.chronicle.queue.bench.BenchmarkUtils.join;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueMultiThreadedJLBHBenchmark implements JLBHTask {
    private static final int ITERATIONS = 1_000_000;
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private Datum datum = new Datum();
    private boolean stopped = false;
    private Thread tailerThread;
    private JLBH jlbh;
    private NanoSampler writeProbe;

    public static void main(String[] args) {
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50000)
                .iterations(ITERATIONS)
                .throughput(100_000)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(false)
                .skipFirstRun(true)
                .runs(5)
                .jlbhTask(new QueueMultiThreadedJLBHBenchmark());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        IOTools.deleteDirWithFiles("replica", 10);

        sourceQueue = single("replica").build();
        sinkQueue = single("replica").build();
        appender = sourceQueue.acquireAppender()
                .disableThreadSafetyCheck(true);
        tailer = sinkQueue.createTailer()
                .disableThreadSafetyCheck(true);

        NanoSampler readProbe = jlbh.addProbe("read");
        writeProbe = jlbh.addProbe("write");
        tailerThread = new Thread(() -> {
            try (final AffinityLock affinityLock = AffinityLock.acquireCore()) {
                Datum datum2 = new Datum();
                while (!stopped) {
                    long beforeReadNs = System.nanoTime();
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (dc.wire() == null)
                            continue;
                        datum2.readMarshallable(dc.wire().bytes());
                        long now = System.nanoTime();
                        jlbh.sample(now - datum2.ts);
                        readProbe.sampleNanos(now - beforeReadNs);
                    }
                }
            }
        });
        tailerThread.start();
    }

    @Override
    public void run(long startTimeNS) {
        datum.ts = startTimeNS;
        try (DocumentContext dc = appender.writingDocument()) {
            datum.writeMarshallable(dc.wire().bytes());
        }
        writeProbe.sampleNanos(System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        stopped = true;
        join(tailerThread);
        sinkQueue.close();
        sourceQueue.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, ITERATIONS, System.out);
    }

    private static class Datum implements BytesMarshallable {
        public long ts = 0;
        public byte[] filler = new byte[4088];

        @Override
        public void readMarshallable(BytesIn bytes) throws IORuntimeException {
            ts = bytes.readLong();
            bytes.read(filler);
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeLong(ts);
            bytes.writeSkip(filler.length);
        }
    }
}
