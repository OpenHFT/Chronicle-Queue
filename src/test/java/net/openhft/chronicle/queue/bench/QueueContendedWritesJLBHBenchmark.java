/*
 * Copyright 2014-2017 Chronicle Software
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

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.jlbh.JLBH;
import net.openhft.chronicle.core.jlbh.JLBHOptions;
import net.openhft.chronicle.core.jlbh.JLBHTask;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueContendedWritesJLBHBenchmark implements JLBHTask {
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptAppender appender;
    private ExcerptTailer tailer;
    private JLBH jlbh;
    private NanoSampler concurrent;
    private NanoSampler concurrent2;
    private volatile boolean stopped = false;
    private volatile boolean write = false;
    private final Datum datum = new Datum();
    private final Datum datum2 = new Datum();

    public static void main(String[] args) {
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(100_000)
                .iterations(3_000_000)
                .throughput(100_000)
                .recordOSJitter(false)
                // disable as otherwise single GC event skews results heavily
                .accountForCoordinatedOmmission(false)
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
        sourceQueue = single("replica").rollCycle(RollCycles.LARGE_DAILY).build();
        sinkQueue = single("replica").rollCycle(RollCycles.LARGE_DAILY).doubleBuffer(true).build();
        appender = sourceQueue.acquireAppender();
        tailer = sourceQueue.createTailer();
        tailer.toStart();
        new Thread(() -> {
            final ExcerptAppender app = sourceQueue.acquireAppender();

            while (!stopped) {
                if (!write)
                    continue;
                write = false;
                final long start = System.nanoTime();
                datum2.ts = start;
                try (DocumentContext dc = app.writingDocument()) {
                    datum2.writeMarshallable(dc.wire().bytes());
                }
                this.concurrent.sampleNanos(System.nanoTime() - start);
            }

            //System.err.println("Total writes: " + i);
        }).start();
    }

    @Override
    public void warmedUp() {

    }

    long written = 0;
    @Override
    public void run(long startTimeNS) {
        final long start = System.nanoTime();
        write = true;
        datum.ts = startTimeNS;
        try (DocumentContext dc = appender.writingDocument()) {
            datum.writeMarshallable(dc.wire().bytes());
        }
        final long now = System.nanoTime();
        concurrent2.sampleNanos(now - start);

        int read = 0;
        while (read < 2) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.wire() == null)
                    continue;
                datum.readMarshallable(dc.wire().bytes());
                read++;
            }
        }

        jlbh.sampleNanos(now - startTimeNS);
        written++;
    }

    @Override
    public void complete() {
        stopped = true;
        write = true;
        sinkQueue.close();
        sourceQueue.close();
        System.err.println("Total writes (main): " + written);
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
            bytes.write(filler);
        }
    }
}
