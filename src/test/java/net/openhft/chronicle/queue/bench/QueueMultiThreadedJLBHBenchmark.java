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

import net.openhft.affinity.Affinity;
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

import java.util.ArrayList;
import java.util.Arrays;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueMultiThreadedJLBHBenchmark implements JLBHTask {
    private static final String PATH = "/dev/shm/replica";
    private static final int MSGSIZE = 512;
    private final static int CPU1 = 2;
    private final static int CPU2 = 4;
    private static final int ITERATIONS = 2_000_000_000;
    private static final long BLOCKSIZE = 8L << 30;
    private static final int RUNS = 3;

    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private final Datum datum = new Datum();
    private boolean stopped = false;
    private Thread tailerThread;
    private JLBH jlbh;
    private NanoSampler writeProbe;

    private void run1(int THROUGHPUT) {
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(500_000)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false).accountForCoordinatedOmission(false)
                .skipFirstRun(true)
                .acquireLock(()-> AffinityLock.acquireLock(CPU1))
                .runs(RUNS)
                .jlbhTask(new QueueMultiThreadedJLBHBenchmark());
        new JLBH(lth).start();
    }

    public static void main(String[] args) {
        QueueMultiThreadedJLBHBenchmark bench = new QueueMultiThreadedJLBHBenchmark();

        ArrayList<Integer> throughputs = new ArrayList<>(Arrays.asList(250_000, 1_500_000));
        for(int throughput : throughputs) {
            System.out.println("Throughput: " + (throughput/1000) + "k msgs/s");
            bench.run1(throughput);
        }
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        IOTools.deleteDirWithFiles(PATH, 10);

        sourceQueue = single(PATH).blockSize(BLOCKSIZE).build();
        sinkQueue = single(PATH).blockSize(BLOCKSIZE).build();
        appender = sourceQueue.acquireAppender();
        tailer = sinkQueue.createTailer();

        NanoSampler readProbe = jlbh.addProbe("read");
        writeProbe = jlbh.addProbe("write");
        tailerThread = new Thread(() -> {
            Affinity.setAffinity(CPU2);
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
        try {
            tailerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sinkQueue.close();
        sourceQueue.close();
        TeamCityHelper.teamCityStatsLastRun(getClass().getSimpleName(), jlbh, ITERATIONS, System.out);
    }

    private static class Datum implements BytesMarshallable {
        public long ts = 0;
        public byte[] filler = new byte[MSGSIZE - 8];

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
