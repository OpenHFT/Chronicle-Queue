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
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
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
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueMultiThreadedJLBHBenchmark2 implements JLBHTask {
    private static final String PATH = System.getProperty("path", "/dev/shm") + "/replica";
    private static final int MSGSIZE = 512;
    private final static String CPU1 = System.getProperty("cpu1", "last-1");
    private final static String CPU2 = System.getProperty("cpu2", "last-2");
    private static final int ITERATIONS = 2_000_000_000;
    private static final long BLOCKSIZE = 4L << 40;
    private static final int RUNS = 3;
    private static volatile long startTime;
    private static volatile Thread thread;
    private final Datum datum = new Datum();
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private boolean stopped = false;
    private Thread tailerThread;
    private JLBH jlbh;
    private NanoSampler writeProbe;
    private ScheduledExecutorService pretoucher;

    public static void main(String[] args) {
        System.out.println("-Dpath=" + PATH + " -Dcpu1=" + CPU1 + " -Dcpu2=" + CPU2);
        warmUp();

        QueueMultiThreadedJLBHBenchmark2 bench = new QueueMultiThreadedJLBHBenchmark2();

        for (int r = 0; r <= 1; r++) {
            int[] throughputs = {1_500_000, 250_000};
            for (int throughput : throughputs) {
                System.out.println("Throughput: " + (throughput / 1000) + "k msgs/s");
                bench.run1(throughput, r == 0 ? 150_000_000 : ITERATIONS);
            }
        }
    }

    private static void warmUp() {
        System.setProperty("SingleChronicleQueueExcerpts.earlyAcquireNextCycle", "true");
        MappedFile.warmup();

/*        Thread watcher = new Thread(() -> {
            while (true) {
                long took = System.nanoTime() - startTime;
                if (took > 1e6) {
                    try {
                        final StackTrace stackTrace = StackTrace.forThread(thread);
                        if (startTime < Long.MAX_VALUE)
                            stackTrace.printStackTrace();
                        else
                            System.err.println("... watcher missed");
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                    startTime = Long.MAX_VALUE;
                }
                Thread.yield();
            }
        }, "watcher");
        watcher.setDaemon(true);
        watcher.start();*/
    }

    void run1(int throughput, int iterations) {
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(5_000_000)
                .iterations(iterations)
                .throughput(throughput)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(false)
                .skipFirstRun(true)
                .acquireLock(() -> AffinityLock.acquireLock(CPU1))
                .runs(1)
                .jlbhTask(new QueueMultiThreadedJLBHBenchmark2());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        IOTools.deleteDirWithFiles(PATH, 10);
        sourceQueue = single(PATH).blockSize(BLOCKSIZE).rollCycle(RollCycles.HUGE_DAILY).build();
        sinkQueue = sourceQueue; //single(PATH).blockSize(BLOCKSIZE).rollCycle(RollCycles.HUGE_DAILY).build();
        appender = sourceQueue.acquireAppender();
        tailer = sinkQueue.createTailer().disableThreadSafetyCheck(true);

        NanoSampler readProbe = jlbh.addProbe("read");
        writeProbe = jlbh.addProbe("write");

        pretoucher = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("pretoucher", true));
        pretoucher.scheduleAtFixedRate(() -> sourceQueue.acquireAppender().pretouch(), 1, 200, TimeUnit.MILLISECONDS);

        tailerThread = new Thread(() -> {
            try (AffinityLock lock = AffinityLock.acquireLock(CPU2)) {
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
        startTime = startTimeNS;
        if (thread == null)
            thread = Thread.currentThread();
        datum.ts = startTimeNS;
        try (DocumentContext dc = appender.writingDocument()) {
            Jvm.safepoint();
            datum.writeMarshallable(dc.wire().bytes());
        }
        long nanos = System.nanoTime() - startTimeNS;
        writeProbe.sampleNanos(nanos);
        startTime = Long.MAX_VALUE;

/*
        if (nanos > 1_000_000) {
            Jvm.safepoint();
            System.out.println("+++ Took " + nanos / 1000 + " us to write Datum");
        }
*/
    }

    @Override
    public void complete() {
        pretoucher.shutdownNow();
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
