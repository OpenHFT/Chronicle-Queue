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
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.bench.BenchmarkUtils.join;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

/*
Chronicle Queue performance is compared when using sparse files and chunking( with standard and large blockSize).
mvn exec:java -Dexec.mainClass=net.openhft.chronicle.queue.bench.QueueSparseFilesJLBHBenchmark -Dexec.classpathScope=test
 */
public class QueueSparseFilesJLBHBenchmark implements JLBHTask {
    private static final int throughput = Integer.getInteger("throughput", 10_000);
    private static final int runTime = Integer.getInteger("runTime", 30);
    private static final long iterations = (long) throughput * runTime;
    private final int round;
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private JLBH jlbh;
    private NanoSampler writeProbe;
    private NanoSampler readProbe;
    private boolean stopped = false;
    private Thread tailerThread;
    private static final long CHUNKSIZE = 4L << 10;
    private static final int WARMUP = 200_000;
    private static final int WRITE_CPU = 8;
    private static final int READ_CPU = 9;
    private static final int RUNS = 5;
    private static final long SPARSEBLOCK = 1L << 40;
    private static final long NORMALBLOCK = 64L << 20;
    private static final String ROOTDIR="/mnt/local/benchmark/";

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }

    public QueueSparseFilesJLBHBenchmark(int round) {
        this.round = round;
    }

    public static void main(String... args) {
        System.out.println("-Dthroughput=" + throughput
                + " -DrunTime=" + runTime);
        for (int round = 1; round <= 3; round++) {
            System.out.println("========\nround= " + round + "\n========\n");
            JLBHOptions lth = new JLBHOptions()
                    .warmUpIterations(WARMUP)
                    .iterations(iterations)
                    .throughput(throughput)
                    .recordOSJitter(false)
                    .skipFirstRun(true)
                    .acquireLock(()->AffinityLock.acquireLock(WRITE_CPU))
                    .runs(RUNS)
                    .jlbhTask(new QueueSparseFilesJLBHBenchmark(round));
            new JLBH(lth).start();
        }
    }

    @Override
    public void init(JLBH jlbh) {
        IOTools.deleteDirWithFiles(ROOTDIR, 5);

        if (round == 1) {
            // Uses sparse file for memory mapping
            sourceQueue = single(ROOTDIR + "sparseFile").useSparseFiles(true).sparseCapacity(SPARSEBLOCK).build();
            sinkQueue = single(ROOTDIR + "sparseFile").useSparseFiles(true).sparseCapacity(SPARSEBLOCK).build();
        } else if (round == 2) {
            // UsesConfigures chunking for memory mapping using a large memory block
            sourceQueue = single(ROOTDIR + "chunking-largeBlockSize").blockSize(SPARSEBLOCK).build();
            sinkQueue = single(ROOTDIR + "chunking-largeBlockSize").blockSize(SPARSEBLOCK).build();
        } else {
            // Configures chunking for memory mapping using a small memory block
            sourceQueue = single(ROOTDIR + "chunking-standardBlockSize").blockSize(NORMALBLOCK).build();
            sinkQueue = single(ROOTDIR + "chunking-standardBlockSize").blockSize(NORMALBLOCK).build();
        }
        appender = sourceQueue.acquireAppender();
        appender.singleThreadedCheckDisabled(true);
        tailer = sinkQueue.createTailer();
        tailer.singleThreadedCheckDisabled(true);
        this.jlbh = jlbh;
        writeProbe = round == 1 ? jlbh.addProbe("write (sparse file)") : round == 2 ? jlbh.addProbe("write (chunking-largeBlockSize)") : jlbh.addProbe("write (chunking-standardBlockSize)");
        readProbe = round == 1 ? jlbh.addProbe("read (sparse file)") : round == 2 ? jlbh.addProbe("read (chunking-largeBlockSize)") : jlbh.addProbe("read (chunking-standardBlockSize)");

        tailerThread = new Thread(() -> {
            try (final AffinityLock affinityLock = AffinityLock.acquireLock(READ_CPU)) {
                while (!stopped) {
                    long beforeReadNS = System.nanoTime();
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent())
                            continue;
                        Bytes bytes = dc.wire().bytes();
                        bytes.readSkip(CHUNKSIZE);
                        long writeNS = bytes.readLong();

                        long now = System.nanoTime();
                        jlbh.sample(now - writeNS);
                        readProbe.sampleNanos(now - beforeReadNS);
                    }
                }

            }
        });
        tailerThread.start();
    }

    @Override
    public void run(long startTimeNS) {
        try (DocumentContext dc = appender.writingDocument()) {
            Bytes bytes = dc.wire().bytes();
            bytes.writeSkip(CHUNKSIZE);
            bytes.writeLong(System.nanoTime());
        }
        writeProbe.sampleNanos(System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        stopped = true;
        join(tailerThread);

        sinkQueue.close();
        sourceQueue.close();

        IOTools.deleteDirWithFiles(ROOTDIR, 5);
    }
}
