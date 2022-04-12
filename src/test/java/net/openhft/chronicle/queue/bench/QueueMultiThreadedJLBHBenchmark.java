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
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.bench.BenchmarkUtils.join;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;

public class QueueMultiThreadedJLBHBenchmark implements JLBHTask {
    private final int iterations;
    private final String path;
    private final int messageSize;
    private final Long blockSize;
    private final String tailerAffinity;
    private final RollCycle rollCycle;
    private final boolean usePretoucher;
    private final boolean useSingleQueueInstance;
    private final BufferMode readBufferMode;
    private final BufferMode writeBufferMode;
    private final Class<?> testClass;
    private SingleChronicleQueue sourceQueue;
    private SingleChronicleQueue sinkQueue;
    private ExcerptTailer tailer;
    private ExcerptAppender appender;
    private final Datum datum;
    private boolean stopped = false;
    private Thread tailerThread;
    private JLBH jlbh;
    private NanoSampler writeProbe;
    private ScheduledExecutorService pretoucherExecutorService;

    public static void main(String[] args) {
        new Builder().run();
    }

    public QueueMultiThreadedJLBHBenchmark(int iterations, String path, String tailerAffinity, @Nullable RollCycle rollCycle,
                                           int messageSize, @Nullable Long blockSize, boolean usePretoucher,
                                           boolean useSingleQueueInstance, @Nullable BufferMode readBufferMode,
                                           @Nullable BufferMode writeBufferMode, @Nullable Class<?> testClass) {
        this.iterations = iterations;
        this.path = path;
        this.tailerAffinity = tailerAffinity;
        this.rollCycle = rollCycle;
        this.messageSize = messageSize;
        this.blockSize = blockSize;
        this.usePretoucher = usePretoucher;
        this.useSingleQueueInstance = useSingleQueueInstance;
        this.readBufferMode = readBufferMode;
        this.writeBufferMode = writeBufferMode;
        this.testClass = testClass == null ? getClass() : testClass;
        this.datum = new Datum(messageSize);
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        IOTools.deleteDirWithFiles(path, 10);

        sourceQueue = createQueueInstance();
        sinkQueue = useSingleQueueInstance ? sourceQueue : createQueueInstance();
        appender = sourceQueue.acquireAppender()
                .disableThreadSafetyCheck(true);
        tailer = sinkQueue.createTailer()
                .disableThreadSafetyCheck(true);

        NanoSampler readProbe = jlbh.addProbe("read");
        writeProbe = jlbh.addProbe("write");

        if (usePretoucher) {
            pretoucherExecutorService = Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("pretoucher", true));
            pretoucherExecutorService.scheduleAtFixedRate(() -> sourceQueue.acquireAppender().pretouch(), 1, 200, TimeUnit.MILLISECONDS);
        }

        tailerThread = new Thread(() -> {
            try (final AffinityLock affinityLock = AffinityLock.acquireLock(tailerAffinity)) {
                Datum datum2 = new Datum(messageSize);
                while (!stopped) {
                    long beforeReadNs = System.nanoTime();
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent())
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

    private SingleChronicleQueue createQueueInstance() {
        final SingleChronicleQueueBuilder builder = single(path);
        if (blockSize != null) {
            builder.blockSize(blockSize);
        }
        if (rollCycle != null) {
            builder.rollCycle(rollCycle);
        }
        if (readBufferMode != null) {
            builder.readBufferMode(readBufferMode);
        }
        if (writeBufferMode != null) {
            builder.writeBufferMode(writeBufferMode);
        }
        return builder.build();
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
        if (pretoucherExecutorService != null) {
            pretoucherExecutorService.shutdownNow();
        }
        stopped = true;
        join(tailerThread);
        sinkQueue.close();
        sourceQueue.close();
        TeamCityHelper.teamCityStatsLastRun(testClass.getSimpleName(), jlbh, iterations, System.out);
    }

    private static class Datum implements BytesMarshallable {
        public long ts = 0;
        public final byte[] filler;

        public Datum(int messageSize) {
            this.filler = new byte[messageSize - 8];
        }

        @Override
        public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException {
            ts = bytes.readLong();
            bytes.read(filler);
        }

        @Override
        public void writeMarshallable(BytesOut<?> bytes) {
            bytes.writeLong(ts);
            bytes.writeSkip(filler.length);
        }
    }

    public static class Builder {
        private int runs = Integer.getInteger("runs", 5);
        private String path = Paths.get(System.getProperty("path", ".")).resolve("replica").normalize().toString();
        private Integer messageSize = Integer.getInteger("messageSize", 256);
        private Long blockSize = Long.getLong("blockSize"); // use default when not specified
        private String producerAffinity = System.getProperty("producerAffinity", "last-1");
        private String consumerAffinity = System.getProperty("consumerAffinity", "last-2");
        private int warmupIterations = Integer.getInteger("warmupIterations", 50_000);
        private int iterations = Integer.getInteger("iterations", 1_000_000);
        private int throughput = Integer.getInteger("throughput", 100_000);
        private boolean usePretoucher = Boolean.getBoolean("usePretoucher");
        private boolean useSingleQueueInstance = Boolean.getBoolean("useSingleQueue");
        @Nullable
        private BufferMode readBufferMode;
        @Nullable
        private BufferMode writeBufferMode;
        @Nullable
        private Class<?> testClass;
        @Nullable
        private RollCycle rollCycle = getRollCycle();

        public void run() {
            System.out.println("-Dpath=" + path);
            final QueueMultiThreadedJLBHBenchmark jlbhTask = new QueueMultiThreadedJLBHBenchmark(iterations, path, consumerAffinity,
                    rollCycle, messageSize, blockSize, usePretoucher, useSingleQueueInstance, readBufferMode, writeBufferMode, testClass);
            JLBHOptions lth = new JLBHOptions()
                    .warmUpIterations(warmupIterations)
                    .iterations(iterations)
                    .throughput(throughput)
                    // disable as otherwise single GC event skews results heavily
                    .recordOSJitter(false)
                    .accountForCoordinatedOmission(false)
                    .skipFirstRun(true)
                    .acquireLock(() -> AffinityLock.acquireLock(producerAffinity))
                    .runs(runs)
                    .jlbhTask(jlbhTask);
            new JLBH(lth).start();
        }

        private RollCycle getRollCycle() {
            final String rollCycle = System.getProperty("rollCycle");
            if (rollCycle != null) {
                return RollCycles.valueOf(rollCycle);
            }
            return null;
        }

        public Builder runs(int runs) {
            this.runs = runs;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder messageSize(Integer messageSize) {
            this.messageSize = messageSize;
            return this;
        }

        public Builder blockSize(Long blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder producerAffinity(String producerAffinity) {
            this.producerAffinity = producerAffinity;
            return this;
        }

        public Builder consumerAffinity(String consumerAffinity) {
            this.consumerAffinity = consumerAffinity;
            return this;
        }

        public Builder warmupIterations(int warmupIterations) {
            this.warmupIterations = warmupIterations;
            return this;
        }

        public Builder iterations(int iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder throughput(int throughput) {
            this.throughput = throughput;
            return this;
        }

        public Builder usePretoucher(boolean usePretoucher) {
            this.usePretoucher = usePretoucher;
            return this;
        }

        public Builder useSingleQueueInstance(boolean useSingleQueueInstance) {
            this.useSingleQueueInstance = useSingleQueueInstance;
            return this;
        }

        public Builder rollCycle(RollCycle rollCycle) {
            this.rollCycle = rollCycle;
            return this;
        }

        public Builder readBufferMode(BufferMode readBufferMode) {
            this.readBufferMode = readBufferMode;
            return this;
        }

        public Builder writeBufferMode(BufferMode writeBufferMode) {
            this.writeBufferMode = writeBufferMode;
            return this;
        }

        public Builder testClass(Class<?> testClass) {
            this.testClass = testClass;
            return this;
        }
    }
}
