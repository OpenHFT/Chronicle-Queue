/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.benchmark;

import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.StackSampler;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.BufferMode;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import static net.openhft.chronicle.queue.benchmark.Main.*;

/**
 * Results 27/10/2015 running on a MBP
 * 50/90 99/99.9 99.99/99.999 - worst was 1.5 / 27  104 / 3,740  8,000 / 13,890 - 36,700
 * <p>
 * Results 14/03/2016 running on E5-2650v2
 * 50/90 99/99.9 99.99 - worst was 0.88 / 1.4  10.0 / 19  72 - 483
 * <p>
 * Results 23/03/2016 running on E5-2643 Debian Kernel 4.2
 * 50/90 99/99.9 99.99 - worst was 0.56 / 0.82  5.0 / 12  40 - 258
 * <p>
 * Results 23/03/2016 running on Linux VM (i7-4800MQ) Debian Kernel 4.2
 * 50/90 99/99.9 99.99 - worst was 0.50 / 1.6  21 / 84  573 - 1,410
 * <p>
 * Results 23/03/2016 running on E3-1505Mv5 Debian Kernel 4.5
 * 50/90 99/99.9 99.99 - worst was 0.33 / 0.36  1.6 / 3.0  18 - 160
 * <p>
 * Results 03/02/2017 running on i7-6700HQ Win 10  100k/s * 5M * 40B
 * 50/90 99/99.9 99.99/99.999 - worst was 0.59 / 0.94  17 / 135  12,850 / 15,470 - 15,990
 * <p>
 * Results 03/02/2017 running on i7-6700HQ Win 10  100k/s * 5M * 40B
 * 50/90 99/99.9 99.99/99.999 - worst was 0.39 / 0.39  0.39 / 28  541 / 967  1,280 / 3,340
 * <p>
 * Results 06/02/2017 running on i7-6700HQ Win 10  1M/s * 5M * 40B
 * 50/90 99/99.9 99.99/99.999 - worst was 0.39 / 0.39  6.3 / 76  516 / 868  999 / 1,030
 * <p>
 * Results 06/02/2017 running on i7-6700HQ Win 10  1.2M/s * 5M * 40B
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.39 / 0.39  12 / 336  2,820 / 3,470  3,600 / 3,600
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 100k/s * 5 M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 - worst was 0.18 / 0.20  0.26 / 0.59  10 / 14 - 117
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 100k/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.19 / 0.23  0.31 / 0.72  10 / 15  88 / 176
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 500k/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.19 / 0.20  0.24 / 8.4  12 / 60  160 / 176
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 500k/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.19 / 0.21  0.25 / 9.0  11 / 76  125 / 135
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 1M/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.19 / 0.20  0.33 / 9.0  15 / 143  176 / 176
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 1.4M/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.18 / 0.20  3.6 / 9.5  96 / 303  336 / 336
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 2.0M/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.19 / 0.20  5.5 / 12  639 / 901  934 / 934
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 2.3M/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.19 / 0.21  9.5 / 6,160  9,700 / 9,700  9,700 / 9,700
 * <p>
 * Results 27/10/2017 running i7-4790, Centos 7 100K/s * 20 M * 40B enableAffinity=true
 * wr: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.014 / 0.017  0.017 / 0.021  0.026 / 0.91  20 / 104
 * in: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.20 / 0.23  0.25 / 1.2  1.5 / 10  29 / 143
 * co: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.21 / 0.24  0.26 / 1.2  1.5 / 10  56 / 143
 * <p>
 * Results 27/10/2017 running i7-4790, Centos 7 1M/s * 20 M * 40B enableAffinity=true
 * wr: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.014 / 0.017  0.019 / 0.025  8.1 / 58  96 / 104
 * in: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.18 / 0.20  0.24 / 0.82  6.8 / 88  143 / 143
 * co: 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.20 / 0.21  0.25 / 0.94  13 / 100  143 / 143
 * <p>
 * I ran with
 * mvn -DenableAffinity=true exec:java -Dexec.classpathScope="test" -Dexec.mainClass=net.openhft.chronicle.queue.benchmark.LatencyDistributionMain
 * <p>
 * Run with 5.19.42, on tmpfs
 * wr: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst was 0.016 / 0.016  0.017 / 0.017  0.017 / 0.017  0.35 / 20  68 / 186  511 / 584 - 612
 * in: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst was 0.23 / 0.24  0.25 / 0.28  0.30 / 0.33  0.73 / 1.5  44 / 137  1,490 / 2,070 - 2,330
 * co: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst was 0.24 / 0.26  0.26 / 0.30  0.32 / 0.35  1.4 / 39  92 / 493  1,490 / 2,070 - 2,350
 * Run with 5.19.42 to an NVMe drive
 * wr: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst was 0.016 / 0.016  0.016 / 0.017  0.017 / 0.017  0.69 / 30  119 / 764  1,590 / 2,080 - 2,320
 * in: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst was 0.20 / 0.22  0.22 / 0.27  0.28 / 0.49  1.0 / 1.8  41 / 69  98 / 164 - 2,300
 * co: 50/90 97/99 99.7/99.9 99.97/99.99 99.997/99.999 99.9997/99.9999 - worst was 0.22 / 0.23  0.24 / 0.28  0.30 / 0.93  1.7 / 46  135 / 764  1,590 / 2,150 - 2,430
 */
public class LatencyDistributionMain {
    private static final int INTLOG_INTERVAL = 20_000_000;
    @Nullable
    final StackSampler sampler = SAMPLING ? new StackSampler() : null;

    public static void main(String[] args) throws InterruptedException {
        assert false : "test runs slower with assertions on";

        System.out.println("Testing with " +
                "-Dtime=" + time + " " +
                "-Dthreads=" + threads + " " +
                "-Dsize=" + size + " " +
                "-Dpath=" + path + " " +
                "-Dthroughput=" + throughput + " " +
                "-Dinterations=" + iterations);
        new LatencyDistributionMain().run(args);
    }

    public void run(String[] args) throws InterruptedException {
        File tmpDir = getTmpDir();
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder
                .fieldlessBinary(tmpDir)
                .blockSize(128 << 20);
        try (ChronicleQueue queue = builder
                .writeBufferMode(BUFFER_MODE)
                .readBufferMode(BufferMode.None)
                .build();
             ChronicleQueue queue2 = builder
                     .writeBufferMode(BufferMode.None)
                     .readBufferMode(BUFFER_MODE)
                     .build()) {

            runTest(queue, queue2);
        }
        IOTools.deleteDirWithFiles(tmpDir, 2);
    }

    private File getTmpDir() {
        return new File(Main.path + "/delete-" + Time.uniqueId() + ".me");
    }

    protected void runTest(@NotNull ChronicleQueue queue, @NotNull ChronicleQueue queue2) throws InterruptedException {

        Histogram histogramCo = new Histogram();
        Histogram histogramIn = new Histogram();
        Histogram histogramWr = new Histogram();
        Thread pretoucher = new Thread(() -> {
            ExcerptAppender appender = queue.acquireAppender();
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    appender.pretouch();
                    Jvm.pause(50);
                }
            } catch (Exception e) {
                if (!appender.isClosed())
                    e.printStackTrace();
            }
        });
        pretoucher.setDaemon(true);
        pretoucher.start();

        ExcerptAppender appender = queue.acquireAppender();
        // two queues as most like in a different process.
        ExcerptTailer tailer = queue2.createTailer();

        String name = getClass().getName();
        Thread tailerThread = new Thread(() -> {
            AffinityLock lock = null;
            try {
                if (Jvm.getBoolean("enableTailerAffinity") || !Jvm.getBoolean("disableAffinity")) {
                    lock = Affinity.acquireLock();
                }
                int counter = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        // if (SAMPLING)
                        // sampler.thread(Thread.currentThread());
                        // boolean found = tailer.readDocument(myReadMarshallable);
                        boolean found;
                        try (DocumentContext dc = tailer.readingDocument()) {
                            found = dc.isPresent();
                            if (found) {
                                int count = counter++;
                                if (count == WARMUP) {
                                    histogramCo.reset();
                                    histogramIn.reset();
                                    histogramWr.reset();
                                }
                                Bytes<?> bytes = dc.wire().bytes();
                                long startCo = bytes.readLong();
                                long startIn = bytes.readLong();
                                long now = System.nanoTime();
                                histogramCo.sample(now - startCo);
                                histogramIn.sample(now - startIn);
                                if (count % INTLOG_INTERVAL == 0) System.out.println("read  " + count);
                            }
                        }
/*
                        if (SAMPLING) {
                            StackTraceElement[] stack = sampler.getAndReset();
                            if (stack != null) {
                                if (!stack[0].getClassName().equals(name) &&
                                        !stack[0].getClassName().equals("java.lang.Thread")) {
                                    StringBuilder sb = new StringBuilder();
                                    Jvm.trimStackTrace(sb, stack);
                                   // System.out.println(sb);
                                }
                            } else if (!found) {
                                Thread.yield();
                            }
                        }
                        */

                    } catch (Exception e) {
                        break;
                    }
                }
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        });

        Thread appenderThread = new Thread(() -> {
            AffinityLock lock = null;
            try {
                if (Jvm.getBoolean("enableAppenderAffinity") || !Jvm.getBoolean("disableAffinity")) {
                    lock = Affinity.acquireLock();
                }

                long next = System.nanoTime();
                long interval = 1_000_000_000 / throughput;
                Map<String, Integer> stackCount = new LinkedHashMap<>();
                BytesStore<?, ?> bytes24 = BytesStore.nativeStoreFrom(new byte[Main.size - 16]);
                for (int i = -WARMUP; i < iterations; i++) {
                    long s0 = System.nanoTime();
                    if (s0 < next) {
                        do ; while (System.nanoTime() < next);
                        next = System.nanoTime(); // if we failed to come out of the spin loop on time, reset next.
                    }

                    if (SAMPLING) {
                        sampler.thread(Thread.currentThread());
                    }
                    long start = System.nanoTime();
                    try (@NotNull DocumentContext dc = appender.writingDocument(false)) {
                        Wire wire = dc.wire();
                        Bytes<?> bytes2 = wire.bytes();
                        bytes2.writeLong(next); // when it should have started
                        bytes2.writeLong(start); // when it actually started.
                        bytes2.write(bytes24);
                        ThroughputMain.addToEndOfCache(wire);
                    }
                    long time = System.nanoTime() - start;
                    histogramWr.sample(start - next);
                    if (SAMPLING && time > 1e3 && i > 0) {
                        StackTraceElement[] stack = sampler.getAndReset();
                        if (stack != null) {
                            if (!stack[0].getClassName().equals(name) &&
                                    !stack[0].getClassName().equals("java.lang.Thread")) {
                                StringBuilder sb = new StringBuilder();
                                Jvm.trimStackTrace(sb, stack);
                                stackCount.compute(sb.toString(), (k, v) -> v == null ? 1 : v + 1);
                            }
                        }
                    }
                    next += interval;
                    if (i % INTLOG_INTERVAL == 0) System.out.println("wrote " + i);
                }
                stackCount.entrySet().stream()
                        .filter(e -> e.getValue() > 1)
                        .forEach(System.out::println);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (lock != null) {
                    lock.release();
                }
            }
        });

        tailerThread.start();

        appenderThread.start();
        appenderThread.join();

        pretoucher.interrupt();
        pretoucher.join();

        //Pause to allow tailer to catch up (if needed)
        Jvm.pause(500);
        tailerThread.interrupt();
        tailerThread.join();

        System.out.println("wr: " + histogramWr.toLongMicrosFormat());
        System.out.println("in: " + histogramIn.toLongMicrosFormat());
        System.out.println("co: " + histogramCo.toLongMicrosFormat());
    }
}
