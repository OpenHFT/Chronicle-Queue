/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.StackSampler;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

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
 * 50/90 99/99.9 99.99/99.999 - worst was 0.59 / 0.94  17 / 135  12,850 / 15,470 - 15,990
 * <p>
 * Results 05/02/2017 running i7-4790, Centos 7 100k/s * 5 M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 - worst was 0.18 / 0.20  0.26 / 0.59  10 / 14 - 117
 * </p>
 * Results 05/02/2017 running i7-4790, Centos 7 100k/s * 20M * 40B enableAffinity=true
 * 50/90 99/99.9 99.99/99.999 99.9999/worst was 0.19 / 0.23  0.31 / 0.72  10 / 15  88 / 176
 */
public class ChronicleQueueLatencyDistribution extends ChronicleQueueTestBase {
    static final boolean SAMPLING = Boolean.getBoolean("sampling");
    static final int warmup = 500_000;
    final StackSampler sampler = SAMPLING ? new StackSampler() : null;

    public static void main(String[] args) throws IOException, InterruptedException {
        assert false : "test runs slower with assertions on";
        new ChronicleQueueLatencyDistribution().run();
    }

    public void run() throws IOException, InterruptedException {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder
                .fieldlessBinary(getTmpDir())
                .blockSize(128 << 20)
                .build()) {

            runTest(queue, 100_000);
        }
    }

    protected void runTest(ChronicleQueue queue, int throughput) throws InterruptedException {
/*
        Jvm.setExceptionHandlers(PrintExceptionHandler.ERR,
                PrintExceptionHandler.OUT,
                PrintExceptionHandler.OUT);
*/

        Histogram histogramCo = new Histogram();
        Histogram histogramIn = new Histogram();
        Histogram histogramWr = new Histogram();
        Thread pretoucher = new Thread(() -> {
            ExcerptAppender appender = queue.acquireAppender();
            while (!Thread.currentThread().isInterrupted()) {
                appender.pretouch();
                Jvm.pause(500);
            }
        });
        pretoucher.setDaemon(true);
        pretoucher.start();

        ExcerptAppender appender = queue.acquireAppender().lazyIndexing(true);
        ExcerptTailer tailer = queue.createTailer();

        String name = getClass().getName();
        Thread tailerThread = new Thread(() -> {
            AffinityLock lock = null;
            try {
                if (Boolean.getBoolean("enableTailerAffinity") || Boolean.getBoolean("enableAffinity")) {
                    lock = Affinity.acquireLock();
                }
                int counter = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
//                        if (SAMPLING)
//                            sampler.thread(Thread.currentThread());
//                        boolean found = tailer.readDocument(myReadMarshallable);
                        boolean found;
                        try (DocumentContext dc = tailer.readingDocument()) {
                            found = dc.isPresent();
                            if (found) {
                                int count = counter++;
                                if (count == warmup) {
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
                                    System.out.println(sb);
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
                if (Boolean.getBoolean("enableAppenderAffinity") || Boolean.getBoolean("enableAffinity")) {
                    lock = Affinity.acquireLock();
                }

                long next = System.nanoTime();
                long interval = 1_000_000_000 / throughput;
                Map<String, Integer> stackCount = new LinkedHashMap<>();
                NativeBytesStore bytes24 = NativeBytesStore.from(new byte[24]);
                for (int i = -warmup; i < 20_000_000; i++) {
                    long s0 = System.nanoTime();
                    if (s0 < next) {
                        busyLoop:
                        do ; while (System.nanoTime() < next);
                        next = System.nanoTime(); // if we failed to come out of the spin loop on time, reset next.
                    }

                    if (SAMPLING) {
                        sampler.thread(Thread.currentThread());
                    }
                    long start = System.nanoTime();
                    try (@NotNull DocumentContext dc = appender.writingDocument(false)) {
                        Bytes<?> bytes2 = dc.wire().bytes();
                        bytes2.writeLong(next); // when it should have started
                        bytes2.writeLong(start); // when it actually started.
                        bytes2.write(bytes24);
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

        //Pause to allow tailer to catch up (if needed)
        Jvm.pause(500);
        tailerThread.interrupt();
        tailerThread.join();

        System.out.println("wr: " + histogramWr.toMicrosFormat());
        System.out.println("in: " + histogramIn.toMicrosFormat());
        System.out.println("co: " + histogramCo.toMicrosFormat());
    }
}
