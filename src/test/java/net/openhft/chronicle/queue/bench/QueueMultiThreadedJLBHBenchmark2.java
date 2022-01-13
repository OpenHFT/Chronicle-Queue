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

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.queue.RollCycles;

public class QueueMultiThreadedJLBHBenchmark2 {
    private static final String PATH = System.getProperty("path", "/dev/shm") + "/replica";
    private static final int MSGSIZE = 512;
    private final static String CPU1 = System.getProperty("cpu1", "last-1");
    private final static String CPU2 = System.getProperty("cpu2", "last-2");
    private static final int ITERATIONS = 2_000_000_000;
    private static final long BLOCKSIZE = 4L << 40;

    public static void main(String[] args) {
        System.out.println("-Dpath=" + PATH + " -Dcpu1=" + CPU1 + " -Dcpu2=" + CPU2);
        warmUp();

        final QueueMultiThreadedJLBHBenchmark.Builder commonOptions = new QueueMultiThreadedJLBHBenchmark.Builder()
                .runs(1)
                .path(PATH)
                .producerAffinity(CPU1)
                .consumerAffinity(CPU2)
                .warmupIterations(5_000_000)
                .usePretoucher(true)
                .useSingleQueueInstance(true)
                .messageSize(MSGSIZE)
                .blockSize(BLOCKSIZE)
                .rollCycle(RollCycles.HUGE_DAILY)
                .testClass(QueueMultiThreadedJLBHBenchmark2.class);

        for (int r = 0; r <= 1; r++) {
            int[] throughputs = {1_500_000, 250_000};
            for (int throughput : throughputs) {
                System.out.println("Throughput: " + (throughput / 1000) + "k msgs/s");
                commonOptions.throughput(throughput)
                        .iterations(r == 0 ? 150_000_000 : ITERATIONS)
                        .run();
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
}
