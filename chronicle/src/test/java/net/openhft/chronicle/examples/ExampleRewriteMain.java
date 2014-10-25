/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.examples;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author peter.lawrey
 *         <p></p>
 *         Chronicle 1.7: 50.0% took 0.3 µs,  90.0% took 0.4 µs,  99.0% took 33.5 µs,  99.9% took 66.9 µs,  99.99% took
 *         119.7 µs,  worst took 183 µs Chronicle 2.0: 50.0% took 0.13 µs, 90.0% took 0.15 µs, 99.0% took 0.44 µs, 99.9%
 *         took 14.37 µs, 99.99% took 22.16 µs, worst took 40 µs
 */
public class ExampleRewriteMain {
    public static void main(String... ignored) throws IOException {
        ChronicleTools.warmup();

        final String basePath = System.getProperty("java.io.tmpdir") + File.separator + "test";
        ChronicleTools.deleteOnExit(basePath);
        final int[] consolidates = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        final int warmup = 50000;
        final int repeats = 5 * 1000 * 1000;
        final int rate = 1 * 1000 * 1000;

        final AffinityLock al = AffinityLock.acquireLock();
        //Write
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    al.acquireLock(AffinityStrategies.DIFFERENT_CORE).bind();

                    final IndexedChronicle chronicle = new IndexedChronicle(basePath);
//                    chronicle.useUnsafe(true); // for benchmarks.
                    final ExcerptAppender excerpt = chronicle.createAppender();
                    for (int i = -warmup; i < repeats; i++) {
                        doSomeThinking();

                        // start writing an new entry
                        excerpt.startExcerpt();
                        excerpt.writeLong(System.nanoTime());
                        excerpt.writeUnsignedShort(consolidates.length);
                        for (final int consolidate : consolidates) {
                            excerpt.writeStopBit(consolidate);
                        }
                        excerpt.finish();
                    }
                    chronicle.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            long last = System.nanoTime();

            private void doSomeThinking() {
                // real programs do some work between messages
                // this has an impact on the worst case latencies.
                while (System.nanoTime() - last < 1e9 / rate) ;
                last = System.nanoTime();
            }
        });
        t.start();

        //Read
        final IndexedChronicle chronicle = new IndexedChronicle(basePath);
//        chronicle.useUnsafe(true); // for benchmarks.
        final ExcerptTailer excerpt = chronicle.createTailer();
        int[] times = new int[repeats];
        for (int count = -warmup; count < repeats; count++) {
            do {
            /* busy wait */
            } while (!excerpt.nextIndex());
            final long timestamp = excerpt.readLong();
            long time = System.nanoTime() - timestamp;
            if (count >= 0)
                times[count] = (int) time;
            final int nbConsolidates = excerpt.readUnsignedShort();
            assert nbConsolidates == consolidates.length;
            for (int i = 0; i < nbConsolidates; i++) {
                excerpt.readStopBit();
            }
            excerpt.finish();
        }
        Arrays.sort(times);
        System.out.printf("After writing %,d excerpts, ", repeats);
        for (double perc : new double[]{50, 90, 99, 99.9, 99.99}) {
            System.out.printf("%s%% took %.2f µs, ", perc, times[((int) (repeats * perc / 100))] / 1000.0);
        }
        System.out.printf("worst took %d µs%n", times[times.length - 1] / 1000);
        chronicle.close();
    }
}
