/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.IOException;
import java.util.Random;

/**
 * @author peter.lawrey
 */
public class ExampleSimpleWriteReadMain {
    public static void main(String... args) throws IOException {
        ChronicleTools.warmup();

        final int warmup = 50 * 1000;
        final int runs = 5 * 1000 * 1000;
        final int rate = 1000 * 1000;
        long start = System.nanoTime();
        final String basePath = System.getProperty("user.home") + "/ExampleSimpleWriteReadMain";
        ChronicleTools.deleteOnExit(basePath);

        final AffinityLock al = AffinityLock.acquireLock();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    al.acquireLock(AffinityStrategies.DIFFERENT_CORE).bind();
                    IndexedChronicle ic = new IndexedChronicle(basePath);
//                    ic.useUnsafe(true); // for benchmarks
                    Random random = new Random();
                    ExcerptAppender excerpt = ic.createAppender();
                    long next = System.nanoTime();
                    for (int i = 1; i <= runs; i++) {
                        double v = random.nextDouble();
                        excerpt.startExcerpt();
                        excerpt.writeUnsignedByte('M'); // message type
                        excerpt.writeLong(next); // write time stamp
                        excerpt.writeLong(0L); // read time stamp
                        excerpt.writeDouble(v);
                        excerpt.finish();
                        next += 1e9 / rate - 30;
                        while (System.nanoTime() < next) ;
                    }
                    ic.close();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }).start();

        IndexedChronicle ic = new IndexedChronicle(basePath);
//        ic.useUnsafe(true); // for benchmarks
        int time1 = 0, time3 = 0, time10 = 0, time30 = 0, time100 = 0;
        ExcerptTailer excerpt = ic.createTailer();
        for (int i = 1; i <= runs; i++) {
            while (!excerpt.nextIndex()) {
                // busy wait
            }
            char ch = (char) excerpt.readUnsignedByte();
            long writeTS = excerpt.readLong();
            excerpt.writeLong(System.nanoTime());
            double d = excerpt.readDouble();
        }

        excerpt = ic.createTailer();
        while (excerpt.nextIndex()) {
            if (excerpt.index() < warmup)
                continue;
            excerpt.readUnsignedByte();
            long writeTS = excerpt.readLong();
            long readTS = excerpt.readLong();
            if (readTS <= 0) throw new AssertionError();
            long time = readTS - writeTS;
            if (time > 1000) {
                if (time > 3000)
                    time3++;
                if (time > 10000)
                    time10++;
                if (time > 30000)
                    time30++;
                if (time > 100000)
                    time100++;
                time1++;
            }
            excerpt.finish();
        }
        ic.close();

        long time = System.nanoTime() - start;
        System.out.printf("Took %.2f seconds to write and read %,d entries%n", time / 1e9, runs);
        System.out.printf("Time 1us: %.3f%%  3us: %.3f%%  10us: %.3f%%  30us: %.3f%%  100us: %.3f%%%n",
                time1 * 100.0 / runs, time3 * 100.0 / runs, time10 * 100.0 / runs, time30 * 100.0 / runs, time100 * 100.0 / runs);
    }
}

/*
On an i7 desktop
Took 17.45 to write and read 30,050,000 entries
Time 1us: 13.05%  3us: 2.88%  10us: 0.13%  30us: 0.004%  100us: 0.004%

On an i5 laptop
Took 6.98 to write and read 30,050,000 entries
Time 2us: 2.06%  10us: 1.45%  100us: 0.47%  1ms: 0.07%  10ms: 0.000%

 */
