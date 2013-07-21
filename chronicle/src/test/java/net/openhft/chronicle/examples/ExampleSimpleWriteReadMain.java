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

import net.openhft.chronicle.*;
import net.openhft.lang.affinity.PosixJNAAffinity;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * @author peter.lawrey
 */
public class ExampleSimpleWriteReadMain {
    public static void main(String... args) throws IOException {
        final int runs = 30050000;
        long start = System.nanoTime();
        final String basePath = System.getProperty("user.home") + "/ExampleSimpleWriteReadMain";
        ChronicleTools.deleteOnExit(basePath);

        if (IndexedChronicleTest.WITH_BINDING)
            PosixJNAAffinity.INSTANCE.setAffinity(1L << 5);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if (IndexedChronicleTest.WITH_BINDING)
                        PosixJNAAffinity.INSTANCE.setAffinity(1L << 2);
                    IndexedChronicle ic = new IndexedChronicle(basePath);
//                    ic.useUnsafe(true); // for benchmarks
                    ExcerptAppender excerpt = ic.createAppender();
                    Random random = new SecureRandom();
                    for (int i = 1; i <= runs; i++) {
                        excerpt.startExcerpt(17);
                        excerpt.writeUnsignedByte('M'); // message type
                        excerpt.writeLong(System.nanoTime()); // e.g. time stamp
                        excerpt.writeDouble(random.nextInt());
                        excerpt.finish();
                    }
                    ic.close();
                    System.nanoTime();
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
            do {
                // busy wait
            } while (!excerpt.nextIndex());
            char ch = (char) excerpt.readUnsignedByte();
            long l = excerpt.readLong();
            long time = System.nanoTime() - l;
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
            if (i == 50000)
                time1 = time3 = time10 = time30 = time100 = 0;
            double d = excerpt.readDouble();
//            System.out.println(l);
//            assert ch == 'M';
//            assert l == i;
//            assert d == i;
            excerpt.finish();
        }
        ic.close();

        long time = System.nanoTime() - start;
        System.out.printf("Took %.2f to write and read %,d entries%n", time / 1e9, runs);
        System.out.printf("Time 1us: %.2f%%  3us: %.2f%%  10us: %.2f%%  30us: %.3f%%  100us: %.3f%%%n",
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
