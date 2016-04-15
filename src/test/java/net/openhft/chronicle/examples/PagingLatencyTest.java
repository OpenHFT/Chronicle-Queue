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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

/*
To add a load to the system, run the following in the console if you have a fast drive.

while true; do dd if=/dev/zero of=$RANDOM bs=10240 count=10000; sleep 1; done
 */
public class PagingLatencyTest {
    private static final String TMP = System.getProperty("java.io.tmpdir");
    public static final int WARMUP = 10 * 1000;

    public static void main(String[] args) throws IOException, InterruptedException {
        String path = new File(TMP, "test-paging-latency").getAbsolutePath();

        ChronicleTools.deleteOnExit(path);
        Chronicle chronicle = ChronicleQueueBuilder.indexed(path).build();

        long start = System.currentTimeMillis();
        double max = 0;

        ExcerptAppender e = chronicle.createAppender();
        int ones = 0, tens = 0, hundreds = 0;
        for (int i = 0; i < 2 * 1000 * 1000; ++i) {
            long s = System.nanoTime();
            e.startExcerpt();
            e.writeLong(1);
            long s2 = System.nanoTime();
            e.position(1024 - 4); // 1K bytes total.
            e.finish();
            long s3 = System.nanoTime();
            double t = (s3 - s) / 100000 / 1e1;
            double t2 = (s2 - s) / 100000 / 1e1;
            double t3 = (s3 - s2) / 100000 / 1e1;

            if (i > WARMUP) {
                if (t > max) max = t;
                if (t > 1) {
                    System.err.println(e.index() + " took " + t + " ms. " + t2 + " / " + t3);
                    if (t >= 100) hundreds++;
                    else if (t >= 10) tens++;
                    else if (t >= 1) ones++;
                }
            }

//            while (System.nanoTime() - s3 < 1e5);
            LockSupport.parkNanos(100000); // 0.1 ms.
        }
        System.out.printf("1s: %,d 10s: %,d 100s %,d%n", ones, tens, hundreds);

        System.out.println("Finished in " + (System.currentTimeMillis() - start) + "ms, max=" + max);
        chronicle.close();
    }
}
