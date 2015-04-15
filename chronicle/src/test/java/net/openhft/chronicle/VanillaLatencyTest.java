/*
 * Copyright 2015 Higher Frequency Trading
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

/*
 * Copyright 2015 Higher Frequency Trading
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
package net.openhft.chronicle;

import net.openhft.lang.Jvm;
import org.HdrHistogram.Histogram;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * @author luke
 *         Date: 4/7/15
 */
@Ignore
public class VanillaLatencyTest {

    @Rule
    public final TemporaryFolder tmpdir = new TemporaryFolder(new File(Jvm.TMP));

    final int SAMPLE = 1000000;  //  10000000
    final int COUNT = SAMPLE * 10; // 100000000
    final int ITEM_SIZE = 256;

    @Ignore
    @Test
    public void testIndexedVsVanillaLatency() throws IOException {
        File root = tmpdir.newFolder("IndexedVsVanillaLatency");
        root.deleteOnExit();

        Random random = new Random();

        System.out.println("Allocating...");
        byte[][] byteArrays = new byte[SAMPLE][ITEM_SIZE];

        // Randomize
        System.out.println("Randomize...");
        for (int i=0;i<SAMPLE;i++) {
            random.nextBytes(byteArrays[0]);
        }

        Chronicle indexedChronicle = ChronicleQueueBuilder
            .indexed(root.getAbsolutePath() + "/indexed-latency")
            .build();

        System.out.println("----------------------- INDEXED -----------------------");
        latencyTest(indexedChronicle, COUNT, byteArrays);
        indexedChronicle.close();

        Chronicle vanillaChronicle = ChronicleQueueBuilder
            .vanilla(root.getAbsolutePath() + "/vanilla-latency")
            .build();

        System.out.println("----------------------- VANILLA -----------------------");
        latencyTest(vanillaChronicle, COUNT, byteArrays);
        vanillaChronicle.close();
    }

    private void latencyTest(Chronicle chronicle, long count, final byte[][] byteArrays) throws IOException {
        Histogram histogram = new Histogram(10000000000L, 3);
        ExcerptAppender appender = chronicle.createAppender();

        System.out.println("GC...");
        System.gc();

        // Warmup
        System.out.println("Warmup...");
        for (int i=0; i<SAMPLE; i++) {
            appender.startExcerpt(ITEM_SIZE);
            appender.write(byteArrays[i], 0, ITEM_SIZE);
            appender.finish();
        }

        System.out.println("Test...");
        int index = 0;
        for (int i=0; i<count; i++, index++) {
            long start = System.nanoTime();

            appender.startExcerpt(ITEM_SIZE);
            appender.write(byteArrays[index % SAMPLE], 0, ITEM_SIZE);
            appender.finish();

            long end = System.nanoTime();
            histogram.recordValue(end - start);

            if (i > 0 && i % SAMPLE == 0) {
                System.out.println(
                    "Total: " + i +
                    " Mean: " + histogram.getMean() +
                    " Max: " + histogram.getMaxValue()+
                    " StdDev: " + histogram.getStdDeviation()
                );
            }
        }
    }
}
