/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import org.junit.Before;
import org.junit.Test;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.fail;

/**
 * @author andrew.bissell
 */
public class NewNumberAppendTest {
    static final String TMP = System.getProperty("java.io.tmpdir");

    private static final int NUM_ENTRIES_PER_RECORD = 20;
    private static final int NUM_RECORDS = 50 * 1000;
    private static final int NUM_WARMUP_RECORDS = 10 * 1000;
    private static final int TOTAL_RECORDS = NUM_RECORDS + NUM_WARMUP_RECORDS;
    private static final long[][] RANDOM_LONGS = new long[TOTAL_RECORDS][NUM_ENTRIES_PER_RECORD];
    private static final double[][] RANDOM_DOUBLES = new double[TOTAL_RECORDS][NUM_ENTRIES_PER_RECORD];
    private static final int MAX_PRECISION = 8;

    @Before
    public void fillRandoms() {
        Random random = new Random();
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            for (int j = 0; j < NUM_ENTRIES_PER_RECORD; j++) {
                RANDOM_LONGS[i][j] = random.nextLong();
                RANDOM_DOUBLES[i][j] = random.nextDouble() * 1e10;
            }
            RANDOM_LONGS[i][0] = Long.MIN_VALUE;
            RANDOM_LONGS[i][1] = Long.MAX_VALUE;
            RANDOM_LONGS[i][2] = 0;
            RANDOM_DOUBLES[i][0] = 0;
        }
    }

    static final Class[] NUMBER_TYPES = {long.class, double.class};
    static final Class[] EXCERPT_TYPES = {/*ByteBuffer.class,*/ Unsafe.class};


    @Test
    public void testNumberAppends() throws IOException {
//        for (int i = 0; i < 3; i++) {
        for (Class type : NUMBER_TYPES)
            for (Class excerptType : EXCERPT_TYPES)
                timeAppends(excerptType, type);
//        }
    }

    private void timeAppends(
            Class excerptType,
            Class numType) throws IOException {

        String newPath = TMP + File.separator + excerptType.getSimpleName() + "Ic";
        ChronicleTools.deleteOnExit(newPath);
        IndexedChronicle newIc = new IndexedChronicle(newPath);
//        newIc.useUnsafe(excerptType == Unsafe.class);

        ExcerptAppender excerpt = newIc.createAppender();

        long start = 0;
        for (int i = -NUM_WARMUP_RECORDS; i < TOTAL_RECORDS; i++) {
            if (i == 0)
                start = System.nanoTime();

            int precision = Math.abs(i) % MAX_PRECISION + 1;
            excerpt.startExcerpt(20 * NUM_ENTRIES_PER_RECORD);
            if (numType == long.class) {
                long[] longs = RANDOM_LONGS[Math.abs(i)];
                for (int j = 0; j < NUM_ENTRIES_PER_RECORD; j++) {
                    excerpt.append(longs[j]);
                    excerpt.append(' '); // need something in between.
                }
            } else if (numType == double.class) {
                double[] doubles = RANDOM_DOUBLES[Math.abs(i)];
                for (int j = 0; j < NUM_ENTRIES_PER_RECORD; j++) {
                    excerpt.append(doubles[j], precision);
                    excerpt.append(' '); // need something in between.
                }
            } else {
                fail();
            }
            excerpt.finish();
        }
        newIc.close();

        long time = System.nanoTime() - start;
        System.out.printf("%s %s average time taken %d ns%n", numType, excerptType, time / TOTAL_RECORDS / NUM_ENTRIES_PER_RECORD);
    }
}


