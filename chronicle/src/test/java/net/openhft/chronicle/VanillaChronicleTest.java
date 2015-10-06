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

package net.openhft.chronicle;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class VanillaChronicleTest extends VanillaChronicleTestBase {
    private static final int N_THREADS = 4;

    private static void appendValues(final ExcerptAppender appender, final long startValue, final long endValue) {
        for (long counter = startValue; counter < endValue; counter++) {
            appender.startExcerpt(20);
            appender.writeUTF("data-" + counter);
            appender.finish();
        }
    }

    private static Set<String> readAvailableValues(final ExcerptTailer tailer) {
        final Set<String> values = new TreeSet<String>();
        while (tailer.nextIndex()) {
            values.add(tailer.readUTF());
        }
        return values;
    }

    private static Set<String> createRangeDataSet(final long start, final long end) {
        final Set<String> values = new TreeSet<String>();
        long counter = start;
        while (counter < end) {
            values.add("data-" + counter);
            counter++;
        }
        return values;
    }

    private static Callable<Void> createAppendTask(final VanillaChronicle chronicle, final long startValue, final long endValue) {
        return new Callable<Void>() {
            @Override
            public Void call()   {
                ExcerptAppender appender = null;
                try {
                    appender = chronicle.createAppender();
                    appendValues(appender, startValue, endValue);
                } catch(IOException e) {
                } finally {
                    if(appender != null) {
                        appender.close();
                    }
                }
                return null;
            }
        };
    }

    static void testSearchRange(List<Integer> ints, Excerpt excerpt, MyExcerptComparator mec, long[] startEnd) {
        int elo = Collections.binarySearch(ints, mec.lo);
        if (elo < 0) {
            elo = ~elo;
        }

        int ehi = Collections.binarySearch(ints, mec.hi);
        if (ehi < 0) {
            ehi = ~ehi;

        } else {
            ehi++;
        }

        excerpt.findRange(startEnd, mec);

        assertEquals(
            "lo: " + mec.lo + ", hi: " + mec.hi,
            "[" + elo + ", " + ehi + "]",
            Arrays.toString(startEnd));
    }

    static class MyExcerptComparator implements ExcerptComparator {
        int lo, hi;

        @Override
        public int compare(Excerpt excerpt) {
            final int x = excerpt.readInt();
            return x < lo ? -1 : x > hi ? +1 : 0;
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testAppend() throws IOException {
        final int RUNS = 1000;

        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
            .defaultMessageSize(128)
            .indexBlockSize(1024)
            .dataBlockSize(1024)
            .build();

        chronicle.clear();

        try {
            ExcerptAppender appender = chronicle.createAppender();
            for (int i = 0; i < RUNS; i++) {
                appender.startExcerpt();
                appender.append(1000000000 + i);
                appender.finish();

                chronicle.checkCounts(1, 2);
            }

            appender.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    // for 0.5m Throughput was 2940 per milli-second
    // for 100m Throughput was 4364 per milli-second
    @Test
    public void testAppend4() throws IOException, InterruptedException {
        final int RUNS = 500000; // increase to 25 million for a proper test. Can be 50% faster.

        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
            .defaultMessageSize(64)
            .build();

        chronicle.clear();

        try {
            long start = System.nanoTime();

            final ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
            for (int t = 0; t < N_THREADS; t++) {
                final int finalT = t;
                es.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ExcerptAppender appender = chronicle.createAppender();
                            for (int i = 0; i < RUNS; i++) {
                                appender.startExcerpt();

                                appender.appendDateMillis(System.currentTimeMillis())
                                    .append(" - ")
                                    .append(finalT)
                                    .append(" / ")
                                    .append(i)
                                    .append('\n');

                                appender.finish();
                            }
                            appender.close();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            es.shutdown();
            es.awaitTermination(30, TimeUnit.SECONDS);

            long time = System.nanoTime() - start;

            System.out.printf("Throughput was %.0f per milli-second%n", 1e6 * (RUNS * N_THREADS) / time);

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailer() throws IOException {
        final int RUNS = 1000000; // 5000000;

        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        ChronicleQueueBuilder builder = ChronicleQueueBuilder.vanilla(baseDir)
            .defaultMessageSize(128)
            .indexBlockSize(256 << 10)
            .dataBlockSize(512 << 10);

        final VanillaChronicle chronicle1 = (VanillaChronicle)builder.build();

        chronicle1.clear();

        final VanillaChronicle chronicle2 = (VanillaChronicle)builder.build();

        try {
            ExcerptAppender appender = chronicle1.createAppender();
            ExcerptTailer tailer = chronicle2.createTailer();

            assertEquals(-1L, tailer.index());
            for (int i = 0; i < RUNS; i++) {
//                if ((i & 65535) == 0)
//                    System.err.println("i: " + i);
//                if (i == 88000)
//                    Thread.yield();
                assertFalse(tailer.nextIndex());
                appender.startExcerpt();
                int value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                chronicle1.checkCounts(1, 2);
                assertTrue("i: " + i, tailer.nextIndex());
                chronicle2.checkCounts(1, 2);
                assertTrue("i: " + i + " remaining: " + tailer.remaining(), tailer.remaining() > 0);
                assertEquals("i: " + i, value, tailer.parseLong());
                assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();
                chronicle2.checkCounts(1, 2);
            }

            appender.close();
            tailer.close();

            chronicle2.checkCounts(1, 1);
            chronicle1.checkCounts(1, 1);
        } finally {
            chronicle2.close();

            chronicle1.close();
            chronicle1.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailerPerf() throws IOException {
        final int WARMUP = 50000;
        final int RUNS = 5000000;

        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        chronicle.clear();

        try {
            final ExcerptAppender appender = chronicle.createAppender();
            final ExcerptTailer tailer = chronicle.createTailer();
            long start = 0;
            assertEquals(-1L, tailer.index());
            for (int i = -WARMUP; i < RUNS; i++) {
                if (i == 0)
                    start = System.nanoTime();
                boolean condition0 = tailer.nextIndex();
                if (condition0)
                    assertFalse("i: " + i, condition0);
                appender.startExcerpt();
                int value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                boolean condition = tailer.nextIndex();
                long actual = tailer.parseLong();
                if (i < 0) {
                    assertTrue("i: " + i, condition);
                    assertEquals("i: " + i, value, actual);
                }
                tailer.finish();
            }
            long time = System.nanoTime() - start;
            System.out.printf("Average write/read times was %.3f us%n", time / RUNS / 1e3);

            appender.close();
            tailer.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailerPerf2() throws IOException, InterruptedException {
        final int WARMUP = 100000;
        final int RUNS = 4000000;
        final int BYTES = 96;

        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        chronicle.clear();

        try {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    ExcerptTailer tailer = null;
                    for (int i = -WARMUP; i < RUNS; i++) {
                        try {
                            tailer = chronicle.createTailer();

                            // busy waith
                            while (!tailer.nextIndex()) ;

                            long actual = -1;
                            for (int j = 0; j < BYTES; j += 8) {
                                actual = tailer.readLong();
                            }

                            if (i < 0) {
                                int value = 1000000000 + i;
                                assertEquals("i: " + i, value, actual);
                            }

                            tailer.finish();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    tailer.close();
                }
            });
            t.start();
            ExcerptAppender appender = chronicle.createAppender();
            long start = 0;
            for (int i = -WARMUP; i < RUNS; i++) {
                if (i == 0) {
                    start = System.nanoTime();
                }

                appender.startExcerpt();
                int value = 1000000000 + i;
                for (int j = 0; j < BYTES; j += 8) {
                    appender.writeLong(value);
                }

                appender.finish();
            }
            t.join();

            long time = System.nanoTime() - start;
            System.out.printf("Average write/read times was %.3f us%n", time / RUNS / 1e3);

            appender.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Ignore
    @Test(expected = UnsupportedOperationException.class)
    public void testFindRange() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();

        try {
            ExcerptAppender appender = chronicle.createAppender();
            List<Integer> ints = new ArrayList<Integer>();
            for (int i = 0; i < 1000; i += 10) {
                appender.startExcerpt();
                appender.writeInt(i);
                appender.finish();
                ints.add(i);
            }
            appender.close();

            Excerpt excerpt = chronicle.createExcerpt();
            final MyExcerptComparator mec = new MyExcerptComparator();
            // exact matches at a the start

            mec.lo = mec.hi = -1;
            assertEquals(~0, excerpt.findMatch(mec));
            mec.lo = mec.hi = 0;
            assertEquals(0, excerpt.findMatch(mec));
            mec.lo = mec.hi = 9;
            assertEquals(~1, excerpt.findMatch(mec));
            mec.lo = mec.hi = 10;
            assertEquals(1, excerpt.findMatch(mec));

            // exact matches at a the end
            mec.lo = mec.hi = 980;
            assertEquals(98, excerpt.findMatch(mec));
            mec.lo = mec.hi = 981;
            assertEquals(~99, excerpt.findMatch(mec));
            mec.lo = mec.hi = 990;
            assertEquals(99, excerpt.findMatch(mec));
            mec.lo = mec.hi = 1000;
            assertEquals(~100, excerpt.findMatch(mec));

            // range match near the start
            long[] startEnd = new long[2];

            mec.lo = 0;
            mec.hi = 3;
            excerpt.findRange(startEnd, mec);
            assertEquals("[0, 1]", Arrays.toString(startEnd));

            mec.lo = 21;
            mec.hi = 29;
            excerpt.findRange(startEnd, mec);
            assertEquals("[3, 3]", Arrays.toString(startEnd));

            /*
            mec.lo = 129;
            mec.hi = 631;
            testSearchRange(ints, excerpt, mec, startEnd);
    */
            Random rand = new Random(1);
            for (int i = 0; i < 1000; i++) {
                int x = rand.nextInt(1010) - 5;
                int y = rand.nextInt(1010) - 5;
                mec.lo = Math.min(x, y);
                mec.hi = Math.max(x, y);
                testSearchRange(ints, excerpt, mec, startEnd);
            }

            excerpt.close();
            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testAppenderTailer() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle writer = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        writer.clear();

        try {
            final ExcerptAppender appender = writer.createAppender();

            for (long i = 0; i < 3; i++) {
                appender.startExcerpt();
                appender.writeLong(i);
                appender.finish();
            }

            {
                final VanillaChronicle reader = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
                final ExcerptTailer tailer = reader.createTailer();

                for (long i = 0; i < 3; i++) {
                    assertTrue(tailer.nextIndex());
                    assertEquals(i, tailer.readLong());
                    tailer.finish();
                }

                tailer.close();
                reader.close();
            }

            {
                final VanillaChronicle reader = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
                final ExcerptTailer tailer = reader.createTailer().toStart();

                for (long i = 0; i < 3; i++) {
                    assertTrue(tailer.nextIndex());
                    assertEquals(i, tailer.readLong());
                    tailer.finish();
                }

                tailer.close();
                reader.close();
            }

            appender.close();

            writer.checkCounts(1, 1);
        } finally {
            writer.close();
            writer.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailerToStart() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        chronicle.clear();

        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = null;

            for (long i = 0; i < 3; i++) {
                appender.startExcerpt();
                appender.writeLong(i);
                appender.finish();
            }

            // test a vanilla tailer, no rewind
            tailer = chronicle.createTailer();
            for (long i = 0; i < 3; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            // test a vanilla tailer, rewind
            tailer = chronicle.createTailer().toStart();
            for (long i = 0; i < 3; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            appender.close();
            tailer.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailerToEnd1() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        chronicle.clear();

        try {
            ExcerptAppender appender = chronicle.createAppender();
            for (long i = 0; i < 3; i++) {
                appender.startExcerpt();
                appender.writeLong(i);
                appender.finish();
            }

            // test a vanilla tailer, wind to end
            ExcerptTailer tailer = chronicle.createTailer().toEnd();
            assertEquals(2, tailer.readLong());
            assertFalse(tailer.nextIndex());

            appender.close();
            tailer.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailerToEnd2() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle wchronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        wchronicle.clear();

        try {
            ExcerptAppender appender = wchronicle.createAppender();
            for (long i = 0; i < 3; i++) {
                appender.startExcerpt();
                appender.writeLong(i);
                appender.finish();
            }

            // test a vanilla tailer, wind to end
            final VanillaChronicle rchronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
            final ExcerptTailer tailer = rchronicle.createTailer().toEnd();
            assertEquals(2, tailer.readLong());
            assertFalse(tailer.nextIndex());

            appender.close();
            tailer.close();

            rchronicle.checkCounts(1, 1);
            rchronicle.close();

            wchronicle.checkCounts(1, 1);
        } finally {
            wchronicle.close();
            wchronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailerEndStart1() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        chronicle.clear();

        try {
            ExcerptAppender appender = chronicle.createAppender();

            // test a vanilla tailer, wind to end on an empty chronicle
            ExcerptTailer tailer = chronicle.createTailer().toEnd();
            assertFalse(tailer.nextIndex());

            // add some data to the chronicle
            for (long i = 0; i < 3; i++) {
                appender.startExcerpt();
                appender.writeLong(i);
                appender.finish();
            }

            // test that the tailer now can tail
            for (long i = 0; i < 3; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            // test a vanilla tailer, wind to end
            tailer = chronicle.createTailer().toEnd();
            assertEquals(2, tailer.readLong());
            assertFalse(tailer.nextIndex());
            tailer.finish();

            // test a vanilla tailer, rewind
            tailer = chronicle.createTailer().toStart();
            for (long i = 0; i < 3; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            appender.close();
            tailer.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testTailerEndStart2() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle wchronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();
        final VanillaChronicle rchronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir).build();

        wchronicle.clear();

        try {
            ExcerptAppender appender = wchronicle.createAppender();

            // test a vanilla tailer, wind to end on an empty chronicle
            ExcerptTailer tailer = rchronicle.createTailer().toEnd();
            assertFalse(tailer.nextIndex());

            // add some data to the chronicle
            for (long i = 0; i < 3; i++) {
                appender.startExcerpt();
                appender.writeLong(i);
                appender.finish();
            }

            // test that the tailer now can tail
            for (long i = 0; i < 3; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            // test a vanilla tailer, wind to end
            tailer = rchronicle.createTailer().toEnd();
            assertEquals(2, tailer.readLong());
            assertFalse(tailer.nextIndex());
            tailer.finish();

            // test a vanilla tailer, rewind
            tailer = rchronicle.createTailer().toStart();
            for (long i = 0; i < 3; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            appender.close();
            tailer.close();

            rchronicle.checkCounts(1, 1);
            wchronicle.checkCounts(1, 1);
        } finally {
            rchronicle.close();

            wchronicle.close();
            wchronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    /*
    TODO Fix this test as it sometime jumps e.g.

    Major: 1396197284
    Major: 1396197285

    java.lang.AssertionError: major jumped
        Expected :1396197286
        Actual   :1396197287
     */
    @Test
    public void testReplicationWithRollingFilesEverySecond() throws IOException, InterruptedException {
//        TODO int RUNS = 100000;
        final int RUNS = 5 * 1000;

        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final ChronicleQueueBuilder.VanillaChronicleQueueBuilder builder =
            ChronicleQueueBuilder.vanilla(baseDir)
                .entriesPerCycle(1L << 16)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10);

        final VanillaChronicle chronicle = (VanillaChronicle)builder.build();

        chronicle.clear();

        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle.createTailer();
            long lastMajor = 0;
            for (int i = 0; i < RUNS; i++) {
                appender.startExcerpt();
                long value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
//                System.out.println("Sleeping " +i );
                Thread.sleep(1);

                assertTrue(tailer.nextIndex());
                long major = tailer.index() / builder.entriesPerCycle();
                if (lastMajor == 0 || lastMajor == major) {
                    // ok.
                } else if (lastMajor + 1 == major) {
                    System.out.println("Major: " + major);

                } else {
                    assertEquals("major jumped", lastMajor + 1, major);
                }
                lastMajor = major;
//                System.out.printf("Index: %x%n", major);
                assertEquals("i: " + i, value, tailer.parseLong());
                assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();
            }

            appender.close();
            tailer.close();

            chronicle.checkCounts(1,1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testReplicationWithRollingFilesEverySecond2() throws IOException, InterruptedException {
        final int RUNS = 10;

        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
            .entriesPerCycle(1L << 20)
            .cycleLength(1000, false)
            .cycleFormat("yyyyMMddHHmmss")
            .indexBlockSize(16L << 10)
            .build();

        chronicle.clear();

        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle.createTailer();

            for (int i = 0; i < RUNS; i++) {
                appender.startExcerpt();
                int value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                Thread.sleep(2000);
            }

            for (int i = 0; i < RUNS; i++) {
                int value = 1000000000 + i;

                tailer.nextIndex();
                assertEquals("i: " + i, value, tailer.parseLong());
                assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();
            }

            appender.close();
            tailer.close();

            chronicle.checkCounts(1,1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testConcurrentAppend() throws IOException, InterruptedException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
            .dataBlockSize(64)
            .indexBlockSize(64)
            .build();

        chronicle.clear();

        try {
            // Create with small data and index sizes so that the test frequently
            // generates new files

            final int numberOfTasks = 2;
            final int countPerTask = 1000;

            // Create tasks that append to the index
            final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
            int nextValue = countPerTask;
            for (int i = 0; i < numberOfTasks; i++) {
                final int endValue = nextValue + countPerTask;
                tasks.add(createAppendTask(chronicle, nextValue, endValue));
                nextValue = endValue;
            }

            // Execute tasks using a thread per task
            TestTaskExecutionUtil.executeConcurrentTasks(tasks, 30000L);

            // Verify that all values have been written
            final ExcerptTailer tailer = chronicle.createTailer();
            final Set<String> values = readAvailableValues(tailer);
            assertEquals(createRangeDataSet(countPerTask, nextValue), values);

            tailer.close();

            chronicle.checkCounts(1,1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testMultipleCycles() throws IOException, InterruptedException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        // Create with small data and index sizes so that the test frequently generates new files
        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
            .entriesPerCycle(1L << 20)
            .cycleLength(1000, false)
            .cycleFormat("yyyyMMddHHmmss")
            .dataBlockSize(128)
            .indexBlockSize(64)
            .build();

        chronicle.clear();

        try {
            final ExcerptAppender appender = chronicle.createAppender();
            appendValues(appender, 1, 20);

            // Ensure the appender writes in another cycle from the initial writes
            Thread.sleep(2000L);
            appendValues(appender, 20, 40);

            // Verify that all values are read by the tailer
            final ExcerptTailer tailer = chronicle.createTailer();
            assertEquals(createRangeDataSet(1, 40), readAvailableValues(tailer));

            // Verify that the tailer reads no new data from a new cycle
            Thread.sleep(2000L);
            assertTrue(!tailer.nextIndex());

            // ### Throws java.lang.NullPointerException
            // - lastIndexFile is set to null by the previous call to nextIndex
            assertTrue(!tailer.nextIndex());

            // Append data in this new cycle
            appendValues(appender, 41, 60);

            // Verify that the tailer can read the new data
            assertEquals(createRangeDataSet(41, 60), readAvailableValues(tailer));

            appender.close();
            tailer.close();

            chronicle.checkCounts(1,1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testMultipleCycles2() throws IOException, InterruptedException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        // Create with small data and index sizes so that the test frequently generates new file
        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
            .entriesPerCycle(1L << 20)
            .cycleLength(1000, false)
            .cycleFormat("yyyyMMddHHmmss")
            .dataBlockSize(128)
            .indexBlockSize(64)
            .build();

        chronicle.clear();

        try {
            final ExcerptAppender appender = chronicle.createAppender();
            final ExcerptTailer tailer = chronicle.createTailer();

            // Append a small number of events in this cycle
            appendValues(appender, 1, 5);

            // Ensure the appender writes in another cycle from the initial writes
            Thread.sleep(2000L);

            appendValues(appender, 5, 50);

            // ### Fails because it only reads the values written in the first cycle
            assertEquals(createRangeDataSet(1, 50), readAvailableValues(tailer));

            appender.close();
            tailer.close();

            chronicle.checkCounts(1,1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testLastIndex() throws IOException, InterruptedException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        // Create with small index size to ensure multiple index files are created
        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
            .indexBlockSize(64)
            .build();

        chronicle.clear();

        try {
            final ExcerptAppender appender = chronicle.createAppender();

            final long index0 = chronicle.lastIndex();
            assertEquals(-1, index0);

            appendValues(appender, 1, 3);
            final long index1 = chronicle.lastIndex();
            assertTrue(index1 > index0);

            appendValues(appender, 1, 5);
            final long index2 = chronicle.lastIndex();
            assertTrue(index2 > index1);

            // The index file will hold 8 entries, so this call will create a new index file
            appendValues(appender, 1, 2);
            final long index3 = chronicle.lastIndex();
            assertTrue(index3 > index2);

            appendValues(appender, 1, 20);
            final long index4 = chronicle.lastIndex();
            assertTrue(index4 > index3);

            appender.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testGetActiveWorkingDirectory() throws Exception {

        final String baseDir = getTestPath();
        final VanillaChronicle chronicle = (VanillaChronicle)ChronicleQueueBuilder.vanilla(baseDir)
                .cycleLength(1000 * 60 * 60)
                .cycleFormat("yyyyMMddHH")
                .build();

        chronicle.clear();

        try {

            final ExcerptAppender appender = chronicle.createAppender();
            final ExcerptTailer tailer = chronicle.createTailer().toStart();

            // Append a small number of events in this cycle
            appendValues(appender, 1, 500);
            readAvailableValues(tailer);

            //Get current file from tailer and check it exists under base directory
            String file = ((VanillaChronicle.VanillaTailer) tailer).getActiveWorkingDirectory();
            assertTrue(new File(baseDir + "/" + file).exists());

        } finally {

            chronicle.close();
            chronicle.clear();
        }
    }

}
