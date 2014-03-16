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

package net.openhft.chronicle;

//import vanilla.java.processingengine.affinity.PosixJNAAffinity;

import net.openhft.chronicle.tools.ChronicleIndexReader;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.StopCharTesters;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author peter.lawrey
 */
public class IndexedChronicleTest {
    static {
        ChronicleTools.warmup();
    }

    public static final String TMP = System.getProperty("java.io.tmpdir");
    private static final long WARMUP = 20000;

    static void validateExcerpt(@NotNull ExcerptCommon r, int i, int expected) {
        if (expected > r.remaining() || 8 * expected < r.remaining())
            assertEquals("index: " + r.index(), expected, r.remaining());
        if (expected > r.capacity() || 8 * expected < r.capacity())
            assertEquals("index: " + r.index(), expected, r.capacity());
        assertEquals(0, r.position());
        long l = r.readLong();
        assertEquals(i, l);
        assertEquals(8, r.position());
        if (expected - 8 != r.remaining())
            assertEquals("index: " + r.index(), expected - 8, r.remaining());
        double d = r.readDouble();
        assertEquals(i, d, 0.0);


        if (0 != r.remaining())
            assertEquals("index: " + r.index(), 0, r.remaining());

        r.position(0);
        long l2 = r.readLong();
        assertEquals(i, l2);

        r.position(expected);

        r.finish();
    }

    static void testSearchRange(List<Integer> ints, Excerpt excerpt, MyExcerptComparator mec, long[] startEnd) {
        int elo = Collections.binarySearch(ints, mec.lo);
        if (elo < 0) elo = ~elo;
        int ehi = Collections.binarySearch(ints, mec.hi);
        if (ehi < 0)
            ehi = ~ehi;
        else ehi++;
        excerpt.findRange(startEnd, mec);
        assertEquals("lo: " + mec.lo + ", hi: " + mec.hi,
                "[" + elo + ", " + ehi + "]",
                Arrays.toString(startEnd));
    }

    @Test
    @Ignore
    public void testWasPadding() throws IOException {
        final String basePath = TMP + "/singleThreaded";
        ChronicleTools.deleteOnExit(basePath);

        ChronicleConfig config = ChronicleConfig.TEST.clone();
        config.dataBlockSize(128);
        config.indexBlockSize(128);
        IndexedChronicle chronicle1 = new IndexedChronicle(basePath, config);
        ExcerptAppender appender = chronicle1.createAppender();

        IndexedChronicle chronicle2 = new IndexedChronicle(basePath, config);
        ExcerptTailer tailer = chronicle2.createTailer();

        assertEquals(-1, tailer.index());
        assertTrue(tailer.wasPadding());
        assertFalse(tailer.index(-1));
        assertTrue(tailer.wasPadding());

        appender.startExcerpt(48);
        appender.position(48);
        appender.finish();

        assertTrue(tailer.nextIndex());
        assertFalse(tailer.wasPadding());
        assertEquals(0, tailer.index());
        assertTrue(tailer.index(0));
        assertFalse(tailer.wasPadding());

        // rewind it to the start - issue # 12
        assertFalse(tailer.index(-1));
        assertEquals(-1, tailer.index());
        assertTrue(tailer.nextIndex());
        assertFalse(tailer.wasPadding());
        assertEquals(0, tailer.index());
        // end of issue # 12;

        assertFalse(tailer.nextIndex());
        assertFalse(tailer.wasPadding());
        assertEquals(0, tailer.index());
        assertFalse(tailer.index(1));
        assertFalse(tailer.wasPadding());

        appender.startExcerpt(48);
        appender.position(48);
        appender.finish();

        assertTrue(tailer.nextIndex());
        assertFalse(tailer.wasPadding());
        assertEquals(2, tailer.index());
        assertTrue(tailer.index(1));
        assertFalse(tailer.wasPadding());
        assertEquals(1, tailer.index());

        assertFalse(tailer.nextIndex());
        assertFalse(tailer.wasPadding());
        assertEquals(1, tailer.index());
        assertFalse(tailer.index(2));
        assertFalse(tailer.wasPadding());
        assertEquals(2, tailer.index());

        // doesn't fit.
        appender.startExcerpt(48);
        appender.position(48);
        appender.finish();

        assertFalse(tailer.index(2));
        assertTrue(tailer.wasPadding());
        assertEquals(2, tailer.index());

        assertTrue(tailer.index(1));

        assertTrue(tailer.nextIndex());
        assertFalse(tailer.wasPadding());
        assertEquals(3, tailer.index());

        assertFalse(tailer.index(2));
        assertTrue(tailer.wasPadding());
        assertEquals(2, tailer.index());

        assertTrue(tailer.index(3));
        assertFalse(tailer.wasPadding());
        assertEquals(3, tailer.index());

        assertFalse(tailer.index(4));
        assertFalse(tailer.wasPadding());
        assertEquals(4, tailer.index());

        chronicle1.close();
        chronicle2.close();
    }

    @Test
    public void singleThreaded() throws IOException {
        final String basePath = TMP + "/singleThreaded";
        ChronicleTools.deleteOnExit(basePath);

        ChronicleConfig config = ChronicleConfig.TEST.clone();
        // TODO fix for 4096 !!!
        int dataBlockSize = 4 * 1024;
        config.dataBlockSize(dataBlockSize);
        config.indexBlockSize(128 * 1024);
        IndexedChronicle chronicle = new IndexedChronicle(basePath, config);
        int i = 0;
        try {
            ExcerptAppender w = chronicle.createAppender();
            ExcerptTailer r = chronicle.createTailer();
            Excerpt e = chronicle.createExcerpt();
            Random rand = new Random(1);
            // finish just at the end of the first page.
            int idx = 0;
            for (i = 0; i < 50000; i++) {
//                System.out.println(i + " " + idx);
//                if (i == 28)
//                    ChronicleIndexReader.main(basePath + ".index");

                assertFalse("i: " + i, r.nextIndex());

                assertFalse("i: " + i, e.index(idx));
                int capacity = 16 * (1 + rand.nextInt(7));

                w.startExcerpt(capacity);
                assertEquals(0, w.position());
                w.writeLong(i);
                assertEquals(8, w.position());
                w.writeDouble(i);

                int expected = 16;
                assertEquals(expected, w.position());
                assertEquals(capacity - expected, w.remaining());

                w.finish();

//                ChronicleIndexReader.main(basePath + ".index");

//                if (i >= 5542)
//                    ChronicleIndexReader.main(basePath + ".index");

                if (!r.nextIndex()) {
                    assertTrue(r.nextIndex());
                }
                validateExcerpt(r, i, expected);

                if (!e.index(idx++)) {
                    assertTrue(e.wasPadding());
                    assertTrue(e.index(idx++));
                }
                validateExcerpt(e, i, expected);

            }
            w.close();
            r.close();
        } finally {
            chronicle.close();
            System.out.println("i: " + i);
//            ChronicleIndexReader.main(basePath + ".index");
//            ChronicleTools.deleteOnExit(basePath);
        }
    }

    @Test
    public void multiThreaded() throws IOException, InterruptedException {
//        for (int i = 0; i < 20; i++) System.out.println();
        if (Runtime.getRuntime().availableProcessors() < 2) {
            System.err.println("Test requires 2 CPUs, skipping");
            return;
        }

        final String basePath = TMP + "/multiThreaded";
        ChronicleTools.deleteOnExit(basePath);
        final ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
        int dataBlockSize = 1 << 26;
        config.dataBlockSize(dataBlockSize);
        config.indexBlockSize(dataBlockSize / 4);
        IndexedChronicle chronicle = new IndexedChronicle(basePath, config);
        final ExcerptTailer r = chronicle.createTailer();

        // shorten the test for a build server.
        final long words = 50L * 1000 * 1000;
        final int size = 4;
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    IndexedChronicle chronicle = new IndexedChronicle(basePath, config);
                    final ExcerptAppender w = chronicle.createAppender();
                    for (int i = 0; i < words; i += size) {
                        w.startExcerpt();
                        for (int s = 0; s < size; s++)
                            w.writeInt(1 + i);
//                        w.position(4L * size);
                        w.finish();
//                        System.out.println(i);
                    }
                    w.close();

//                    chronicle.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();

        long maxDelay = 0, maxJitter = 0;
        for (long i = 0; i < words; i += size) {
            if (!r.nextIndex()) {
                long start0 = System.nanoTime();
                long last = start0;
                while (!r.nextIndex()) {
                    long now = System.nanoTime();
                    long jitter = now - last;
                    if (i > WARMUP && maxJitter < jitter)
                        maxJitter = jitter;
                    long delay0 = now - start0;
                    if (delay0 > 100e6)
                        throw new AssertionError("delay: " + delay0 / 1000000 + ", index: " + r.index());
                    if (i > WARMUP && maxDelay < delay0)
                        maxDelay = delay0;
                    last = now;
                }
            }
            try {
                for (int s = 0; s < size; s++) {
                    int j = r.readInt();
                    if (j != i + 1) {
                        ChronicleIndexReader.main(basePath + ".index");
                        throw new AssertionError(j + " != " + (i + 1));
                    }
                }
                r.finish();
            } catch (Exception e) {
                System.err.println("i= " + i);
                e.printStackTrace();
                break;
            }
        }

        r.close();
        long rate = words / size * 10 * 1000L / (System.nanoTime() - start);
        System.out.println("Rate = " + rate / 10.0 + " Mmsg/sec for " + size * 4 + " byte messages, " +
                "maxJitter: " + maxJitter / 1000 + " us, " +
                "maxDelay: " + maxDelay / 1000 + " us," + "");
//                "totalWait: " + (PrefetchingMappedFileCache.totalWait.longValue() + SingleMappedFileCache.totalWait.longValue()) / 1000 + " us");
        Thread.sleep(200);
        ChronicleTools.deleteOnExit(basePath);
    }

    @Test
    @Ignore
    public void multiThreaded2() throws IOException, InterruptedException {
        if (Runtime.getRuntime().availableProcessors() < 3) {
            System.err.println("Test requires 3 CPUs, skipping");
            return;
        }
        final String basePath = TMP + "/multiThreaded";
        final String basePath2 = TMP + "/multiThreaded2";
        ChronicleTools.deleteOnExit(basePath);
        ChronicleTools.deleteOnExit(basePath2);

        final ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
//        config.dataBlockSize(4*1024);
//        config.indexBlockSize(4 * 1024);

        final int runs = 100 * 1000 * 1000;
        final int size = 4;
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    IndexedChronicle chronicle = new IndexedChronicle(basePath, config);
                    final ExcerptAppender w = chronicle.createAppender();
                    for (int i = 0; i < runs; i += size) {
                        w.startExcerpt();
                        for (int s = 0; s < size; s++)
                            w.writeInt(1 + i);
                        w.finish();
                    }
                    w.close();
//                    chronicle.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, "t1");
        t.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    IndexedChronicle chronicle = new IndexedChronicle(basePath, config);
                    final ExcerptTailer r = chronicle.createTailer();
                    IndexedChronicle chronicle2 = null;
                    try {
                        chronicle2 = new IndexedChronicle(basePath2, config);
                    } catch (FileNotFoundException e) {
                        System.in.read();
                    }
                    final ExcerptAppender w = chronicle2.createAppender();
                    for (int i = 0; i < runs; i += size) {
                        do {
                        } while (!r.nextIndex());

                        w.startExcerpt();
                        for (int s = 0; s < size; s++)
                            w.writeInt(r.readInt());
                        r.finish();
                        w.finish();
                    }
                    w.close();
//                    chronicle.close();
//                    chronicle2.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, "t2");
        t2.start();

        IndexedChronicle chronicle = new IndexedChronicle(basePath2, config);
        final ExcerptTailer r = chronicle.createTailer();

        for (int i = 0; i < runs; i += size) {
            do {
            } while (!r.nextIndex());
            try {
                for (int s = 0; s < size; s++) {
                    long l = r.readInt();
                    if (l != i + 1)
                        throw new AssertionError();
                }
                r.finish();
            } catch (Exception e) {
                System.err.println("i= " + i);
                e.printStackTrace();
                break;
            }
        }
        r.close();
        long rate = 2 * runs / size * 10000L / (System.nanoTime() - start);
        System.out.println("Rate = " + rate / 10.0 + " Mmsg/sec");
        chronicle.close();
        Thread.sleep(200);
        ChronicleTools.deleteOnExit(basePath);
        ChronicleTools.deleteOnExit(basePath2);
    }

    @Test
    public void testOneAtATime() throws IOException {
        ChronicleConfig config = ChronicleConfig.TEST.clone();
        config.indexBlockSize(128); // very small
        config.dataBlockSize(128);  // very small
        final String basePath = TMP + "/testOneAtATime";
        ChronicleTools.deleteOnExit(basePath);

        File indexFile = new File(basePath + ".index");

        for (int i = 0; i < 1000; i++) {
            if (i % 10 == 0)
                System.out.println("i: " + i);

            long indexFileSize = indexFile.length();
            IndexedChronicle chronicle = new IndexedChronicle(basePath, config);
            assertEquals("Index should not grow on open (i=" + i + ")", indexFileSize, indexFile.length());

            if (i == 0) {
                ExcerptTailer tailer = chronicle.createTailer();
                assertFalse(tailer.nextIndex());
                Excerpt excerpt = chronicle.createExcerpt();
                assertFalse(excerpt.index(0));
            }

            ExcerptAppender appender = chronicle.createAppender();
            appender.startExcerpt();
            appender.writeDouble(i);
            appender.finish();
//            ChronicleIndexReader.main(basePath+".index");

            ExcerptTailer tailer = chronicle.createTailer();
            long[] indexes = new long[i + 1];
            long lastIndex = -1;
            for (int j = 0; j <= i; j++) {
                assertTrue(tailer.nextIndex());
                assertTrue(tailer.index() + " > " + lastIndex, tailer.index() > lastIndex);
                lastIndex = tailer.index();
                double d = tailer.readDouble();
                assertEquals(j, d, 0.0);
                assertEquals(0, tailer.remaining());
                indexes[j] = tailer.index();
                tailer.finish();
            }
            assertFalse(tailer.nextIndex());
            Excerpt excerpt = chronicle.createExcerpt();
            // forward
            for (int j = 0; j < i; j++) {
                assertTrue(excerpt.index(indexes[j]));
                double d = excerpt.readDouble();
                assertEquals(j, d, 0.0);
                assertEquals(0, excerpt.remaining());
                excerpt.finish();
            }
            assertFalse(excerpt.index(indexes[indexes.length - 1] + 1));
            // backward
            for (int j = i - 1; j >= 0; j--) {
                assertTrue(excerpt.index(indexes[j]));
                double d = excerpt.readDouble();
                assertEquals(j, d, 0.0);
                assertEquals(0, excerpt.remaining());
                excerpt.finish();
            }
            assertFalse(excerpt.index(-1));
            chronicle.close();
        }
    }

    @Test
    public void testFindRange() throws IOException {
        final String basePath = TMP + "/testFindRange";
        ChronicleTools.deleteOnExit(basePath);

        IndexedChronicle chronicle = new IndexedChronicle(basePath);
        ExcerptAppender appender = chronicle.createAppender();
        List<Integer> ints = new ArrayList<Integer>();
        for (int i = 0; i < 1000; i += 10) {
            appender.startExcerpt();
            appender.writeInt(i);
            appender.finish();
            ints.add(i);
        }
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

        chronicle.close();

    }

    @Test
    public void testParseLines() throws IOException {
        final String basePath = TMP + "/testParseLines";
        ChronicleTools.deleteOnExit(basePath);

        IndexedChronicle chronicle = new IndexedChronicle(basePath);
        ExcerptAppender appender = chronicle.createAppender();

        int runs = 10000;
        for (int i = 0; i < runs; i++) {
            appender.startExcerpt();
            appender.append("Hello world ").append(i).append("\n");
            appender.finish();
        }

        ExcerptTailer tailer = chronicle.createTailer();
        for (int i = 0; i < runs; i++) {
            assertTrue(tailer.nextIndex());
            String s = tailer.parseUTF(StopCharTesters.CONTROL_STOP);
//            System.out.println(s);
            assertEquals("Hello world " + i, s);
            tailer.finish();
        }
        chronicle.close();
    }

    static class MyExcerptComparator implements ExcerptComparator {

        int lo, hi;

        @Override
        public int compare(Excerpt excerpt) {
            final int x = excerpt.readInt();
            return x < lo ? -1 : x > hi ? +1 : 0;
        }

    }

}
