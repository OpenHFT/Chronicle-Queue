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

package net.openhft.chronicle.sandbox;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class VanillaChronicleTest {
    private static final int N_THREADS = 4;

    @Test
    public void testAppend() throws IOException {
        final int RUNS = 1000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testAppend";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.defaultMessageSize(128);
        config.indexBlockSize(1024);
        config.dataBlockSize(1024);
        VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();
        ExcerptAppender appender = chronicle.createAppender();
        for (int i = 0; i < RUNS; i++) {
            appender.startExcerpt();
            appender.append(1000000000 + i);
            appender.finish();
        }
        chronicle.close();
        chronicle.clear();
    }

    @Test
    public void testAppend4() throws IOException, InterruptedException {
        final int RUNS = 20000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testAppend4";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.defaultMessageSize(128);
//        config.indexBlockSize(1024);
//        config.dataBlockSize(1024);
        long start = System.nanoTime();
        final VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();
        ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        for (int t = 0; t < N_THREADS; t++) {
            final int finalT = t;
            es.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        ExcerptAppender appender = chronicle.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();
                            appender.append(finalT).append("/").append(i).append('\n');
                            appender.finish();
                            Thread.yield();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        es.shutdown();
        es.awaitTermination(2, TimeUnit.SECONDS);
        long time = System.nanoTime() - start;
        chronicle.close();
        chronicle.clear();
        System.out.printf("Took an average of %.1f us per entry%n", time / 1e3 / (RUNS * N_THREADS));
    }

    @Test
    public void testTailer() throws IOException {
        final int RUNS = 1000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testTailer";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.defaultMessageSize(128);
        config.indexBlockSize(1024);
        config.dataBlockSize(1024);
        VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();
        VanillaChronicle chronicle2 = new VanillaChronicle(baseDir, config);
        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = chronicle2.createTailer();

        assertEquals(-1L, tailer.index());
        for (int i = 0; i < RUNS; i++) {
            assertFalse(tailer.nextIndex());
            appender.startExcerpt();
            int value = 1000000000 + i;
            appender.append(value).append(' ');
            appender.finish();
            assertTrue("i: " + i, tailer.nextIndex());
            assertTrue("i: " + i + " remaining: " + tailer.remaining(), tailer.remaining() > 0);
            assertEquals("i: " + i, value, tailer.parseLong());
            assertEquals("i: " + i, 0, tailer.remaining());
            tailer.finish();
            chronicle.checkCounts();
            chronicle2.checkCounts();
        }
        chronicle2.close();
        chronicle.clear();
    }


    @Test
    public void testTailerPerf() throws IOException {
        final int RUNS = 1000000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testTailerPerf";
        VanillaChronicle chronicle = new VanillaChronicle(baseDir);
        chronicle.clear();
        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = chronicle.createTailer();
        long start = System.nanoTime();
        assertEquals(-1L, tailer.index());
        for (int i = 0; i < RUNS; i++) {
            assertFalse(tailer.nextIndex());
            appender.startExcerpt();
            int value = 1000000000 + i;
            appender.append(value).append(' ');
            appender.finish();
            assertTrue("i: " + i, tailer.nextIndex());
            assertEquals("i: " + i, value, tailer.parseLong(), 0.0);
            tailer.finish();
        }
        long time = System.nanoTime() - start;
        System.out.printf("Average write/read times was %.1f us%n", time / RUNS / 1e3);
        chronicle.close();
        chronicle.clear();
    }
}
