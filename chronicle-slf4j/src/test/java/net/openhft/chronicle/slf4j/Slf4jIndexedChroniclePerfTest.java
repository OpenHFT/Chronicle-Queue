/*
 * Copyright 2014 Peter Lawrey
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
package net.openhft.chronicle.slf4j;

import net.openhft.lang.io.IOTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Ignore
public class Slf4jIndexedChroniclePerfTest extends Slf4jChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Before
    public void setUp() {
        System.setProperty(
            "slf4j.chronicle.properties",
            System.getProperty("slf4j.chronicle.indexed.properties")
        );

        getChronicleLoggerFactory().relaod();
        getChronicleLoggerFactory().warmup();
    }

    @After
    public void tearDown() {
        getChronicleLoggerFactory().shutdown();

        IOTools.deleteDir(basePath(ChronicleLoggingConfig.TYPE_INDEXED));
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLoggingPerf1() throws IOException {
        Logger l = LoggerFactory.getLogger(Slf4jVanillaChronicleLoggerTest.class);

        for(int x=0;x<10;x++) {
            long start = System.nanoTime();

            int items = 1000000;
            for (int i = 1; i <= items; i++) {
                l.trace("something to slf4j ({}}",i);
            }

            long end = System.nanoTime();

            System.out.printf("testLoggingPerf1: took an average of %.2f us to write %d items (level disabled)\n",
                (end - start) / items / 1e3,
                items);
        }
    }

    @Test
    public void testLoggingPerf2() throws IOException {
        Logger l = LoggerFactory.getLogger(Slf4jVanillaChronicleLoggerTest.class);

        for(int x=0;x<10;x++) {
            long start = System.nanoTime();

            int items = 1000000;
            for (int i = 1; i <= items; i++) {
                l.warn("something to slf4j ({})",i);
            }

            long end = System.nanoTime();

            System.out.printf("testLoggingPerf2: took an average of %.2f us to write %d items (level enabled)\n",
                (end - start) / items / 1e3,
                items);
        }
    }

    @Test
    public void testLoggingPerf3() throws IOException, InterruptedException {
        final int RUNS = 1000000;
        final int THREADS = 4;

        final long start = System.nanoTime();

        ExecutorService es = Executors.newFixedThreadPool(THREADS);
        for (int t = 0; t < THREADS; t++) {
            es.submit(new RunnableChronicle(RUNS,"thread-" + t));
        }

        es.shutdown();
        es.awaitTermination(2, TimeUnit.SECONDS);

        final long time = System.nanoTime() - start;

        System.out.printf("testLoggingPerf3: took an average of %.1f us per entry\n",
            time / 1e3 / (RUNS * THREADS)
        );
    }
}
