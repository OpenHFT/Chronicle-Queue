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
package org.slf4j.impl.chronicle;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.sandbox.VanillaChronicle;
import net.openhft.chronicle.slf4j.ChronicleLogger;
import net.openhft.chronicle.slf4j.ChronicleLoggerFactory;
import net.openhft.chronicle.slf4j.ChronicleLoggingHelper;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * TODO: add test case for text-logegrs
 */
public class Slf4jChronicleLoggerTest extends Slf4jChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLoggerFactory() {
        assertEquals(
            StaticLoggerBinder.getSingleton().getLoggerFactory().getClass(),
            ChronicleLoggerFactory.class);
    }

    @Test
    public void testLogger() {
        Logger l1 = LoggerFactory.getLogger(Slf4jChronicleLoggerTest.class);
        Logger l2 = LoggerFactory.getLogger(Slf4jChronicleLoggerTest.class);
        Logger l3 = LoggerFactory.getLogger("Logger1");
        Logger l4 = LoggerFactory.getLogger("readwrite");

        assertNotNull(l1);
        assertEquals(l1.getClass(),ChronicleLogger.class);

        assertNotNull(l2);
        assertEquals(l2.getClass(),ChronicleLogger.class);

        assertNotNull(l3);
        assertEquals(l3.getClass(),ChronicleLogger.class);

        assertNotNull(l4);
        assertEquals(l4.getClass(),ChronicleLogger.class);


        assertEquals(l1,l2);
        assertNotEquals(l1,l3);
        assertNotEquals(l3,l4);
        assertNotEquals(l1,l4);

        ChronicleLogger cl1 = (ChronicleLogger)l1;

        assertEquals(cl1.getLevel(), ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
        assertEquals(cl1.getName(),Slf4jChronicleLoggerTest.class.getName());

        ChronicleLogger cl2 = (ChronicleLogger)l2;
        assertEquals(cl2.getLevel(),ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
        assertEquals(cl2.getName(),Slf4jChronicleLoggerTest.class.getName());

        ChronicleLogger cl3 = (ChronicleLogger)l3;
        assertEquals(cl3.getLevel(),ChronicleLoggingHelper.LOG_LEVEL_INFO);
        assertEquals(cl3.getName(),"Logger1");

        ChronicleLogger cl4 = (ChronicleLogger)l4;
        assertEquals(cl4.getLevel(),ChronicleLoggingHelper.LOG_LEVEL_DEBUG);
        assertEquals(cl4.getName(),"readwrite");
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLogging1() throws IOException {
        Logger l = LoggerFactory.getLogger("readwrite");
        l.trace("trace");
        l.debug("debug");
        l.info("info");
        l.warn("warn");
        l.error("error");

        Thread.currentThread().setName("th-test-logging_1");

        VanillaChronicle reader = new VanillaChronicle(BASEPATH_LOGGER_RW);
        ExcerptTailer tailer = reader.createTailer();

        // debug
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_DEBUG, tailer.readByte());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("debug",tailer.readEnum(String.class));

        // info
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_INFO, tailer.readByte());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("info",tailer.readEnum(String.class));

        // warn
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_WARN, tailer.readByte());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("warn",tailer.readEnum(String.class));

        // error
        assertTrue(tailer.nextIndex());
        tailer.readLong();
        assertEquals(ChronicleLoggingHelper.LOG_LEVEL_ERROR, tailer.readByte());
        assertEquals("th-test-logging_1", tailer.readEnum(String.class));
        assertEquals("readwrite",tailer.readEnum(String.class));
        assertEquals("error",tailer.readEnum(String.class));

        assertFalse(tailer.nextIndex());

        tailer.close();
        reader.close();
    }

    @Ignore
    @Test
    public void testTextLogging1() throws IOException {
        Logger l = LoggerFactory.getLogger("Text1");
        l.trace("trace");
        l.debug("debug");
        l.info("info");
        l.warn("warn");
        l.error("error");
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testLoggingPerf1() throws IOException {
        Logger l = LoggerFactory.getLogger(Slf4jChronicleLoggerTest.class);

        for(int x=0;x<10;x++) {
            long start = System.nanoTime();

            int items = 10000;
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
        Logger l = LoggerFactory.getLogger(Slf4jChronicleLoggerTest.class);

        for(int x=0;x<10;x++) {
            long start = System.nanoTime();

            int items = 10000;
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
        final int RUNS = 20000;
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

    // *************************************************************************
    //
    // *************************************************************************

    /**
     *
     */
    private class RunnableChronicle implements Runnable {
        private final Logger logger;
        private final int runs;

        /**
         *
         * @param runs
         * @param loggerName
         */
        public RunnableChronicle(int runs,String loggerName) {
            this.logger = LoggerFactory.getLogger(loggerName);
            this.runs = runs;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < this.runs; i++) {
                    this.logger.info("runLoop {}",i);
                    Thread.yield();
                }
            } catch (Exception e) {
                this.logger.warn("Exception",e);
            }
        }
    }
}
