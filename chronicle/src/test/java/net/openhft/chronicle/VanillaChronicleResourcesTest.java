/*
 * Copyright 2014 Higher Frequency Trading
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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VanillaChronicleResourcesTest extends VanillaChronicleTestBase {

    @Test
    public void testResourcesCleanup1() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = new VanillaChronicle(baseDir);
        chronicle.clear();

        try {
            final ExcerptAppender appender1 = chronicle.createAppender();
            appender1.startExcerpt();
            appender1.writeInt(1);
            appender1.finish();
            chronicle.checkCounts(1, 2);

            final ExcerptAppender appender2 = chronicle.createAppender();
            appender2.startExcerpt();
            appender2.writeInt(2);
            appender2.finish();
            chronicle.checkCounts(1, 2);

            assertTrue(appender1 == appender2);

            appender2.close();

            chronicle.checkCounts(1, 1);

            final ExcerptTailer tailer = chronicle.createTailer();
            assertTrue(tailer.nextIndex());
            chronicle.checkCounts(1, 2);

            tailer.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testResourcesCleanup2() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.dataBlockSize(64);
        config.indexBlockSize(64);

        final VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();

        try {

            final ExcerptAppender appender = chronicle.createAppender();
            for (int counter = 0; counter < 100; counter++) {
                appender.startExcerpt(20);
                appender.writeUTF("data-" + counter);
                appender.finish();
            }

            appender.close();

            chronicle.checkCounts(1,1);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testResourcesCleanup3() throws Exception {
        final int nbThreads = 5;
        final int nbAppend = 10;
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        System.out.println("BaseDir : " + baseDir);
        System.out.println("PID : " + getPID());

        final VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.entriesPerCycle(1L << 20);
        config.cycleLength(1000, false);
        config.cycleFormat("yyyyMMddHHmmss");
        config.indexBlockSize(64);
        config.dataBlockSize(64);
        config.dataCacheCapacity(nbThreads + 1);
        config.indexCacheCapacity(2);

        final VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();

        try {
            final ExecutorService es = Executors.newFixedThreadPool(nbThreads);
            final CountDownLatch latch = new CountDownLatch(nbThreads);

            for(int i=0;i<nbThreads;i++) {
                es.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final ExcerptAppender appender = chronicle.createAppender();
                            for (int counter = 0; counter < nbAppend; counter++) {
                                appender.startExcerpt(4);
                                appender.writeInt(counter);
                                appender.finish();

                                sleep(2000);
                            }

                            appender.close();
                            latch.countDown();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            es.shutdown();
            latch.await();

            chronicle.checkCounts(1, 1);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    // https://github.com/OpenHFT/Java-Chronicle/issues/75
    // https://higherfrequencytrading.atlassian.net/browse/CHRON-47
    @Ignore
    @Test
    public void testResourcesCleanup4() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        System.out.println("BaseDir : " + baseDir);
        System.out.println("PID : " + getPID());

        final int runs = 30 * 5;
        final int nbThreads = Runtime.getRuntime().availableProcessors()*2;
        final byte[] data = new byte[4096];
        Arrays.fill(data, (byte) 'x');

        final VanillaChronicle chronicle = new VanillaChronicle(
            baseDir,
            new VanillaChronicleConfig()
                .cycleFormat("yyyyMMdd/HHmmss")
                .cycleLength(30 * 1000, false) //every 30 seconds
                .defaultMessageSize(data.length)
                .entriesPerCycle(4000000)
                .dataCacheCapacity(nbThreads + 2)
                .dataBlockSize(data.length * 15)
                .indexCacheCapacity(2)
                .indexBlockSize(1024));

        chronicle.clear();

        final ExecutorService es = Executors.newCachedThreadPool();
        for (int i=0; i<nbThreads; i++) {
            es.execute(new ResourcesCleanup4Task(chronicle, runs, data));
        }

        for(int i=0;i<6;i++) {
            Thread.sleep(31 * 1000);
            System.out.println("After " + (31 * (i + 1)) + " seconds");
            lsof(getPIDAsString(), ".* txt .*testResourcesCleanup4.*");
        }

        es.shutdown();
        es.awaitTermination(30, TimeUnit.SECONDS);

        System.out.println("Before close:");
        lsof(getPIDAsString(), ".*testResourcesCleanup4.*");

        chronicle.checkCounts(1, 1);
        chronicle.close();

        System.out.println("After close:");
        lsof(getPIDAsString(), ".*testResourcesCleanup4.*");

        chronicle.clear();
    }

    private final class ResourcesCleanup4Task implements Runnable {
        private final Chronicle chronicle;
        private final byte[] data;
        private final int runs;

        public ResourcesCleanup4Task(final Chronicle chronicle, int runs, final byte[] data) {
            this.runs = runs;
            this.chronicle = chronicle;
            this.data = data;
        }

        public void run() {
            try {
                ExcerptAppender appender = null;

                for (int i=0; i<runs; i++) {
                    appender = chronicle.createAppender();
                    appender.startExcerpt(data.length);
                    appender.write(data);
                    appender.finish();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }

                if(appender != null) {
                    appender.close();
                }
            }
            catch (IOException e) {

            }
        }
    }
}
