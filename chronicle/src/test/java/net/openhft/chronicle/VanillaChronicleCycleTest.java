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

import net.openhft.lang.io.IOTools;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VanillaChronicleCycleTest extends VanillaChronicleTestBase {

    @Test
    public void testsCycles() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        ChronicleQueueBuilder.VanillaChronicleQueueBuilder builder = ChronicleQueueBuilder.vanilla(basePath);

        for(VanillaChronicle.Cycle cycle : VanillaChronicle.Cycle.values()) {
            builder.cycle(cycle);
            Assert.assertEquals(cycle.entries(), builder.entriesPerCycle());
            Assert.assertEquals(cycle.format(), builder.cycleFormat());
            Assert.assertEquals(cycle.length(), builder.cycleLength());
        }
    }

    @Test
    public void testsEntriesPerCyclesCorrection() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        ChronicleQueueBuilder.VanillaChronicleQueueBuilder builder = ChronicleQueueBuilder.vanilla(basePath);

        for(VanillaChronicle.Cycle cycle : VanillaChronicle.Cycle.values()) {
            Assert.assertEquals(
                cycle.entries(),
                builder.cycleLength(cycle.length(), false).entriesPerCycle()
            );
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testsUnknownCycle() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        ChronicleQueueBuilder.vanilla(basePath).cycleLength(1001);
    }

    @Test
    public void testCycleEverySecond() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        final int iterations = 20;
        final CountDownLatch latch = new CountDownLatch(iterations);
        final VanillaChronicle.Cycle cycle = VanillaChronicle.Cycle.SECONDS;
        final ExecutorService svc = Executors.newFixedThreadPool(2);

        svc.execute(createWriter(ChronicleQueueBuilder.vanilla(basePath).cycle(cycle).build(), cycle, iterations));
        svc.execute(createReader(ChronicleQueueBuilder.vanilla(basePath).cycle(cycle).build(), cycle, latch));
        svc.shutdown();
        svc.awaitTermination(1, TimeUnit.MINUTES);

        Assert.assertEquals(0, latch.getCount());

        File[] files = new File(basePath).listFiles();
        Assert.assertNotNull(files);
        Assert.assertTrue(files.length >= 5);
    }

    @Test
    public void testCycleEverySecondWithOneSubdirectory() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        final int iterations = 20;
        final CountDownLatch latch = new CountDownLatch(iterations);
        final VanillaChronicle.Cycle cycle = VanillaChronicle.Cycle.SECONDS;
        final ExecutorService svc = Executors.newFixedThreadPool(2);

        svc.execute(
            createWriter(
                ChronicleQueueBuilder.vanilla(basePath).dataBlockSize(1024).cycle(cycle).cycleFormat("yyyyMMdd" + File.separator + "HHmmss").build(),
                cycle,
                iterations)
        );
        svc.execute(
            createReader(
                ChronicleQueueBuilder.vanilla(basePath).dataBlockSize(1024).cycle(cycle).cycleFormat("yyyyMMdd" + File.separator + "HHmmss").build(),
                cycle,
                latch)
        );

        svc.shutdown();
        svc.awaitTermination(1, TimeUnit.MINUTES);

        Assert.assertEquals(0, latch.getCount());
    }

    @Test
    public void testCycleEverySecondWithTwoSubdirectory() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        final int iterations = 20;
        final CountDownLatch latch = new CountDownLatch(iterations);
        final VanillaChronicle.Cycle cycle = VanillaChronicle.Cycle.SECONDS;
        final ExecutorService svc = Executors.newFixedThreadPool(2);

        svc.execute(
            createWriter(
                ChronicleQueueBuilder.vanilla(basePath).dataBlockSize(1024).cycle(cycle).cycleFormat("yyyyMMdd" + File.separator + "HHmm" + File.separator + "ss").build(),
                cycle,
                iterations)
        );
        svc.execute(
            createReader(
                ChronicleQueueBuilder.vanilla(basePath).dataBlockSize(1024).cycle(cycle).cycleFormat("yyyyMMdd" + File.separator + "HHmm" + File.separator + "ss").build(),
                cycle,
                latch)
        );

        svc.shutdown();
        svc.awaitTermination(1, TimeUnit.MINUTES);

        Assert.assertEquals(0, latch.getCount());
    }

    @Ignore("Test is taking too much time on CI")
    @Test
    public void testCycleEveryMinute() throws Exception {
        final String basePath = getTestPath();
        IOTools.deleteDir(basePath);

        final int iterations = 20;
        final CountDownLatch latch = new CountDownLatch(iterations);
        final VanillaChronicle.Cycle cycle = VanillaChronicle.Cycle.MINUTES;
        final ExecutorService svc = Executors.newFixedThreadPool(2);

        svc.execute(createWriter(ChronicleQueueBuilder.vanilla(basePath).cycle(cycle).build(), cycle, iterations));
        svc.execute(createReader(ChronicleQueueBuilder.vanilla(basePath).cycle(cycle).build(), cycle, latch));
        svc.shutdown();
        svc.awaitTermination(10, TimeUnit.MINUTES);

        Assert.assertEquals(0, latch.getCount());

        File[] files = new File(basePath).listFiles();
        Assert.assertNotNull(files);
        Assert.assertTrue(files.length >= 5);
    }

    // *************************************************************************
    //
    // *************************************************************************

    static Runnable createReader(final Chronicle chron, final VanillaChronicle.Cycle cycle, final CountDownLatch latch) {
        return new Runnable() {
            @Override
            public void run() {
                try(ExcerptTailer tailer = chron.createTailer()) {
                    while(latch.getCount() != 0) {
                        if(tailer.nextIndex()) {
                            tailer.readInt();
                            tailer.finish();
                            latch.countDown();
                        } else {
                            Thread.sleep(cycle.length() / 4);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("", e);
                }
            }
        };
    }

    static Runnable createWriter(final Chronicle chron, final VanillaChronicle.Cycle cycle, final int loops) {
        return new Runnable() {
            @Override
            public void run() {
                try(ExcerptAppender appender = chron.createAppender()) {
                    for (int i = 0; i < loops; i++) {
                        appender.startExcerpt(4);
                        appender.writeInt(i);
                        appender.finish();

                        Thread.sleep(cycle.length() / 4);
                    }
                } catch (Exception e) {
                    LOGGER.warn("", e);
                }
            }
        };
    }
}
