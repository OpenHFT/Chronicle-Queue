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

package net.openhft.chronicle.tcp;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.Jvm;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static net.openhft.chronicle.ChronicleQueueBuilder.ReplicaChronicleQueueBuilder;
import static net.openhft.chronicle.ChronicleQueueBuilder.vanilla;
import static org.junit.Assert.*;

public class StatefulVanillaChronicleTest extends StatefulChronicleTestBase {

    @Test
    public void testReplication1() throws IOException, InterruptedException {
        final int RUNS = 100;

        final String sourceBasePath = getVanillaTestPath("source");
        final String sinkBasePath = getVanillaTestPath("sink");

        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(sourceBasePath)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = vanilla(sinkBasePath)
                .sink()
                .connectAddress("localhost", port)
                .build();

        try {

            final Thread at = new Thread("th-appender") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();
                            long value = 1000000000 + i;
                            appender.append(value).append(' ');
                            appender.finish();
                        }

                        appender.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptTailer tailer = sink.createTailer();
                        for (int i = 0; i < RUNS; i++) {
                            long value = 1000000000 + i;
                            assertTrue(tailer.nextIndex());
                            long val = tailer.parseLong();

                            assertEquals("i: " + i, value, val);
                            assertEquals("i: " + i, 0, tailer.remaining());
                            tailer.finish();
                        }

                        tailer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sourceBasePath).exists());
            assertFalse(new File(sinkBasePath).exists());
        }
    }

    @Test
    public void testReplicationWithRolling1() throws IOException, InterruptedException {
        final int RUNS = 500;

        final String sourceBasePath = getVanillaTestPath("source");
        final String sinkBasePath = getVanillaTestPath("sink");

        final Chronicle sourceChronicle = vanilla(sourceBasePath)
                .dataBlockSize(1L << 20)
                .entriesPerCycle(1L << 20)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10)
                .build();

        final Chronicle sinkChronicle = vanilla(sinkBasePath)
                .dataBlockSize(1L << 20)
                .entriesPerCycle(1L << 20)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10)
                .build();

        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.source(sourceChronicle)
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = ChronicleQueueBuilder.sink(sinkChronicle)
                .connectAddress("localhost", port)
                .build();

        try {
            final Thread at = new Thread("th-appender") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();

                            long value = 1000000000 + i;
                            appender.append(value).append(' ');
                            appender.finish();

                            sleep(10);
                        }

                        appender.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptTailer tailer = sink.createTailer();
                        for (int i = 0; i < RUNS; i++) {
                            while (!tailer.nextIndex()) ;

                            long value = 1000000000 + i;
                            assertEquals("i: " + i, value, tailer.parseLong());
                            assertEquals("i: " + i, 0, tailer.remaining());

                            tailer.finish();
                        }

                        tailer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sourceBasePath).exists());
            assertFalse(new File(sinkBasePath).exists());
        }
    }

    @Test
    public void testReplicationWithRolling2() throws IOException, InterruptedException {
        final int RUNS = 100;

        final String sourceBasePath = getVanillaTestPath("source");
        final String sinkBasePath = getVanillaTestPath("sink");

        final Chronicle sourceChronicle = vanilla(sourceBasePath)
                .dataBlockSize(1L << 20)
                .entriesPerCycle(1L << 20)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10)
                .build();

        final Chronicle sinkChronicle = vanilla(sinkBasePath)
                .dataBlockSize(1L << 20)
                .entriesPerCycle(1L << 20)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10)
                .build();

        final PortSupplier portSupplier = new PortSupplier();
        final Chronicle source = ChronicleQueueBuilder.source(sourceChronicle)
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = ChronicleQueueBuilder.sink(sinkChronicle)
                .connectAddress("localhost", port)
                .build();

        try {
            final Thread at = new Thread("th-appender") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();

                            long value = 1000000000 + i;
                            appender.append(value).append(' ');
                            appender.finish();

                            sleep(100);
                        }

                        appender.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            final Thread tt = new Thread("th-tailer") {
                public void run() {
                    AffinityLock lock = AffinityLock.acquireLock();
                    try {
                        final ExcerptTailer tailer = sink.createTailer();
                        for (int i = 0; i < RUNS; i++) {
                            while (!tailer.nextIndex()) ;

                            long value = 1000000000 + i;
                            assertEquals("i: " + i, value, tailer.parseLong());
                            assertEquals("i: " + i, 0, tailer.remaining());

                            tailer.finish();
                        }

                        tailer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.release();
                    }
                }
            };

            at.start();
            tt.start();

            at.join();
            tt.join();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();

            assertFalse(new File(sinkBasePath).exists());
            assertFalse(new File(sourceBasePath).exists());
        }
    }

    /**
     * This test tests the following functionality. (1) It ensures that data can be written to a
     * VanillaChronicle Source over a period of 10 seconds whilst the chronicle is rolling files
     * every second. (2) It also ensures that the Sink can be tailed to fetch the items from that
     * Source. (3) Critically it ensures that even though the Sink is stopped and then restarted it
     * resumes from the index at which it stopped.
     */
    //@Ignore // need to investigate why toEnd does not find the righ message
    @Test
    public void testSourceSinkStartResumeRollingEverySecond() throws IOException, InterruptedException {
        //This is the config that is required to make the VanillaChronicle roll every second

        final String sourceBasePath = getVanillaTestPath("source");
        final String sinkBasePath = getVanillaTestPath("sink");
        assertNotNull(sourceBasePath);
        assertNotNull(sinkBasePath);

        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(sourceBasePath)
                .dataBlockSize(1L << 20)
                .entriesPerCycle(1L << 20)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        ExcerptAppender appender = source.createAppender();
        LOGGER.info("writing 100 items will take take 10 seconds.");
        for (int i = 0; i < 100; i++) {
            appender.startExcerpt();
            int value = 1000000000 + i;
            appender.append(value).append(' '); //this space is really important.
            appender.finish();

            Thread.sleep(100);

            if (i % 10 == 0) {
                LOGGER.info(".");
            }
        }

        appender.close();

        final int port = portSupplier.getAndAssertOnError();

        //create a tailer to get the first 50 items then exit the tailer
        final Chronicle sink1 = vanilla(sinkBasePath)
                .dataBlockSize(1L << 20)
                .entriesPerCycle(1L << 20)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10)
                .sink()
                .connectAddress("localhost", port)
                .build();

        final ExcerptTailer tailer1 = sink1.createTailer().toStart();

        LOGGER.info("Sink1 reading first 50 items then stopping");
        for (int count = 0; count < 50; ) {
            if (tailer1.nextIndex()) {
                assertEquals(1000000000 + count, tailer1.parseLong());
                tailer1.finish();

                count++;
            }
        }

        tailer1.close();
        sink1.close();
        ChronicleTools.checkCount(sink1, 1, 1);

        //now resume the tailer to get the first 50 items
        final Chronicle sink2 = vanilla(sinkBasePath)
                .dataBlockSize(1L << 20)
                .entriesPerCycle(1L << 20)
                .cycleLength(1000, false)
                .cycleFormat("yyyyMMddHHmmss")
                .indexBlockSize(16L << 10)
                .sink()
                .connectAddress("localhost", port)
                .build();

        //Take the tailer to the last index (item 50) and start reading from there.
        final ExcerptTailer tailer2 = sink2.createTailer().toEnd();
        assertEquals(1000000000 + 49, tailer2.parseLong());
        tailer2.finish();

        LOGGER.info("Sink2 restarting to continue to read the next 50 items");
        for (int count = 50; count < 100; ) {
            if (tailer2.nextIndex()) {
                assertEquals(1000000000 + count, tailer2.parseLong());
                tailer2.finish();

                count++;
            }
        }

        tailer2.close();

        sink2.close();
        ChronicleTools.checkCount(sink2, 1, 1);
        sink2.clear();

        source.close();
        ChronicleTools.checkCount(source, 1, 1);
        source.clear();

        assertFalse(new File(sourceBasePath).exists());
        assertFalse(new File(sinkBasePath).exists());
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-77
     *
     * @throws java.io.IOException
     */
    @Test
    public void testVanillaJira77() throws IOException {
        String basePath = getVanillaTestPath();

        Chronicle chronicleSrc = vanilla(basePath + "-src").build();
        chronicleSrc.clear();

        Chronicle chronicleTarget = vanilla(basePath + "-target").build();
        chronicleTarget.clear();

        testJira77(
                chronicleSrc,
                chronicleTarget);
    }

    /**
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-80
     *
     * @throws IOException
     */
    @Test
    public void testVanillaJira80() throws IOException {
        String basePath = getVanillaTestPath();

        testJira80(
                vanilla(basePath + "-master"),
                vanilla(basePath + "-slave")
        );
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-104
     */
    @Test
    public void testVanillaClientReconnection() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final String basePathSink = getVanillaTestPath("sink");
        final PortSupplier portSupplier = new PortSupplier();
        final int runs = 50;
        final int itemsPerRun = 15;
        final int totalItems = runs * itemsPerRun;
        final CountDownLatch latch = new CountDownLatch(totalItems);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final Chronicle sink = vanilla(basePathSink)
                            .sink()
                            .connectAddressProvider(new AddressProvider() {
                                @Override
                                public InetSocketAddress get() {
                                    return new InetSocketAddress(
                                            "localhost",
                                            portSupplier.getAndAssertOnError());
                                }
                            })
                            .build();

                    ExcerptTailer tailer = sink.createTailer();
                    while (latch.getCount() > 0) {
                        if (tailer.nextIndex()) {
                            final long actual = tailer.readLong();
                            assertEquals(totalItems - latch.getCount(), actual);
                            tailer.finish();
                            latch.countDown();

                        } else {
                            Thread.sleep(100);
                        }
                    }

                    tailer.clear();
                    sink.close();
                    sink.clear();
                } catch (Exception e) {
                    LOGGER.warn("", e);
                    errorCollector.addError(e);
                }
            }
        });

        t.start();


        for (int i = 0; i < runs; i++) {
            final int expectedLatchCount = totalItems - ((i + 1) * itemsPerRun);
            appendToSource(portSupplier, basePathSource, itemsPerRun, i, new Runnable() {
                @Override
                public void run() {
                    if (expectedLatchCount > 0) {
                        while (latch.getCount() > expectedLatchCount) {
                            try {
                                Thread.sleep(250);
                            } catch (InterruptedException e) {
                                // do nothing
                            }
                        }
                    } else {
                        try {
                            latch.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                    }
                }
            });
        }

        assertEquals(0, latch.getCount());
    }

    private void appendToSource(PortSupplier portSupplier, String basePathSource, int items, int run, Runnable waiter) throws IOException {
        portSupplier.reset();

        // Source 1
        Chronicle source = vanilla(basePathSource)
                .dataBlockSize(1L << 20)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        ExcerptAppender appender = source.createAppender();
        for (long i = 0; i < items; i++) {
            final long l = (run * items) + i;
            appender.startExcerpt(8);
            appender.writeLong(l);
            appender.finish();
        }

        appender.close();

        waiter.run();

        source.close();
//        source.clear();
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testVanillaNonBlockingClient() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final String basePathSink = getVanillaTestPath("sink");
        final PortSupplier portSupplier = new PortSupplier();

        final ChronicleQueueBuilder sourceBuilder = vanilla(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier);

        final Chronicle source = sourceBuilder.build();

        final ReplicaChronicleQueueBuilder sinkBuilder = vanilla(basePathSink)
                .sink()
                .connectAddress("localhost", portSupplier.getAndAssertOnError())
                .readSpinCount(5);

        final Chronicle sink = sinkBuilder.build();

        testNonBlockingClient(
                source,
                sink,
                sinkBuilder.heartbeatIntervalMillis()
        );
    }

    // Tests that a large (size greater than a single write) excerpt can be fully sourced
    @Test
    public void testLargeExcerpt() throws Exception {
        final String basePathSource = getVanillaTestPath("source");
        final String basePathSink = getVanillaTestPath("sink");
        final PortSupplier portSupplier = new PortSupplier();

        final ChronicleQueueBuilder sourceBuilder = vanilla(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .sendBufferSize(16)
                .receiveBufferSize(16);

        final Chronicle source = sourceBuilder.build();

        final ReplicaChronicleQueueBuilder sinkBuilder = vanilla(basePathSink)
                .sink()
                .connectAddress("localhost", portSupplier.getAndAssertOnError())
                .readSpinCount(5)
                .sendBufferSize(16)
                .receiveBufferSize(16);

        final Chronicle sink = sinkBuilder.build();

        testNonBlockingClient(
                source,
                sink,
//                sinkBuilder.heartbeatIntervalMillis()
                500000
        );
    }
}
