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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.ChronicleQueueBuilder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StatelessIndexedChronicleTailerTest extends StatelessChronicleTestBase {

    @Test
    public void testIndexedStatelessSink_001() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", portSupplier.getAndAssertOnError())
            .build();

        final int items = 1000000;
        final ExcerptAppender appender = source.createAppender();

        try {
            for (long i = 1; i <= items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            final ExcerptTailer tailer1 = sink.createTailer().toStart();
            assertEquals(-1,tailer1.index());

            for (long i = 1; i <= items; i++) {
                assertTrue(tailer1.nextIndex());
                assertEquals(i - 1, tailer1.index());
                assertEquals(i, tailer1.readLong());
                tailer1.finish();
            }

            assertFalse(tailer1.nextIndex());
            tailer1.close();

            final ExcerptTailer tailer2 = sink.createTailer().toEnd();
            assertEquals(items - 1, tailer2.index());
            assertEquals(items, tailer2.readLong());
            assertFalse(tailer2.nextIndex());
            tailer2.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testIndexedStatelessSink_002() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        try {
            final ExcerptAppender appender = source.createAppender();
            appender.startExcerpt(8);
            appender.writeLong(1);
            appender.finish();
            appender.startExcerpt(8);
            appender.writeLong(2);
            appender.finish();

            final ExcerptTailer tailer = sink.createTailer().toEnd();
            assertFalse(tailer.nextIndex());

            appender.startExcerpt(8);
            appender.writeLong(3);
            appender.finish();

            while(!tailer.nextIndex());

            assertEquals(2, tailer.index());
            assertEquals(3, tailer.readLong());
            tailer.finish();
            tailer.close();

            appender.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Ignore // Sometimes it fails or stales
    @Test
    public void testIndexedStatelessSink_003() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        try {
            final int items = 1000000;
            final ExcerptAppender appender = source.createAppender();
            final ExcerptTailer tailer = sink.createTailer();
            final Random r = new Random();

            for (long i = 0; i <= items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            for (int i=1; i < 100; i++) {
                int index = r.nextInt(items);

                assertTrue("Index " + index + " not found", tailer.index(index));
                assertEquals(index, tailer.index());
                assertEquals(index, tailer.readLong());

                tailer.finish();
            }

            tailer.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testIndexedStatelessSink_004() throws IOException, InterruptedException {
        final int tailers = 4;
        final int items = 1000000;

        final String basePathSource = getIndexedTestPath("source");
        final ExecutorService executor = Executors.newFixedThreadPool(tailers);

        final PortSupplier portSupplier = new PortSupplier();
        final Chronicle source = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndAssertOnError();

        try {
            for(int i=0;i<tailers;i++) {
                executor.submit(new Runnable() {
                    public void run() {
                        try {
                            final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
                                .connectAddress("localhost", port)
                                .build();

                            final ExcerptTailer tailer = sink.createTailer().toStart();
                            for (long i = 0; i < items; ) {
                                if (tailer.nextIndex()) {
                                    assertEquals(i, tailer.index());
                                    assertEquals(i, tailer.readLong());

                                    tailer.finish();

                                    i++;
                                }
                            }

                            tailer.close();

                            sink.close();
                            sink.clear();
                        } catch (Exception e) {
                            errorCollector.addError(e);
                        } catch (AssertionError e) {
                            errorCollector.addError(e);
                        }
                    }
                });
            }

            Thread.sleep(100);

            final ExcerptAppender appender = source.createAppender();
            for (int i=0; i<items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testIndexedStatelessSink_005() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        final int items = 1000;
        final ExcerptAppender appender = source.createAppender();
        final ExcerptTailer tailer = sink.createTailer();

        try {
            for (int i = 0; i < items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();

                assertTrue(tailer.index(i));
                assertEquals(i, tailer.readLong());
                tailer.finish();
            }

            appender.close();
            tailer.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testIndexedStatelessSink_006() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        final int items = 1000000;
        final ExcerptAppender appender = source.createAppender();

        try {
            for (int i=1; i <= items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            final ExcerptTailer tailer1 = sink.createTailer().toStart();
            assertEquals(-1,tailer1.index());
            assertTrue(tailer1.nextIndex());
            assertEquals(0, tailer1.index());
            assertEquals(1, tailer1.readLong());
            tailer1.finish();
            tailer1.close();

            final ExcerptTailer tailer2 = sink.createTailer().toEnd();
            assertEquals(items - 1, tailer2.index());
            assertEquals(items, tailer2.readLong());
            tailer2.finish();
            tailer2.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testStatelessIndexedNonBlockingClient() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final ChronicleQueueBuilder sourceBuilder = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier);

        final Chronicle source = sourceBuilder.build();

        final ReplicaChronicleQueueBuilder sinkBuilder = remoteTailer()
                .connectAddress("localhost", portSupplier.getAndAssertOnError())
                .readSpinCount(5);

        final Chronicle sinnk = sinkBuilder.build();

        testNonBlockingClient(
                source,
                sinnk,
                sinkBuilder.heartbeatIntervalMillis()
        );
    }

    // *************************************************************************
    // JIRA
    // *************************************************************************

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-103
     */
    @Test
    public void testIndexedRemoteClientReconnection() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();
        final int items = 20;
        final CountDownLatch latch = new CountDownLatch(items);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final Chronicle sink = remoteTailer()
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
                    while(latch.getCount() > 0) {
                        if(tailer.nextIndex()) {
                            if (tailer.remaining() >= 8) {
                                assertEquals(items - latch.getCount() + 1, tailer.readLong());
                            }

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

        // Source 1
        final Chronicle source1 = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        ExcerptAppender appender1 = source1.createAppender();
        for(long i=0; i < items / 2 ; i++) {
            appender1.startExcerpt(8);
            appender1.writeLong(i + 1);
            appender1.finish();
        }

        appender1.close();

        while(latch.getCount() > 10) {
            Thread.sleep(250);
        }

        source1.close();

        portSupplier.reset();

        // Source 2
        final Chronicle source2 = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        ExcerptAppender appender2 = source2.createAppender();
        for(long i=items / 2; i < items; i++) {
            appender2.startExcerpt(8);
            appender2.writeLong(i + 1);
            appender2.finish();
        }

        appender2.close();

        final Chronicle check = vanilla(basePathSource).build();
        final ExcerptTailer checkTailer = check.createTailer();
        for(long i=1; i<items; i++) {
            if(checkTailer.nextIndex()) {
                assertEquals(i, checkTailer.readLong());
                checkTailer.finish();
            }
        }

        checkTailer.close();

        latch.await(5, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());

        source2.close();
        source2.clear();
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-74
     */
    @Test
    public void testIndexedJiraChron74() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron74(portSupplier.getAndAssertOnError(), chronicle);
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-75
     */
    @Test
    public void testIndexedJiraChron75() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron75(portSupplier.getAndAssertOnError(), chronicle);
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-78
     */
    @Test
    public void testIndexedJiraChron78() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron78(portSupplier.getAndAssertOnError(), chronicle);
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-81
     */
    @Test
    public void testIndexedJiraChron81() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.indexed(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron81(portSupplier.getAndAssertOnError(), chronicle);
    }
}
