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
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static net.openhft.chronicle.ChronicleQueueBuilder.indexed;
import static net.openhft.chronicle.ChronicleQueueBuilder.remoteTailer;
import static net.openhft.chronicle.ChronicleQueueBuilder.vanilla;
import static net.openhft.chronicle.ChronicleQueueBuilder.ReplicaChronicleQueueBuilder;
import static org.junit.Assert.*;

public class StatelessVanillaChronicleTailerTest extends StatelessChronicleTestBase {

    @Test(expected = UnsupportedOperationException.class)
    public void testVanillaStatelessExceptionOnCreateAppender() throws Exception {
        ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", 9876)
            .build()
            .createAppender();
    }

    @Test(expected = IllegalStateException.class)
    public void testVanillaStatelessExceptionOnCreatTailerTwice() throws Exception {
        Chronicle ch = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", 9876)
            .build();

        ch.createTailer();
        ch.createTailer();
    }

    @Test
    public void testVanillaStatelessSink_001() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();
        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
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
                assertEquals(i, tailer1.readLong());
                tailer1.finish();
            }

            assertFalse(tailer1.nextIndex());
            tailer1.close();

            final ExcerptTailer tailer2 = sink.createTailer().toEnd();
            assertEquals(items, tailer2.readLong());
            assertFalse(tailer2.nextIndex());
            tailer2.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaStatelessSink_002() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();
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

            assertEquals(3, tailer.readLong());
            tailer.finish();
            tailer.close();

            appender.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaStatelessSink_004() throws Exception {
        final int tailers = 4;
        final int items = 1000000;

        final String basePathSource = getVanillaTestPath("-source");
        final ExecutorService executor = Executors.newFixedThreadPool(tailers);
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();
        try {
            for(int i=0;i<tailers;i++) {
                executor.submit(new Runnable() {
                    public void run() {
                        try {
                            final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
                                .connectAddress("localhost", port)
                                .build();

                            final ExcerptTailer tailer = sink.createTailer().toStart();
                            for (long i = 1; i <= items; ) {
                                if (tailer.nextIndex()) {
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

            for (int i=1; i<=items; i++) {
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

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaStatelessSink_005() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();
        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        final int items = 1000;
        final ExcerptAppender appender = source.createAppender();
        final ExcerptTailer st = source.createTailer().toStart();
        final ExcerptTailer tailer = sink.createTailer();

        try {
            for (int i = 0; i < items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();

                st.nextIndex();
                st.finish();

                assertTrue(tailer.index(st.index()));
                assertEquals(i, tailer.readLong());
            }

            appender.close();
            tailer.close();
            st.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaStatelessSink_006() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();
        final Chronicle sink = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        final int items = 1000000;
        final ExcerptAppender appender = source.createAppender();
        final ExcerptTailer st = source.createTailer().toStart();

        long startIndex = Long.MIN_VALUE;
        long endIndex = Long.MIN_VALUE;

        try {
            for (int i = 1; i <= items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();

                st.nextIndex();
                st.finish();

                if(i == 1) {
                    startIndex = st.index();
                } else if(i == items) {
                    endIndex = st.index();
                }
            }

            appender.close();
            st.close();

            final ExcerptTailer tailer1 = sink.createTailer().toStart();
            assertEquals(-1,tailer1.index());
            assertTrue(tailer1.nextIndex());
            assertEquals(startIndex, tailer1.index());
            assertEquals(1, tailer1.readLong());
            tailer1.finish();
            tailer1.close();

            final ExcerptTailer tailer2 = sink.createTailer().toEnd();
            assertEquals(endIndex, tailer2.index());
            assertEquals(items, tailer2.readLong());
            tailer2.finish();
            tailer2.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testStatelessVanillaNonBlockingClient() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final ChronicleQueueBuilder sourceBuilder = vanilla(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier);

        final Chronicle source = sourceBuilder.build();

        final ReplicaChronicleQueueBuilder sinkBuilder = remoteTailer()
                .connectAddress("localhost", portSupplier.getAndCheckPort())
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
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-74
     */
    @Test
    public void testVanillaJiraChron74() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron74(portSupplier.getAndCheckPort(), chronicle);
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-75
     */
    @Test
    public void testVanillaJiraChron75() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron75(portSupplier.getAndCheckPort(), chronicle);
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-78
     */
    @Test
    public void testVanillaJiraChron78() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron78(portSupplier.getAndCheckPort(), chronicle);
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-81
     */
    @Test
    public void testVanillaJiraChron81() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle chronicle = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        testJiraChron81(portSupplier.getAndCheckPort(), chronicle);
    }
}
