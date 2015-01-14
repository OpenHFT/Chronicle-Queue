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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StatelessVanillaChronicleAppenderTest extends StatelessChronicleTestBase {

    @Test(expected = UnsupportedOperationException.class)
    public void testVanillaStatelessExceptionOnCreateTailer() throws Exception {
        ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", 9876)
            .build()
            .createTailer();
    }

    @Test(expected = IllegalStateException.class)
    public void testVanillaStatelessExceptionOnCreatAppenderTwice() throws Exception {
        Chronicle ch = ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", 9876)
            .build();

        ch.createAppender();
        ch.createAppender();
    }

    @Test
    public void testVanillaStatelessAppender_001() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();

        final Chronicle remoteAppender = ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", port)
            .appendRequireAck(false)
            .build();

        final int items = 1000000;
        final ExcerptAppender appender = remoteAppender.createAppender();
        final ExcerptTailer tailer = source.createTailer();

        try {
            source.clear();

            for (long i = 1; i <= items; i++) {
                appender.startExcerpt(16);
                appender.writeLong(i);
                appender.writeLong(i + 1);
                appender.finish();
            }

            appender.close();

            int count = 0;
            for (long i = 1; i <= items; i++) {
                while(!tailer.nextIndex());
                count++;

                assertEquals(i    , tailer.readLong());
                assertEquals(i + 1, tailer.readLong());
                tailer.finish();
            }

            tailer.close();

            assertEquals(items, count);
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaStatelessAppender_002() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();

        final Chronicle remoteAppender = ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", port)
            .appendRequireAck(false)
            .build();
        final Chronicle remoteTailer = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        final int items = 1000000;
        final ExcerptAppender appender = remoteAppender.createAppender();
        final ExcerptTailer tailer = remoteTailer.createTailer();

        try {
            source.clear();

            for (long i = 1; i <= items; i++) {
                appender.startExcerpt(16);
                appender.writeLong(i);
                appender.writeLong(i + 1);
                appender.finish();
            }

            appender.close();

            int count = 0;
            for (long i = 1; i <= items; i++) {
                while(!tailer.nextIndex());
                count++;

                assertEquals(i, tailer.readLong());
                assertEquals(i + 1, tailer.readLong());
                tailer.finish();
            }

            tailer.close();
            assertEquals(items, count);
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaStatelessAppender_003() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();

        final Chronicle remoteAppender = ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", port)
            .appendRequireAck(false)
            .build();
        final Chronicle remoteTailer = ChronicleQueueBuilder.remoteTailer()
            .connectAddress("localhost", port)
            .build();

        final int items = 1000000;

        source.clear();

        final Thread appenderTh = new Thread() {
            public void run() {
                try {
                    ExcerptAppender appender = remoteAppender.createAppender();

                    for (long i = 1; i <= items; i++) {
                        appender.startExcerpt(16);
                        appender.writeLong(i);
                        appender.writeLong(i + 1);
                        appender.finish();
                    }

                    appender.close();
                } catch (Exception e) {
                    LOGGER.warn("", e);
                    errorCollector.addError(e);
                } catch (AssertionError ae) {
                    LOGGER.warn("", ae);
                    errorCollector.addError(ae);
                }
            }
        };

        final Thread tailerTh = new Thread() {
            public void run() {
                try {
                    ExcerptTailer tailer = remoteTailer.createTailer();

                    int count = 0;
                    for (long i = 1; i <= items; i++) {
                        while(!tailer.nextIndex());
                        count++;

                        assertEquals(i, tailer.readLong());
                        assertEquals(i + 1, tailer.readLong());
                        tailer.finish();
                    }

                    tailer.close();

                    assertEquals(items, count);
                } catch (Exception e) {
                    LOGGER.warn("", e);
                    errorCollector.addError(e);
                } catch (AssertionError ae) {
                    LOGGER.warn("", ae);
                    errorCollector.addError(ae);
                }
            }
        };

        appenderTh.start();
        tailerTh.start();

        appenderTh.join();
        tailerTh.join();

        source.close();
        source.clear();

        assertFalse(new File(basePathSource).exists());
    }

    @Test
    public void testVanillaStatelessAppender_004() throws Exception {
        final String basePathSource = getVanillaTestPath("-source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = ChronicleQueueBuilder.vanilla(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndCheckPort();

        final Chronicle remoteAppender = ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", port)
            .appendRequireAck(true)
            .build();

        final int items = 1000000;
        final ExcerptAppender appender = remoteAppender.createAppender();
        final ExcerptTailer tailer = source.createTailer();

        try {
            source.clear();

            for (long i = 1; i <= items; i++) {
                appender.startExcerpt(16);
                appender.writeLong(i);
                appender.writeLong(i + 1);
                appender.finish();

                while(!tailer.nextIndex());

                assertEquals(i , tailer.readLong());
                assertEquals(i + 1, tailer.readLong());
                assertEquals(tailer.index() , appender.lastWrittenIndex());

                tailer.finish();
            }

            appender.close();
            tailer.close();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }
}
