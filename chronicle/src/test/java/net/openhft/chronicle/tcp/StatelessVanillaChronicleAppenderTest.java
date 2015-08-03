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
import java.io.IOException;

import static net.openhft.chronicle.ChronicleQueueBuilder.vanilla;
import static net.openhft.chronicle.ChronicleQueueBuilder.remoteAppender;
import static net.openhft.chronicle.ChronicleQueueBuilder.remoteTailer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StatelessVanillaChronicleAppenderTest extends StatelessChronicleTestBase {

    @Test(expected = UnsupportedOperationException.class)
    public void testVanillaStatelessExceptionOnCreateTailer() throws IOException, InterruptedException {
        ChronicleQueueBuilder.remoteAppender()
            .connectAddress("localhost", 9876)
            .build()
            .createTailer();
    }

    @Test(expected = IllegalStateException.class)
    public void testVanillaStatelessExceptionOnCreatAppenderTwice() throws IOException, InterruptedException {
        Chronicle ch = remoteAppender()
            .connectAddress("localhost", 9876)
            .build();

        ch.createAppender();
        ch.createAppender();
    }

    @Test
    public void testVanillaStatelessAppender() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final Chronicle remoteAppender = remoteAppender()
            .connectAddress("localhost", portSupplier.getAndAssertOnError())
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

//            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaStatelessAppenderResizeWriteBuffer() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final Chronicle remoteAppender = remoteAppender()
                .connectAddress("localhost", portSupplier.getAndAssertOnError())
                .appendRequireAck(false)
                .minBufferSize(4)
                .build();

        final int items = 10;
        final ExcerptAppender appender = remoteAppender.createAppender();
        final ExcerptTailer tailer = source.createTailer();

        try {
            source.clear();

            for (long i = 1; i <= items; i++) {
                appender.startExcerpt(i * 8);
                for(int x=0;x<i;x++) {
                    appender.writeLong(x + 1);
                }

                appender.finish();
            }

            appender.close();

            int count = 0;
            for (long i = 1; i <= items; i++) {
                while(!tailer.nextIndex());
                count++;

                for(int x=0;x<i;x++) {
                    assertEquals(x + 1, tailer.readLong());
                }

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
    public void testVanillaStatelessAppenderAndTailer() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndAssertOnError();

        final Chronicle remoteAppender = remoteAppender()
            .connectAddress("localhost", port)
            .appendRequireAck(false)
            .build();
        final Chronicle remoteTailer = remoteTailer()
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
    public void testVanillaStatelessAppenderAndTailerMT() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final int port = portSupplier.getAndAssertOnError();

        final Chronicle remoteAppender = remoteAppender()
            .connectAddress("localhost", port)
            .appendRequireAck(false)
            .build();
        final Chronicle remoteTailer = remoteTailer()
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
    public void testVanillaStatelessAppenderIndices() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(basePathSource)
            .source()
            .bindAddress(0)
            .connectionListener(portSupplier)
            .build();

        final Chronicle remoteAppender = remoteAppender()
            .connectAddress("localhost", portSupplier.getAndAssertOnError())
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

//            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test( expected = IllegalStateException.class)
    public void testVanillaStatelessAppenderExceptionOnDisconnect() throws IOException, InterruptedException {
        final String basePathSource = getVanillaTestPath("source");
        final PortSupplier portSupplier = new PortSupplier();

        final Chronicle source = vanilla(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final Chronicle remoteAppender = remoteAppender()
                .connectAddress("localhost",  portSupplier.getAndAssertOnError())
                .appendRequireAck(true)
                .build();

        final int items = 100;
        final ExcerptAppender appender = remoteAppender.createAppender();

        try {
            source.clear();

            for (long i = 1; i <= items; i++) {
                appender.startExcerpt(16);
                appender.writeLong(i);
                appender.writeLong(i + 1);
                appender.finish();

                if(i == 10) {
                    source.close();
                }
            }
        } finally {
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }
}
