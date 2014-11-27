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


import junit.framework.AssertionFailedError;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.ExtendedSSLSession;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StatelessChronicleTestBase {
    protected static final Logger LOGGER    = LoggerFactory.getLogger("StatelessChronicleTestBase");
    protected static final String TMP_DIR   = System.getProperty("java.io.tmpdir");
    protected static final String PREFIX    = "ch-stateless-";
    protected static final int    BASE_PORT = 12000;

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final ErrorCollector errorCollector = new ErrorCollector();

    protected synchronized String getIndexedTestPath() {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName();
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getIndexedTestPath(String suffix) {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName() + suffix;
        ChronicleTools.deleteOnExit(path);

        return path;
    }

    protected synchronized String getVanillaTestPath() {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName();
        final File f = new File(path);
        if(f.exists()) {
            f.delete();
        }

        return path;
    }

    protected synchronized String getVanillaTestPath(String suffix) {
        final String path = TMP_DIR + "/" + PREFIX + testName.getMethodName() + suffix;
        final File f = new File(path);
        if(f.exists()) {
            f.delete();
        }

        return path;
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected void testJiraChron74(final int port, final Chronicle source) throws Exception {
        Chronicle sink = null;
        ExcerptTailer tailer = null;

        try {
            sink = ChronicleQueueBuilder.statelessSink()
                .connectAddress(new InetSocketAddress("localhost", port))
                .build();

            tailer = sink.createTailer();
            assertFalse(tailer.nextIndex());
            tailer.close();

            sink.close();
            sink.clear();
            sink = null;

            final ExcerptAppender appender = source.createAppender();
            appender.startExcerpt(8);
            appender.writeLong(1);
            appender.finish();
            appender.startExcerpt(8);
            appender.writeLong(2);
            appender.finish();

            sink =ChronicleQueueBuilder.statelessSink()
                .connectAddress("localhost", port)
                .build();

            tailer = sink.createTailer().toStart();
            assertTrue("nextIndex should return true", tailer.nextIndex());
            assertEquals(1L, tailer.readLong());
            tailer.finish();
            assertTrue("nextIndex should return true", tailer.nextIndex());
            assertEquals(2L, tailer.readLong());
            tailer.finish();
            tailer.close();
            tailer = null;

            sink.close();
            sink.clear();
            sink = null;

            sink = ChronicleQueueBuilder.statelessSink()
                .connectAddress("localhost", port)
                .build();

            tailer = sink.createTailer().toEnd();
            assertFalse("nextIndex should return false", tailer.nextIndex());

            sink.close();
            sink.clear();
            sink = null;

            appender.close();
        } finally {
            source.close();
            source.clear();
        }
    }

    protected void testJiraChron75(final int port, final Chronicle source) throws Exception {
        final int items = 1000000;
        final int clients = 4;
        final int warmup = 100;

        final CountDownLatch latch = new CountDownLatch(warmup);
        final ExecutorService executor = Executors.newFixedThreadPool(clients);

        try {
            for(int i=0; i<clients; i++) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        int cnt = 0;
                        Chronicle sink = null;
                        ExcerptTailer tailer = null;

                        try {
                            final long threadId = Thread.currentThread().getId();

                            latch.await();

                            sink = ChronicleQueueBuilder.statelessSink().connectAddress("localhost", port).build();
                            tailer = sink.createTailer();//.toStart();

                            LOGGER.info("Start ChronicleSink on thread {}", threadId);

                            Jira75Quote quote = null;
                            while (cnt < items) {
                                if (tailer.nextIndex()) {
                                    quote = tailer.readObject(Jira75Quote.class);
                                    tailer.finish();
                                    assertEquals(cnt, quote.getQuantity(), 0);
                                    assertEquals(cnt, quote.getPrice(), 0);
                                    assertEquals("instr-" + cnt, quote.getInstrument());
                                    assertEquals('f', quote.getEntryType());

                                    cnt++;
                                }
                            }

                            assertEquals('f', quote.getEntryType());

                            LOGGER.info("Done ({})", threadId);
                        } catch (Exception e) {
                            errorCollector.addError(e);
                        } catch (AssertionError ae) {
                            errorCollector.addError(ae);
                        } finally {
                            try {
                                if(tailer != null) {
                                    tailer.close();
                                }
                                if(sink != null) {
                                    sink.close();
                                }
                            } catch (IOException ioe) {
                                errorCollector.addError(ioe);
                            }
                        }
                    }
                });
            }

            LOGGER.info("Write {} elements to the source", items);
            final ExcerptAppender appender = source.createAppender();
            for(int i=0;i<items;i++) {
                appender.startExcerpt();
                appender.writeObject(new Jira75Quote(i, i, DateTime.now(), "instr-" + i,'f'));
                appender.finish();

                if(latch.getCount() > 0) {
                    latch.countDown();
                }
            }

            appender.close();

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            assertEquals(0, latch.getCount());
        } catch(Exception e) {
            LOGGER.warn("Exception", e);
        } finally {
            source.close();
            source.clear();
        }
    }

    protected void testJiraChron78(final int port, final Chronicle source) throws Exception {
        final Chronicle sink = ChronicleQueueBuilder.statelessSink()
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

            for (long i = 1; i <= items; i++) {
                assertTrue(tailer1.nextIndex());
                assertEquals(i, tailer1.readLong());
            }

            assertFalse(tailer1.nextIndex());
            tailer1.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    protected void testJiraChron81(final int port, final Chronicle source) throws Exception {
        final Chronicle sink = ChronicleQueueBuilder.statelessSink()
            .connectAddress("localhost", port)
            .build();

        try {
            final ExcerptTailer tailer = sink.createTailer().toEnd();
            final ExcerptAppender appender = source.createAppender();

            assertFalse(tailer.nextIndex());

            for(int i=0;i<1000;i++) {
                Thread.sleep(10000);
            }

            appender.startExcerpt(8);
            appender.writeLong(1L);
            appender.finish();

            assertTrue("", tailer.nextIndex());
            assertEquals(1L, tailer.readLong());
            tailer.finish();

            assertFalse(tailer.nextIndex());

            tailer.close();
            appender.close();
        } finally {
            sink.close();
            sink.clear();

            source.close();
            source.clear();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected static final class Jira75Quote implements BytesMarshallable {
        double price;
        double quantity;
        DateTime dateTime;
        String instrument;
        char entryType;

        public Jira75Quote() {
            this.price = 0;
            this.quantity = 0;
            this.dateTime = null;
            this.instrument = "";
            this.entryType = ' ';
        }

        public Jira75Quote(double price, double quantity, DateTime dateTime, String instrument, char entryType) {
            this.price = price;
            this.quantity = quantity;
            this.dateTime = dateTime;
            this.instrument = instrument;
            this.entryType = entryType;
        }

        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }

        public double getQuantity() { return quantity; }
        public void setQuantity(double quantity) { this.quantity = quantity; }

        public DateTime getDateTime() { return dateTime; }
        public void setDateTime(DateTime dateTime) { this.dateTime = dateTime; }

        public String getInstrument() { return instrument; }
        public void setInstrument(String instrument) { this.instrument = instrument; }

        public char getEntryType() { return entryType; }
        public void setEntryType(char entryType) { this.entryType = entryType; }

        public String toString() {
            return "Jira75Quote ["
               + "price=" + price
               + ", quantity=" + quantity
               + ", dateTime=" + dateTime
               + ", instrument=" + instrument
               + ", entryType=" + entryType
               + "]";
        }

        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
            boolean readDateTime = in.readBoolean();
            price = in.readDouble();
            quantity = in.readDouble();
            instrument = in.readUTFΔ();
            entryType = in.readChar();
            dateTime = readDateTime ? new DateTime(new Date(in.readLong())).withZone(DateTimeZone.UTC) : null;
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            boolean writeDateTime = getDateTime() != null;
            out.writeBoolean(writeDateTime);
            out.writeDouble(price);
            out.writeDouble(quantity);
            out.writeUTFΔ(instrument);
            out.writeChar(entryType);
            if(writeDateTime) {
                out.writeLong(dateTime.toDate().getTime());
            }
        }
    }
}
