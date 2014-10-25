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
package net.openhft.chronicle.tcp2;

import net.openhft.chronicle.*;
import net.openhft.chronicle.tcp.ChronicleSource;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ChronicleSink2Test {
    protected static final Logger LOGGER    = LoggerFactory.getLogger("ChronicleSink2Test");
    protected static final String TMP_DIR   = System.getProperty("java.io.tmpdir");
    protected static final String PREFIX    = "ch-synk2-";
    protected static final int    BASE_PORT = 13000;

    @Rule
    public final TestName testName = new TestName();

    // *************************************************************************
    //
    // *************************************************************************

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

    protected Chronicle indexedChronicleSource(String basePath, int port) throws IOException {
        return new ChronicleSource(new IndexedChronicle(basePath), port);
    }

    protected Chronicle indexedChronicleSource(String basePath, int port, ChronicleConfig config) throws IOException {
        return new ChronicleSource(new IndexedChronicle(basePath, config), port);
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testIndexedJiraChron75() throws Exception {
        final int port = BASE_PORT + 108;
        final String basePathSource = getIndexedTestPath("-source");
        final Chronicle source = indexedChronicleSource(basePathSource, port);

        testJiraChron75(port, source);
    }

    protected void testJiraChron75(final int port, final Chronicle source) throws Exception {
        final int items = 1000000;
        final int clients = 1;
        final int warmup = 100;

        final ExecutorService executor = Executors.newFixedThreadPool(clients);
        final CountDownLatch latch = new CountDownLatch(warmup);

        try {
            for(int i=0;i<clients;i++) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        int cnt = 0;
                        ExcerptTailer tailer = null;
                        Chronicle sink = null;

                        try {
                            final long threadId = Thread.currentThread().getId();

                            TcpConnection cnx = new SinkTcpConnectionInitiator(
                                "s-" + threadId,
                                new InetSocketAddress("localhost", port)
                            );

                            sink = new ChronicleSink2(null, cnx);
                            tailer = sink.createTailer().toStart();

                            latch.await();

                            LOGGER.info("Start ChronicleSink on thread {}", threadId);
                            for(cnt=0; cnt<items;) {
                                if(tailer.nextIndex()) {
                                    Jira75Quote quote = tailer.readObject(Jira75Quote.class);
                                    tailer.finish();

                                    assertEquals(cnt, quote.getQuantity(), 0);
                                    assertEquals(cnt, quote.getPrice(), 0);
                                    assertEquals("instr-" + cnt, quote.getInstrument());

                                    cnt++;
                                }
                            }

                            assertEquals(items, cnt);

                            tailer.close();
                            sink.close();
                        } catch(Exception e) {
                            LOGGER.warn("Exception {}", cnt, e);
                        }
                    }
                });
            }

            LOGGER.info("Write {} elements to the source", items);
            final ExcerptAppender appender = source.createAppender();
            for(int i=0;i<items;i++) {
                appender.startExcerpt(1000);
                appender.writeObject(new Jira75Quote(i, i, "instr-" + i));
                appender.finish();

                if(i < warmup) {
                    latch.countDown();
                }
            }

            appender.close();

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch(Exception e) {
            LOGGER.warn("Exception", e);
        } finally {
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
        String instrument;

        public Jira75Quote() {
            this.price = 0;
            this.quantity = 0;
            this.instrument = "";
        }

        public Jira75Quote(double price, double quantity, String instrument) {
            this.price = price;
            this.quantity = quantity;
            this.instrument = instrument;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public double getQuantity() {
            return quantity;
        }

        public void setQuantity(double quantity) {
            this.quantity = quantity;
        }

        public String getInstrument() {
            return instrument;
        }

        public void setInstrument(String instrument) {
            this.instrument = instrument;
        }

        public String toString() {
            return "Jira75Quote ["
                + "price=" + price
                + ", quantity=" + quantity
                + ", instrument=" + instrument
                + "]";
        }

        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
            price = in.readDouble();
            quantity = in.readDouble();
            instrument = in.readUTFΔ();
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
            out.writeDouble(price);
            out.writeDouble(quantity);
            out.writeUTFΔ(instrument);
        }
    }
}
