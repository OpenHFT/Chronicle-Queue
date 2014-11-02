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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.tcp.ChronicleSource;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.StopCharTesters;
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
    protected static final String PREFIX    = "ch-snk2-";
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
        return new ChronicleSource(ChronicleQueueBuilder.indexed(basePath).build(), port);
    }

    //protected Chronicle indexedChronicleSource(String basePath, int port, ChronicleConfig config) throws IOException {
    //    return new ChronicleSource(new IndexedChronicle(basePath, config), port);
    //}

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testIndexedChron2_001() throws Exception {
        final int port = BASE_PORT + 108;
        final String basePathSource = getIndexedTestPath("-source");
        final Chronicle source = indexedChronicleSource(basePathSource, port);

        testIndexedChron2(port, source, basePathSource);
    }

    protected void testIndexedChron2(final int port, final Chronicle source) throws Exception {
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

                            sink = ChronicleQueueBuilder.sink(null)
                                .connectAddress(new InetSocketAddress("localhost", port))
                                .build();

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



    protected void testIndexedChron2(final int port, final Chronicle source, final String path) throws Exception {
        final int items = 100;
        final int clients = 1;
        final int warmup = 5;

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

                            sink = ChronicleQueueBuilder.indexed(path)
                                .sink()
                                    .connectAddress(new InetSocketAddress("localhost", port))
                                    .sharedChronicle(true)
                                .build();

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
                } else {
                    Thread.sleep(250);
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

    @Test
    public void testOverTCP() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("-source");
        final String basePathSink = getIndexedTestPath("-sink");

        // NOTE: the sink and source must have different chronicle files.
        // TODO, make more robust.
        final int messages = 5 * 1000 * 1000;

        TcpConnection cnx = new SinkTcpInitiator(new InetSocketAddress("localhost", 9876));

        final Chronicle source = new ChronicleSource(ChronicleQueueBuilder.indexed(basePathSource).build(), 9876);

        final Chronicle sink = ChronicleQueueBuilder.indexed(basePathSink)
            .sink()
                .connectAddress(new InetSocketAddress("localhost", 9876))
            .build();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ExcerptAppender excerpt = source.createAppender();
                    for (int i = 1; i <= messages; i++) {
                        // use a size which will cause mis-alignment.
                        excerpt.startExcerpt();
                        excerpt.writeLong(i);
                        excerpt.append(' ');
                        excerpt.append(i);
                        excerpt.append('\n');
                        excerpt.finish();
                    }
                    System.out.println(System.currentTimeMillis() + ": Finished writing messages");
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        });

        long start = System.nanoTime();
        t.start();
        ExcerptTailer excerpt = sink.createTailer();
        int count = 0;
        for (int i = 1; i <= messages; i++) {
            while (!excerpt.nextIndex()) {
                count++;
            }

            long n = excerpt.readLong();
            String text = excerpt.parseUTF(StopCharTesters.CONTROL_STOP);
            if (i != n) {
                assertEquals('\'' + text + '\'', i, n);
            }

            excerpt.finish();
        }

        sink.close();
        System.out.println("There were " + count + " InSynk messages");
        t.join();

        source.close();
        long time = System.nanoTime() - start;
        System.out.printf("Messages per second %,d%n", (int) (messages * 1e9 / time));

    }
}
