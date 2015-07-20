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
import net.openhft.lang.io.StopCharTesters;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.ChronicleQueueBuilder.ReplicaChronicleQueueBuilder;
import static net.openhft.chronicle.ChronicleQueueBuilder.indexed;
import static org.junit.Assert.assertEquals;

/**
 * @author peter.lawrey
 */
public class StatefulIndexedChronicleTest extends StatefulChronicleTestBase {

    @Test
    public void testOverTCP() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final String basePathSink = getIndexedTestPath("sink");

        // NOTE: the sink and source must have different chronicle files.
        // TODO, make more robust.
        final int messages = 5 * 1000 * 1000;

        final PortSupplier portSupplier = new PortSupplier();
        final Chronicle source = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = indexed(basePathSink)
                .sink()
                .connectAddress("localhost", port)
                .build();

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ExcerptAppender excerpt = source.createAppender();
                    for (int i = 1; i <= messages; i++) {
                        // use a size which will cause mis-alignment.
                        excerpt.startExcerpt();
                        excerpt.writeLong(i);
                        excerpt.append(' ');
                        excerpt.append(i);
                        excerpt.append('\n');
                        excerpt.finish();
                    }
                    excerpt.close();
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

        t.join();
        long time = System.nanoTime() - start;

        System.out.println("There were " + count + " InSynk messages");
        System.out.printf("Messages per second %,d%n", (int) (messages * 1e9 / time));

        sink.close();
        source.close();

        assertIndexedClean(basePathSource);
        assertIndexedClean(basePathSink);
    }

    @Test
    public void testPricePublishing1() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final String basePathSink = getIndexedTestPath("sink");

        final PortSupplier portSupplier = new PortSupplier();
        final Chronicle source = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = indexed(basePathSink)
                .sink()
                .connectAddress("localhost", port)
                .build();

        final PriceWriter pw = new PriceWriter(source.createAppender());
        final AtomicInteger count = new AtomicInteger();
        final PriceReader reader = new PriceReader(sink.createTailer(), new PriceListener() {
            @Override
            public void onPrice(long timeInMicros, String symbol, double bp, int bq, double ap, int aq) {
                count.incrementAndGet();
            }
        });

        pw.onPrice(1, "symbol", 99.9, 1, 100.1, 2);
        reader.read();

        long start = System.nanoTime();
        int prices = 12000000;
        for (int i = 1; i <= prices; i++) {
            pw.onPrice(i, "symbol", 99.9, i, 100.1, i + 1);
        }

        long mid = System.nanoTime();
        while (count.get() < prices) {
            reader.read();
        }

        long end = System.nanoTime();
        System.out.printf("Took an average of %.2f us to write and %.2f us to read%n",
                (mid - start) / prices / 1e3, (end - mid) / prices / 1e3);

        source.close();
        sink.close();

        assertIndexedClean(basePathSource);
        assertIndexedClean(basePathSink);
    }

    @Test
    public void testPricePublishing2() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final String basePathSink = getIndexedTestPath("sink");

        final PortSupplier portSupplier = new PortSupplier();
        final Chronicle source = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = indexed(basePathSink)
                .sink()
                .connectAddress("localhost", port)
                .build();

        final PriceWriter pw = new PriceWriter(source.createAppender());
        final AtomicInteger count = new AtomicInteger();
        final PriceReader reader = new PriceReader(sink.createTailer(), new PriceListener() {
            @Override
            public void onPrice(long timeInMicros, String symbol, double bp, int bq, double ap, int aq) {
                count.incrementAndGet();
            }
        });

        pw.onPrice(1, "symbol", 99.9, 1, 100.1, 2);
        assertEquals(-1, reader.excerpt.index());
        reader.read();
        assertEquals(0, reader.excerpt.index());

        long start = System.nanoTime();
        int prices = 2 * 1000 * 1000;
        for (int i = 1; i <= prices; i++) {
            pw.onPrice(i, "symbol", 99.9, i, 100.1, i + 1);
        }

        long mid = System.nanoTime();
        while (count.get() < prices) {
            reader.read();
        }

        long end = System.nanoTime();
        System.out.printf("Took an average of %.2f us to write and %.2f us to read using Excerpt%n",
                (mid - start) / prices / 1e3, (end - mid) / prices / 1e3);

        source.close();
        sink.close();

        assertIndexedClean(basePathSource);
        assertIndexedClean(basePathSink);
    }

    @Test
    public void testPricePublishing3() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final String basePathSink = getIndexedTestPath("sink");

        final PortSupplier portSupplier = new PortSupplier();
        final Chronicle source = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        final int port = portSupplier.getAndAssertOnError();
        final Chronicle sink = indexed(basePathSink)
                .sink()
                .connectAddress("localhost", port)
                .build();

        final PriceWriter pw = new PriceWriter(source.createAppender());
        final AtomicInteger count = new AtomicInteger();
        PriceReader reader = new PriceReader(sink.createTailer(), new PriceListener() {
            @Override
            public void onPrice(long timeInMicros, String symbol, double bp, int bq, double ap, int aq) {
                count.incrementAndGet();
            }
        });

        pw.onPrice(1, "symbol", 99.9, 1, 100.1, 2);
        assertEquals(-1, reader.excerpt.index());
        reader.read();
        assertEquals(0, reader.excerpt.index());

        long start = System.nanoTime();
        int prices = 2 * 1000 * 1000;
        for (int i = 1; i <= prices; i++) {
            pw.onPrice(i, "symbol", 99.9, i, 100.1, i + 1);
        }

        long mid = System.nanoTime();
        while (count.get() < prices)
            reader.read();

        long end = System.nanoTime();
        System.out.printf("Took an average of %.2f us to write and %.2f us to read using Tailer%n",
                (mid - start) / prices / 1e3, (end - mid) / prices / 1e3);

        source.close();
        sink.close();

        assertIndexedClean(basePathSource);
        assertIndexedClean(basePathSink);
    }

    // Took an average of 2.8 us to write and 7.6 us to read (Java 7)
    @Test
    public void testSerializationPerformance() throws IOException, ClassNotFoundException {
        List<byte[]> bytes = new ArrayList<byte[]>();
        long start = System.nanoTime();
        int prices = 200 * 1000;
        for (int i = 0; i < prices; i++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            PriceUpdate pu = new PriceUpdate(1 + i, "symbol", 99.9, i + 1, 100.1, i + 2);
            oos.writeObject(pu);
            oos.close();
            bytes.add(baos.toByteArray());
        }

        long mid = System.nanoTime();
        for (byte[] bs : bytes) {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bs));
            PriceUpdate pu = (PriceUpdate) ois.readObject();
        }

        long end = System.nanoTime();
        System.out.printf("Took an average of %.1f us to write and %.1f us to read%n",
                (mid - start) / prices / 1e3, (end - mid) / prices / 1e3);
    }

    // Took an average of 0.42 us to write and 0.61 us to read (Java 6)
    // Took an average of 0.35 us to write and 0.59 us to read (Java 7)

    // *************************************************************************
    //
    // *************************************************************************

    interface PriceListener {
        void onPrice(long timeInMicros, String symbol, double bp, int bq, double ap, int aq);
    }

    static class PriceWriter implements PriceListener {
        private final ExcerptAppender excerpt;

        PriceWriter(ExcerptAppender excerpt) {
            this.excerpt = excerpt;
        }

        @Override
        public void onPrice(long timeInMicros, @NotNull String symbol, double bp, int bq, double ap, int aq) {
            excerpt.startExcerpt();
            excerpt.writeByte('P'); // code for a price
            excerpt.writeLong(timeInMicros);
            excerpt.writeEnum(symbol);
            excerpt.writeDouble(bp);
            excerpt.writeInt(bq);
            excerpt.writeDouble(ap);
            excerpt.writeInt(aq);
            excerpt.finish();
        }
    }

    static class PriceReader {
        private final ExcerptTailer excerpt;
        private final PriceListener listener;

        PriceReader(ExcerptTailer excerpt, PriceListener listener) {
            this.excerpt = excerpt;
            this.listener = listener;
        }

        public boolean read() {
            if (!excerpt.nextIndex()) {
                return false;
            }

            char ch = (char) excerpt.readByte();
            switch (ch) {
                case 'P': {
                    long timeInMicros = excerpt.readLong();
                    String symbol = excerpt.readEnum(String.class);
                    double bp = excerpt.readDouble();
                    int bq = excerpt.readInt();
                    double ap = excerpt.readDouble();
                    int aq = excerpt.readInt();
                    listener.onPrice(timeInMicros, symbol, bp, bq, ap, aq);
                    break;
                }

                default:
                    throw new AssertionError("Unexpected code " + ch);
            }
            return true;
        }
    }

    static class PriceUpdate implements Externalizable, Serializable {
        private long timeInMicros;
        private String symbol;
        private double bp;
        private int bq;
        private double ap;
        private int aq;

        public PriceUpdate() {
        }

        PriceUpdate(long timeInMicros, String symbol, double bp, int bq, double ap, int aq) {
            this.timeInMicros = timeInMicros;
            this.symbol = symbol;
            this.bp = bp;
            this.bq = bq;
            this.ap = ap;
            this.aq = aq;
        }

        //        @Override
        public void writeExternal(@NotNull ObjectOutput out) throws IOException {
            out.writeLong(timeInMicros);
            out.writeUTF(symbol);
            out.writeDouble(bp);
            out.writeInt(bq);
            out.writeDouble(ap);
            out.writeInt(aq);
        }

        //        @Override
        public void readExternal(@NotNull ObjectInput in) throws IOException, ClassNotFoundException {
            timeInMicros = in.readLong();
            symbol = in.readUTF();
            bp = in.readDouble();
            bq = in.readInt();
            ap = in.readDouble();
            aq = in.readInt();
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    /**
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-77
     *
     * @throws IOException
     */
    @Test
    public void testIndexedJira77() throws IOException {
        Chronicle chronicleSrc = indexed(getIndexedTestPath("source")).build();
        chronicleSrc.clear();

        Chronicle chronicleTarget = indexed(getIndexedTestPath("target")).build();
        chronicleTarget.clear();

        testJira77(
                chronicleSrc,
                chronicleTarget
        );
    }

    /**
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-80
     *
     * @throws IOException
     */
    @Test
    public void testIndexedJira80() throws IOException {
        testJira80(
                indexed(getIndexedTestPath("master")),
                indexed(getIndexedTestPath("slave"))
        );
    }

    /*
     * https://higherfrequencytrading.atlassian.net/browse/CHRON-104
     */
    @Test
    public void testIndexedClientReconnection() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final String basePathSink = getIndexedTestPath("sink");
        final PortSupplier portSupplier = new PortSupplier();
        final int items = 20;
        final CountDownLatch latch = new CountDownLatch(items);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final Chronicle sink = indexed(basePathSink)
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
                            long actual = tailer.readLong();
                            assertEquals(items - latch.getCount(), actual);
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
        Chronicle source1 = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        ExcerptAppender appender1 = source1.createAppender();
        for (long i = 0; i < items / 2; i++) {
            appender1.startExcerpt(8);
            appender1.writeLong(i);
            appender1.finish();
        }

        appender1.close();

        while (latch.getCount() > 10) {
            Thread.sleep(250);
        }

        source1.close();

        portSupplier.reset();

        // Source 2
        Chronicle source2 = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .build();

        ExcerptAppender appender2 = source2.createAppender();
        for (long i = items / 2; i < items; i++) {
            appender2.startExcerpt(8);
            appender2.writeLong(i);
            appender2.finish();
        }

        appender2.close();

        latch.await(500, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());

/*
        source2.close();
        source2.clear();
*/
    }

    // *************************************************************************
    //
    // *************************************************************************

    static final int RATE = Integer.getInteger("rate", 100000);
    static final int COUNT = Integer.getInteger("count", RATE * 2);
    static final int WARMUP = Integer.getInteger("warmup", RATE);

    @Test
    public void testReplicationLatencyPerf() throws IOException, InterruptedException {

        final Chronicle source = ChronicleQueueBuilder
                .indexed(getIndexedTestPath("latencysource"))
                .source()
                .bindAddress(54321)
                .build();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                AffinityLock lock = AffinityLock.acquireLock();
                try {
                    ExcerptAppender appender = source.createAppender();
                    long spacing = 1000000000 / RATE;
                    long now = System.nanoTime();
                    for (int i = -WARMUP; i < COUNT; i++) {
                        while (now > System.nanoTime()) {
                            // busy waiting.
                        }
                        appender.startExcerpt();
                        appender.writeLong(now);
                        appender.finish();
                        now += spacing;
                    }
                    appender.startExcerpt();
                    appender.writeLong(-1);
                    appender.finish();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    lock.release();
                }
            }
        });
        t.start();

        Chronicle sink = ChronicleQueueBuilder
                .indexed(getIndexedTestPath("latencysink"))
                .sink()
                .connectAddress("localhost", 54321)
                .build();

        AffinityLock lock = AffinityLock.acquireLock();
        try {
            ExcerptTailer tailer = sink.createTailer();
            long[] times = new long[COUNT];
            int count = -WARMUP;
            while (true) {
                if (tailer.nextIndex()) {
                    long timestamp = tailer.readLong();
                    if (timestamp < 0)
                        break;
                    if (++count > 0 && count < times.length) {
                        times[count] = System.nanoTime() - timestamp;
                    }
//                if ((count & 1023) == 0)
//                    System.out.println(count);
                    tailer.finish();
                }
            }
            assertEquals(COUNT, count);
            Arrays.sort(times);
            System.out.printf("Latencies 50 90/99 99.9/99.99 %%tile %,d %,d/%,d %,d/%,d us%n",
                    times[times.length - times.length / 2] / 1000,
                    times[times.length - times.length / 10] / 1000,
                    times[times.length - times.length / 100] / 1000,
                    times[times.length - times.length / 1000 - 1] / 1000,
                    times[times.length - times.length / 10000 - 1] / 1000
            );
        } finally {
            lock.release();
        }
        t.join(10000);

        source.close();
        sink.close();

        assertIndexedClean(source.name());
        assertIndexedClean(sink.name());
    }

    @Test
    public void testIndexedNonBlockingClient() throws IOException, InterruptedException {
        final String basePathSource = getIndexedTestPath("source");
        final String basePathSink = getIndexedTestPath("sink");
        final PortSupplier portSupplier = new PortSupplier();

        final ChronicleQueueBuilder sourceBuilder = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier);

        final Chronicle source = sourceBuilder.build();

        final ReplicaChronicleQueueBuilder sinkBuilder = indexed(basePathSink)
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

    @Test
    public void testIndexedLargeExcerpt() throws Exception {
        final String basePathSource = getIndexedTestPath("source");
        final String basePathSink = getIndexedTestPath("sink");
        final PortSupplier portSupplier = new PortSupplier();

        final ChronicleQueueBuilder sourceBuilder = indexed(basePathSource)
                .source()
                .bindAddress(0)
                .connectionListener(portSupplier)
                .sendBufferSize(16)
                .receiveBufferSize(16);

        final Chronicle source = sourceBuilder.build();

        final ReplicaChronicleQueueBuilder sinkBuilder = indexed(basePathSink)
                .sink()
                .connectAddress("localhost", portSupplier.getAndAssertOnError())
                .readSpinCount(5)
                .sendBufferSize(16)
                .receiveBufferSize(16);

        final Chronicle sinnk = sinkBuilder.build();

        testNonBlockingClient(
                source,
                sinnk,
                sinkBuilder.heartbeatIntervalMillis()
        );
    }
}
