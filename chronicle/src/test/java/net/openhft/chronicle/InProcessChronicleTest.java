/*
 * Copyright 2014 Higher Frequency Trading
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle;

import net.openhft.chronicle.tcp.InProcessChronicleSink;
import net.openhft.chronicle.tcp.InProcessChronicleSource;
import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.StopCharTesters;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author peter.lawrey
 */
public class InProcessChronicleTest extends IndexedChronicleTestBase {

    public static final int PORT = 12345;

    @Test
    public void testOverTCP() throws IOException, InterruptedException {
        final String basePathSource = getTestPath("-source");
        final String basePathSink = getTestPath("-sink");

        // NOTE: the sink and source must have different chronicle files.
        // TODO, make more robust.
        final int messages = 5 * 1000 * 1000;

        final Chronicle source = new InProcessChronicleSource(new IndexedChronicle(basePathSource), PORT + 1);
        final Chronicle sink = new InProcessChronicleSink(new IndexedChronicle(basePathSink), "localhost", PORT + 1);

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
            while (!excerpt.nextIndex())
                count++;
            long n = excerpt.readLong();
            String text = excerpt.parseUTF(StopCharTesters.CONTROL_STOP);
            if (i != n)
                assertEquals('\'' + text + '\'', i, n);
            excerpt.finish();
        }

        sink.close();
        System.out.println("There were " + count + " isSync messages");
        t.join();

        source.close();
        long time = System.nanoTime() - start;
        System.out.printf("Messages per second %,d%n", (int) (messages * 1e9 / time));

        assertClean(basePathSource);
        assertClean(basePathSink);
    }

    @Test
    public void testPricePublishing1() throws IOException, InterruptedException {
        final String basePathSource = getTestPath("-source");
        final String basePathSink = getTestPath("-sink");

        final Chronicle source = new InProcessChronicleSource(new IndexedChronicle(basePathSource), PORT + 2);
        final Chronicle sink = new InProcessChronicleSink(new IndexedChronicle(basePathSink), "localhost", PORT + 2);

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
        while (count.get() < prices)
            reader.read();

        long end = System.nanoTime();
        System.out.printf("Took an average of %.2f us to write and %.2f us to read%n",
                (mid - start) / prices / 1e3, (end - mid) / prices / 1e3);

        source.close();
        sink.close();

        assertClean(basePathSource);
        assertClean(basePathSink);
    }

    @Test
    public void testPricePublishing2() throws IOException, InterruptedException {
        final String basePathSource = getTestPath("-source");
        final String basePathSink = getTestPath("-sink");

        final Chronicle source = new InProcessChronicleSource(new IndexedChronicle(basePathSource), PORT + 3);
        final Chronicle sink = new InProcessChronicleSink(new IndexedChronicle(basePathSink), "localhost", PORT + 3);

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

        assertClean(basePathSource);
        assertClean(basePathSink);
    }

    @Test
    public void testPricePublishing3() throws IOException, InterruptedException {
        final String basePathSource = getTestPath("-source");
        final String basePathSink = getTestPath("-sink");

        final Chronicle source = new InProcessChronicleSource(new IndexedChronicle(basePathSource), PORT + 4);
        final Chronicle sink = new InProcessChronicleSink(new IndexedChronicle(basePathSink), "localhost", PORT + 4);

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

        assertClean(basePathSource);
        assertClean(basePathSink);
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
            if (!excerpt.nextIndex()) return false;
//            System.out.println("ei: "+excerpt.index());
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
/*

    @Test
    @Ignore
    public void testOverTCPRolling() throws IOException, InterruptedException {
        String baseDir = System.getProperty("java.io.tmpdir");
        String srcBasePath = baseDir + "/IPCTR.testOverTCP.source";
        ChronicleTools.deleteDirOnExit(srcBasePath);
        // NOTE: the sink and source must have different chronicle files.
        // TODO, make more robust.
        final int messages = 2 * 1000 * 1000;
        ChronicleConfig config = ChronicleConfig.TEST.clone();
        config.indexFileExcerpts(512);
//        config.dataBlockSize(4096);
//        config.indexBlockSize(4096);
        final Chronicle source = new InProcessChronicleSource(new RollingChronicle(srcBasePath, config), PORT + 1);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
//                    PosixJNAAffinity.INSTANCE.setAffinity(1 << 1);
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

//        PosixJNAAffinity.INSTANCE.setAffinity(1 << 2);
        String snkBasePath = baseDir + "/IPCTR.testOverTCP.sink";
        ChronicleTools.deleteDirOnExit(snkBasePath);
        Chronicle sink = new InProcessChronicleSink(new RollingChronicle(snkBasePath, config), "localhost", PORT + 1);

        long start = System.nanoTime();
        t.start();
        ExcerptTailer excerpt = sink.createTailer();
        int count = 0;
        for (int i = 1; i <= messages; i++) {
            while (!excerpt.nextIndex())
                count++;
            long n = excerpt.readLong();
            String text = excerpt.parseUTF(StopCharTesters.CONTROL_STOP);
            if (i != n)
                assertEquals('\'' + text + '\'', i, n);
            excerpt.finish();
            System.out.println(i);
        }
        sink.close();
        System.out.println("There were " + count + " isSync messages");
        t.join();
        source.close();
        long time = System.nanoTime() - start;
        System.out.printf("Messages per second %,d%n", (int) (messages * 1e9 / time));
    }
*/


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
}
