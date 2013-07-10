package net.openhft.chronicle;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author peter.lawrey
 */
public class IndexedChronicleTest {
    @Test
    public void singleThreaded() throws IOException {
        final String basePath = System.getProperty("java.io.tmpdir") + "/singleThreaded";
        ChronicleTools.deleteOnExit(basePath);

        IndexedChronicle chronicle = new IndexedChronicle(basePath);
        ExcerptAppender w = chronicle.createAppender();
        ExcerptTailer r = chronicle.createTailer();
        OUTER:
        for (int i = 0; i < 50000; i++) {
            // System.out.println(i);

            w.startExcerpt(8);
            w.writeLong(1);
/*
            w.writeDouble(2);
            w.write(3);
*/
            w.finish();

            int count = 100;
            do {
                if (count-- < 0)
                    break OUTER;
            } while (!r.nextIndex());
            long l = r.readLong();
            r.finish();
            assertEquals(1, l);
/*
            double d = r.readDouble();
            assertEquals(2, d, 0.0);
            byte b = r.readByte();
            assertEquals(3, b);
*/
        }
        w.close();
        r.close();
    }

    @Test
    public void multiThreaded() throws IOException {
        final String basePath = System.getProperty("java.io.tmpdir") + "/multiThreaded";
        ChronicleTools.deleteOnExit(basePath);
        IndexedChronicle chronicle = new IndexedChronicle(basePath);
        final ExcerptTailer r = chronicle.createTailer();

        final int runs = 100 * 1000 * 1000; // longs
        final int size = 2; // longs
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    IndexedChronicle chronicle = new IndexedChronicle(basePath);
                    final ExcerptAppender w = chronicle.createAppender();
                    for (int i = 0; i < runs; i += size) {
                        w.startExcerpt(8 * size);
                        for (int s = 0; s < size; s += 2) {
                            w.writeLong(1 + i);
                            w.writeLong(1 + i);
                        }
//                        w.writeDouble(2);
//                        w.writeShort(3);
//                        w.writeByte(4);
                        w.finish();
                    }
                    w.close();
                    chronicle.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();

        for (int i = 0; i < runs; i += size) {
            do {
            } while (!r.nextIndex());
            try {
                for (int s = 0; s < size; s += 2) {
                    long l = r.readLong();
//                    if (l != i + 1)
//                        throw new AssertionError();
                    long l2 = r.readLong();
//                    if (l2 != i + 1)
//                        throw new AssertionError();
                }
//            double d = r.readDouble();
//            short s = r.readShort();
//                byte b = r.readByte();
//                if (b != 4)
//                    throw new AssertionError();
                r.finish();
            } catch (Exception e) {
                System.err.println("i= " + i);
                e.printStackTrace();
                break;
            }
        }
        r.close();
        long rate = runs / size * 10000L / (System.nanoTime() - start);
        System.out.println("Rate = " + rate / 10.0 + " Mmsg/sec");
        chronicle.close();
        System.gc();
    }
}
