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
        Excerpt r = chronicle.createTailer();
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
        final Excerpt r = chronicle.createTailer();

        final int runs = 100 * 1000 * 1000;
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    IndexedChronicle chronicle = new IndexedChronicle(basePath);
                    final ExcerptAppender w = chronicle.createAppender();
                    for (int i = 0; i < runs; i++) {
                        w.startExcerpt(8);
                        w.writeInt(1 + i);
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

        for (int i = 0; i < runs; i++) {
            do {
            } while (!r.nextIndex());
            try {
                int l = r.readInt();
                if (l != i + 1)
                    throw new AssertionError();
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
        long rate = runs * 1000L / (System.nanoTime() - start);
        System.out.println("Rate = " + rate + " Mmsg/sec");
    }
}
