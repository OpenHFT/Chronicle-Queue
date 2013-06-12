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
        ExcerptWriter w = chronicle.createWriter();
        ExcerptReader r = chronicle.createReader();
        OUTER:
        for (int i = 0; i < 200000; i++) {
            // System.out.println(i);

            w.startExcerpt(17);
            w.writeLong(1);
            w.writeDouble(2);
            w.write(3);
            w.finish();

            int count = 100;
            do {
                if (count-- < 0)
                    break OUTER;
            } while (!r.nextIndex());
            long l = r.readLong();
            assertEquals(1, l);
            double d = r.readDouble();
            assertEquals(2, d, 0.0);
            byte b = r.readByte();
            assertEquals(3, b);
            r.finish();
        }
        w.close();
        r.close();
    }

    @Test
    public void multiThreaded() throws IOException {
        final String basePath = System.getProperty("java.io.tmpdir") + "/multiThreaded";
        ChronicleTools.deleteOnExit(basePath);
        IndexedChronicle chronicle = new IndexedChronicle(basePath);
        final ExcerptReader r = chronicle.createReader();

        final int runs = 50 * 1000 * 1000;
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    IndexedChronicle chronicle = new IndexedChronicle(basePath);
                    final ExcerptWriter w = chronicle.createWriter();
                    for (int i = 0; i < runs; i++) {
                        w.startExcerpt(19);
                        w.writeLong(1);
                        w.writeDouble(2);
                        w.writeShort(3);
                        w.writeByte(4);
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
            long l = r.readLong();
            double d = r.readDouble();
            short s = r.readShort();
            byte b = r.readByte();
            if (b != 4)
                throw new AssertionError();
            r.finish();
        }
        r.close();
        long rate = runs * 1000L / (System.nanoTime() - start);
        System.out.println("Rate = " + rate + " Mmsg/sec");
    }
}
