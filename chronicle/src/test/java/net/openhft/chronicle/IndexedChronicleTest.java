/*
 * Copyright 2013 Peter Lawrey
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

//import net.openhft.lang.affinity.PosixJNAAffinity;

import net.openhft.lang.affinity.PosixJNAAffinity;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author peter.lawrey
 */
public class IndexedChronicleTest {
    static final boolean WITH_BINDING;

    static {
        boolean binding = false;

        if (Runtime.getRuntime().availableProcessors() > 10) {
            try {
                PosixJNAAffinity.INSTANCE.getcpu();
                binding = true;
                System.out.println("binding: true");
            } catch (Throwable ignored) {
            }
        }
        WITH_BINDING = binding;
    }

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
    public void multiThreaded() throws IOException, InterruptedException {
        final String basePath = System.getProperty("java.io.tmpdir") + "/multiThreaded";
        ChronicleTools.deleteOnExit(basePath);
        IndexedChronicle chronicle = new IndexedChronicle(basePath);
        final ExcerptTailer r = chronicle.createTailer();

        final long words = 2000L * 1000 * 1000;
        final int size = 4;
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if (WITH_BINDING)
                        PosixJNAAffinity.INSTANCE.setAffinity(1L << 5);
                    IndexedChronicle chronicle = new IndexedChronicle(basePath);
                    final ExcerptAppender w = chronicle.createAppender();
                    for (int i = 0; i < words; i += size) {
                        w.startExcerpt(4L * size);
                        for (int s = 0; s < size; s++)
                            w.writeInt(1 + i);
//                        w.position(4L * size);
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

        if (WITH_BINDING)
            PosixJNAAffinity.INSTANCE.setAffinity(1L << 2);

        for (long i = 0; i < words; i += size) {
            int counter = 0;
            while (!r.nextIndex()) {
                if (counter++ > 1000000000)
                    throw new AssertionError("index: " + r.index());
            }
            try {
//                for (int s = 0; s < size; s++) {
//                    int j = r.readInt();
//                    if (j != i + 1)
//                        throw new AssertionError();
//                }
                r.finish();
            } catch (Exception e) {
                System.err.println("i= " + i);
                e.printStackTrace();
                break;
            }
        }

        r.close();
        long rate = words / size * 10 * 1000L / (System.nanoTime() - start);
        System.out.println("Rate = " + rate / 10.0 + " Mmsg/sec for " + size * 4 + " byte messages");
    }

    @Test
    public void multiThreaded2() throws IOException {
        final String basePath = System.getProperty("java.io.tmpdir") + "/multiThreaded";
        final String basePath2 = System.getProperty("java.io.tmpdir") + "/multiThreaded2";
        ChronicleTools.deleteOnExit(basePath);
        ChronicleTools.deleteOnExit(basePath2);

        final int runs = 1000 * 1000 * 1000;
        final int size = 4;
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if (WITH_BINDING)
                        PosixJNAAffinity.INSTANCE.setAffinity(1L << 5);
                    IndexedChronicle chronicle = new IndexedChronicle(basePath);
                    final ExcerptAppender w = chronicle.createAppender();
                    for (int i = 0; i < runs; i += size) {
                        w.startExcerpt(4 * size);
                        for (int s = 0; s < size; s++)
                            w.writeInt(1 + i);
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

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if (WITH_BINDING)
                        PosixJNAAffinity.INSTANCE.setAffinity(1L << 2);
                    IndexedChronicle chronicle = new IndexedChronicle(basePath);
                    final ExcerptTailer r = chronicle.createTailer();
                    IndexedChronicle chronicle2 = new IndexedChronicle(basePath2);
                    final ExcerptAppender w = chronicle2.createAppender();
                    for (int i = 0; i < runs; i += size) {
                        do {
                        } while (!r.nextIndex());

                        w.startExcerpt(r.remaining());
                        for (int s = 0; s < size; s++)
                            w.writeInt(r.readInt());
                        r.finish();
                        w.finish();
                    }
                    w.close();
                    chronicle.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t2.start();

        IndexedChronicle chronicle = new IndexedChronicle(basePath2);
        final ExcerptTailer r = chronicle.createTailer();

        for (int i = 0; i < runs; i += size) {
            do {
            } while (!r.nextIndex());
            try {
                for (int s = 0; s < size; s++) {
                    long l = r.readInt();
                    if (l != i + 1)
                        throw new AssertionError();
                }
                r.finish();
            } catch (Exception e) {
                System.err.println("i= " + i);
                e.printStackTrace();
                break;
            }
        }
        r.close();
        long rate = 2 * runs / size * 10000L / (System.nanoTime() - start);
        System.out.println("Rate = " + rate / 10.0 + " Mmsg/sec");
    }
}
