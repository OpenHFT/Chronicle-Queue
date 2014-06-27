/*
 * Copyright 2014 Higher Frequency Trading
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
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class InMemoryVanillaChronicleTest extends InMemoryChronicleTestBase {

    @Test
    public void testVanillaInMemorySink_001() throws Exception {
        final int port = BASE_PORT + 201;
        final String basePathSource = getVanillaTestPath("-source");
        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryVanillaChronicleSink("localhost", port);

        final int items = 1000000;
        final ExcerptAppender appender = source.createAppender();

        try {
            for (int i = 0; i < items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            final ExcerptTailer tailer1 = sink.createTailer().toStart();
            for (long i = 0; i < items; i++) {
                assertEquals(i, tailer1.readLong());
                tailer1.finish();

                assertTrue(tailer1.nextIndex());
            }

            tailer1.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testVanillaInMemorySink_002() throws Exception {
        final int port = BASE_PORT + 202;
        final String basePathSource = getVanillaTestPath("-source");
        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryVanillaChronicleSink("localhost", port);

        try {
            final ExcerptAppender appender = source.createAppender();
            appender.startExcerpt(8);
            appender.writeLong(0);
            appender.finish();
            appender.startExcerpt(8);
            appender.writeLong(1);
            appender.finish();

            final ExcerptTailer tailer = sink.createTailer().toEnd();
            assertFalse(tailer.nextIndex());

            appender.startExcerpt(8);
            appender.writeLong(2);
            appender.finish();

            while(!tailer.nextIndex());

            assertEquals(2, tailer.readLong());
            tailer.finish();
            tailer.close();

            appender.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }

    @Test
    public void testVanillaInMemorySink_003() throws Exception {
        final int port = BASE_PORT + 303;
        final String basePathSource = getVanillaTestPath("-source");

        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryVanillaChronicleSink("localhost", port);
        final int items = 10;//1000000;
        final long[] indices = new long[items];
        final long[] values = new long[items];
        final ExcerptAppender appender = source.createAppender();

        try {
            for (int i = 0; i < items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();

                indices[i] = appender.index();
                values[i]  = i;
            }

            appender.close();

            final ExcerptTailer tailer = sink.createTailer().toStart();

            final Random r = new Random();
            for(int i=0;i<1000;i++) {
                int index = r.nextInt(items);

                assertTrue(tailer.index(indices[index]));
                assertTrue(tailer.nextIndex());
                System.out.println(">" + tailer.index());
                //assertEquals(indices[index], tailer.index());
                assertEquals(values[index], tailer.readLong());
                tailer.finish();
            }

            tailer.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }


    /*
    @Test
    public void testVanillaInMemorySink_004() throws Exception {
        final int port = BASE_PORT + 104;
        final int items = 1000000;
        final String basePathSource = getVanillaTestPath("-source");
        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryVanillaChronicleSink("localhost", port);

        try {

            final Thread appenderThread = new Thread() {
                public void run() {
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        final Random r = new Random();

                        for (int i=0; i<items; i++) {
                            appender.startExcerpt(8);
                            appender.writeLong(i);
                            appender.finish();
                        }


                        appender.close();
                    } catch (Exception e) {
                    }
                }
            };

            final Runnable runnableTailer = new Runnable() {
                public void run() {
                    try {
                        final ExcerptTailer tailer = sink.createTailer().toStart();
                        for(int i=0; i<items; ) {
                            if(tailer.nextIndex()) {
                                assertEquals(i, tailer.index());
                                assertEquals(i, tailer.readLong());
                                tailer.finish();

                                i++;
                            }
                        }

                        tailer.close();
                    } catch (Exception e) {
                    }
                }
            };


            final Thread tailerThread1 = new Thread(runnableTailer);
            final Thread tailerThread2 = new Thread(runnableTailer);
            tailerThread1.start();
            tailerThread2.start();

            Thread.sleep(100);

            appenderThread.start();

            tailerThread1.join();
            tailerThread2.join();
            appenderThread.join();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }
    */
}
