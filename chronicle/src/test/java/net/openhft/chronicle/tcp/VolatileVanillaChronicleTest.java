/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VolatileVanillaChronicleTest extends VolatileChronicleTestBase {

    @Test
    public void testVanillaVolatileSink_001() throws Exception {
        final int port = BASE_PORT + 201;
        final String basePathSource = getVanillaTestPath("-source");
        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = volatileChronicleSink("localhost", port);

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
            assertEquals(-1,tailer1.index());

            for (long i = 1; i <= items; i++) {
                assertTrue(tailer1.nextIndex());
                assertEquals(i, tailer1.readLong());
                tailer1.finish();
            }

            assertFalse(tailer1.nextIndex());
            tailer1.close();

            final ExcerptTailer tailer2 = sink.createTailer().toEnd();
            assertEquals(items, tailer2.readLong());
            assertFalse(tailer2.nextIndex());
            tailer2.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaVolatileSink_002() throws Exception {
        final int port = BASE_PORT + 202;
        final String basePathSource = getVanillaTestPath("-source");
        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = volatileChronicleSink("localhost", port);

        try {
            final ExcerptAppender appender = source.createAppender();
            appender.startExcerpt(8);
            appender.writeLong(1);
            appender.finish();
            appender.startExcerpt(8);
            appender.writeLong(2);
            appender.finish();

            final ExcerptTailer tailer = sink.createTailer().toEnd();
            assertFalse(tailer.nextIndex());

            appender.startExcerpt(8);
            appender.writeLong(3);
            appender.finish();

            while(!tailer.nextIndex());

            assertEquals(3, tailer.readLong());
            tailer.finish();
            tailer.close();

            appender.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaVolatileSink_004() throws Exception {
        final int port = BASE_PORT + 204;
        final int tailers = 4;
        final int items = 1000000;
        final String basePathSource = getVanillaTestPath("-source");
        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = volatileChronicleSink("localhost", port);
        final ExecutorService executor = Executors.newFixedThreadPool(tailers);

        try {
            for(int i=0;i<tailers;i++) {
                executor.submit(new Runnable() {
                    public void run() {
                        try {
                            final ExcerptTailer tailer = sink.createTailer().toStart();
                            for (int i = 1; i <= items; ) {
                                if (tailer.nextIndex()) {
                                    assertEquals(i, tailer.readLong());
                                    tailer.finish();

                                    i++;
                                }
                            }

                            tailer.close();
                        } catch (Exception e) {
                        }
                    }
                });
            }

            Thread.sleep(100);

            final ExcerptAppender appender = source.createAppender();

            for (int i=1; i<=items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaVolatileSink_005() throws Exception {
        final int port = BASE_PORT + 205;
        final String basePathSource = getVanillaTestPath("-source");
        final ChronicleSource source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = volatileChronicleSink("localhost", port);

        final int items = 1000;
        final ExcerptAppender appender = source.createAppender();
        final ExcerptTailer st = source.createTailer().toStart();
        final ExcerptTailer tailer = sink.createTailer();

        try {
            for (int i = 0; i < items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();

                st.nextIndex();
                st.finish();

                assertTrue(tailer.index(st.index()));
                assertEquals(i, tailer.readLong());
            }

            appender.close();
            tailer.close();
            st.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }

    @Test
    public void testVanillaVolatileSink_006() throws Exception {
        final int port = BASE_PORT + 206;
        final String basePathSource = getVanillaTestPath("-source");
        final Chronicle source = vanillaChronicleSource(basePathSource, port);
        final Chronicle sink = volatileChronicleSink("localhost", port);

        final int items = 1000000;
        final ExcerptAppender appender = source.createAppender();
        final ExcerptTailer st = source.createTailer().toStart();

        long startIndex = Long.MIN_VALUE;
        long endIndex = Long.MIN_VALUE;

        try {
            for (int i = 1; i <= items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();

                st.nextIndex();
                st.finish();

                if(i == 1) {
                    startIndex = st.index();
                } else if(i == items) {
                    endIndex = st.index();
                }
            }

            appender.close();
            st.close();

            final ExcerptTailer tailer1 = sink.createTailer().toStart();
            assertEquals(-1,tailer1.index());
            assertTrue(tailer1.nextIndex());
            assertEquals(startIndex, tailer1.index());
            assertEquals(1, tailer1.readLong());
            tailer1.finish();
            tailer1.close();

            final ExcerptTailer tailer2 = sink.createTailer().toEnd();
            assertEquals(endIndex, tailer2.index());
            assertEquals(items, tailer2.readLong());
            tailer2.finish();
            tailer2.close();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();

            assertFalse(new File(basePathSource).exists());
        }
    }
}
