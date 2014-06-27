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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class InMemoryIndexedChronicleTest extends InMemoryChronicleTestBase {

    @Test
    public void testIndexedInMemorySink_001() throws Exception {
        final int port = BASE_PORT + 101;
        final String basePathSource = getIndexedTestPath("-source");
        final Chronicle source = indexedChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryIndexedChronicleSink("localhost", port);

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
                assertEquals(i, tailer1.index());
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
    public void testIndexedInMemorySink_002() throws Exception {
        final int port = BASE_PORT + 102;
        final String basePathSource = getIndexedTestPath("-source");
        final Chronicle source = indexedChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryIndexedChronicleSink("localhost", port);

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

            assertEquals(2, tailer.index());
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
    public void testIndexedInMemorySink_003() throws Exception {
        final int port = BASE_PORT + 103;
        final String basePathSource = getIndexedTestPath("-source");

        final Chronicle source = indexedChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryIndexedChronicleSink("localhost", port);

        final int items = 1000000;
        final ExcerptAppender appender = source.createAppender();

        try {
            for (int i = 0; i < items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            final ExcerptTailer tailer = sink.createTailer().toStart();

            final Random r = new Random();
            for(int i=0;i<1000;i++) {
                int index = r.nextInt(items - -1) + -1;

                assertTrue(tailer.index(index));
                assertEquals(index + 1, tailer.index());
                assertEquals(index + 1, tailer.readLong());

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

    @Test
    public void testIndexedInMemorySink_004() throws Exception {
        final int port = BASE_PORT + 104;
        final int tailers = 4;
        final int items = 1000000;
        final String basePathSource = getIndexedTestPath("-source");
        final Chronicle source = indexedChronicleSource(basePathSource, port);
        final Chronicle sink = inMemoryIndexedChronicleSink("localhost", port);
        final ExecutorService executor = Executors.newFixedThreadPool(tailers + 1);

        try {

            for(int i=0;i<tailers;i++) {
                executor.submit(new Runnable() {
                    public void run() {
                        try {
                            final ExcerptTailer tailer = sink.createTailer().toStart();
                            for (int i = 0; i < items; ) {
                                if (tailer.nextIndex()) {
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
                });
            }

            Thread.sleep(100);

            executor.submit(new Runnable() {
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
            });

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }
}
