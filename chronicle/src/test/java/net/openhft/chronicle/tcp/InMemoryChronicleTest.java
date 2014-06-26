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
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryChronicleTest extends InMemoryChronicleTestBase {

    public void testInMemorySink(final Chronicle source, final Chronicle sink) throws Exception {
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

            for (long i = 0; i < items; i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i, tailer.index());
                assertEquals(i, tailer.readLong());
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

    // *************************************************************************
    // INDEXED
    // *************************************************************************

    @Test
    public void testIndexedInMemorySink_001() throws Exception {
        final int port = BASE_PORT + 100;
        final String basePathSource = getTestPath("-source");

        testInMemorySink(
            indexedChronicleSource(basePathSource, port),
            inMemoryIndexedChronicleSink("localhost", port)
        );
    }

    @Test
    public void testIndexedInMemorySink_002() throws Exception {
        final int port = BASE_PORT + 101;
        final String basePathSource = getTestPath("-source");

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
                assertTrue(tailer.nextIndex());
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

    // *************************************************************************
    // VANILLA
    // *************************************************************************

    @Ignore
    @Test
    public void testVanillaInMemorySink_001() throws Exception {
        final int port = BASE_PORT + 200;
        final String basePathSource = getTestPath("-source");

        testInMemorySink(
            vanillaChronicleSource(basePathSource, port),
            inMemoryVanillaChronicleSink("localhost", port)
        );
    }
}
