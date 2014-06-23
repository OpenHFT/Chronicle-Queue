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
import net.openhft.chronicle.ChronicleType;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryChronicleTest extends InMemoryChronicleTestBase {

    @Test
    public void testInMemorySynk_001() throws Exception {
        final int items = 5;
        final String basePathSource = getTestPath("-source");

        final Chronicle source = indexedChronicleSource(basePathSource, BASE_PORT + 1);
        final ExcerptAppender appender = source.createAppender();

        try {
            for (int i = 0; i < items; i++) {
                appender.startExcerpt(8);
                appender.writeLong(i);
                appender.finish();
            }

            appender.close();

            final Chronicle sink = new InMemoryChronicleSynk(ChronicleType.INDEXED, "localhost", BASE_PORT + 1);
            final ExcerptTailer tailer = sink.createTailer().toStart();

            for (long i = 0; i < items; i++) {
                assertTrue(tailer.nextIndex());
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
}
