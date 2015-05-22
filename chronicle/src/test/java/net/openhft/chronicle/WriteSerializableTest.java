/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle;

import net.openhft.chronicle.tools.ChronicleTools;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.constraints.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.*;

public class WriteSerializableTest extends VanillaChronicleTestBase {
    @Test
    public void testWriteSerializable0() throws IOException {
        String basePath = getTestPath();
        ChronicleTools.deleteOnExit(basePath);
        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = chronicle.createTailer();
        try {
            for (int i = 0; i < 10; i++) {
                appender.startExcerpt();
                appender.writeUTFΔ("test");
                appender.writeObject(new MyData0());
                appender.finish();

                assertEquals(i + 1, appender.index());

                assertTrue(tailer.nextIndex());
                assertEquals(59, tailer.remaining());
                assertEquals("test", tailer.readUTFΔ());
                assertEquals(MyData0.class, tailer.readObject().getClass());
                tailer.finish();
            }
        } finally {
            chronicle.close();
        }
    }

    @Test
    public void testWriteSerializable() throws IOException {
        String basePath = getTestPath();
        ChronicleTools.deleteOnExit(basePath);
        Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = chronicle.createTailer();
        try {
            for (int i = 0; i < 10; i++) {
                appender.startExcerpt();
                appender.writeUTFΔ("test");
                appender.writeObject(new MyData());
                appender.finish();
                try {
                    appender.finish();
                    fail();
                } catch (IllegalStateException expected) {
                }

                assertEquals(i + 1, appender.index());

                assertTrue(tailer.nextIndex());
                assertEquals(90, tailer.remaining());
                assertEquals("test", tailer.readUTFΔ());
                assertEquals(MyData.class, tailer.readObject().getClass());
                tailer.finish();
            }
        } finally {
            chronicle.close();
        }
    }

    static class MyData0 implements BytesMarshallable {
        @Override
        public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        }

        @Override
        public void writeMarshallable(@NotNull Bytes out) {
        }
    }

    static class MyData implements Serializable {
    }
}

