/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import java.io.File;
import java.nio.file.Files;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueueTest extends ChronicleQueueTestBase {
    @Test
    public void testAppend() {
        ChronicleQueue queue = createQueue(WireType.TEXT);

        final ExcerptAppender appender = queue.createAppender();
        appender.writeDocument(w -> w.write(TestKey.test).int32(1));

        int x = 0;
        queue.close();
        int y = 0;

        //assert queue.file().delete();
    }

    @Test
    public void testMappedBytesWireRelease() throws Exception {
        File tmp = Files.createTempFile("chronicle", "map").toFile();
        tmp.deleteOnExit();

        MappedBytes mb = MappedBytes.mappedBytes(tmp, 1024);

        Wire wire = WireType.TEXT.apply(mb);
        wire.startUse();
        wire.headerNumber(1);
        wire.writeFirstHeader();
        wire.updateFirstHeader();

        assertEquals(2, mb.mappedFile().refCount());
        assertEquals(2, mb.refCount());

        mb.release();
        mb.release();
    }

    @Test
    public void testMappedBytesRelease() throws Exception {
        File tmp = Files.createTempFile("chronicle", "map").toFile();
        tmp.deleteOnExit();

        MappedBytes mb = MappedBytes.mappedBytes(tmp, 1024);
        mb.mappedFile().reserve();
        mb.reserve();

        assertEquals(2, mb.mappedFile().refCount());
        assertEquals(2, mb.refCount());

        mb.release();
        mb.release();
    }
}
