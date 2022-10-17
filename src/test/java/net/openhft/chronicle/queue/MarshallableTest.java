/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MarshallableTest extends QueueTestCommon {
    @Test
    public void testWriteText() {
        File dir = getTmpDir();
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            ExcerptTailer tailer = queue.createTailer();
            ExcerptTailer tailer2 = queue.createTailer();

            int runs = 1000;
            for (int i = 0; i < runs; i++)
                appender.writeText("" + i);
            for (int i = 0; i < runs; i++)
                assertEquals("" + i, tailer.readText());
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < runs; i++) {
                assertTrue(tailer2.readText(sb));
                assertEquals("" + i, sb.toString());
            }
        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }
}
