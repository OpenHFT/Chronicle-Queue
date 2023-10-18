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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StoreTailerNotReachedTest extends QueueTestCommon {
    @Test
    public void afterNotReached() {
        String path = OS.getTarget() + "/afterNotReached-" + Time.uniqueId();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = q.createAppender()) {
            appender.writeText("Hello");
            ExcerptTailer tailer = q.createTailer();
            assertEquals("Hello", tailer.readText());
            assertNull(tailer.readText());
            appender.writeText("World");
            assertEquals("World", tailer.readText());
            assertNull(tailer.readText());
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
