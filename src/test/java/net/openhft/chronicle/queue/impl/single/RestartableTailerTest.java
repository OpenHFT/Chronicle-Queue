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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RestartableTailerTest extends QueueTestCommon {
    @Test
    public void restartable() {
        String tmp = OS.getTarget() + "/restartable-" + Time.uniqueId();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             final ExcerptAppender excerptAppender = cq.createAppender()) {
            for (int i = 0; i < 7; i++) {
                excerptAppender.writeText("test " + i);
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 0", atailer.readText());
            assertEquals("test 1", atailer.readText());
            assertEquals("test 2", atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 0", btailer.readText());
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 3", atailer.readText());
            assertEquals("test 4", atailer.readText());
            assertEquals("test 5", atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 1", btailer.readText());
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertEquals("test 6", atailer.readText());
            assertNull(atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 2", btailer.readText());
            }
        }

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();
             ExcerptTailer atailer = cq.createTailer("a")) {
            assertNull(atailer.readText());

            try (ExcerptTailer btailer = cq.createTailer("b")) {
                assertEquals("test 3", btailer.readText());
            }
        } finally {
            IOTools.deleteDirWithFiles(tmp);
        }
    }
}
