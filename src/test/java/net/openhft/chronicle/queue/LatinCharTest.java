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

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static org.junit.Assert.assertEquals;

public class LatinCharTest extends QueueTestCommon {

    private static class Message extends SelfDescribingMarshallable {
        String s;
        long l;

        Message(final String s, final long l) {
            this.s = s;
            this.l = l;
        }

        Message() {
        }
    }

    @Test
    public void shouldCorrectlyEncodeDecode() {

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(DirectoryUtils.tempDir("temp"))
                .rollCycle(MINUTELY)
                .build();
             ExcerptAppender appender = queue.acquireAppender();
             ExcerptTailer tailer = queue.createTailer("test-tailer")) {

            // the é character in the line below is causing it to fail under java 11
            Message expected = new Message("awésome-message-1", 1L);
            appender.writeDocument(expected);

            Message actual = new Message();
            tailer.readDocument(actual);

            assertEquals(expected, actual);
        }

    }
}
