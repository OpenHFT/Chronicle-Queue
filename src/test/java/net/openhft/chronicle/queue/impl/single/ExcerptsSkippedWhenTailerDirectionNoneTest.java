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

import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;

public final class ExcerptsSkippedWhenTailerDirectionNoneTest extends QueueTestCommon {
    @Test
    public void shouldNotSkipMessageAtStartOfQueue() {
        final File tmpDir = getTmpDir();
        try (final ChronicleQueue writeQueue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .rollCycle(TEST_DAILY)
                             .build()) {
            final ExcerptAppender excerptAppender = writeQueue.acquireAppender();
            try (final DocumentContext ctx = excerptAppender.writingDocument()) {
                ctx.wire().getValueOut().object("first");
            }
            try (final DocumentContext ctx = excerptAppender.writingDocument()) {
                ctx.wire().getValueOut().object("second");
            }
        }

        try (final ChronicleQueue readQueue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .rollCycle(TEST_DAILY)
                             .build()) {

            final ExcerptTailer tailer = readQueue.createTailer();
            final RollCycle rollCycle = readQueue.rollCycle();
            assertEquals(0L, rollCycle.toSequenceNumber(tailer.index()));
            try (final DocumentContext ctx = tailer.direction(TailerDirection.NONE).readingDocument()) {
                // access the first document without incrementing sequence number
            }
            assertEquals(0L, rollCycle.toSequenceNumber(tailer.index()));

            String value;
            try (DocumentContext dc =
                         tailer.direction(TailerDirection.FORWARD).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertEquals(1L, rollCycle.toSequenceNumber(tailer.index()));

            assertEquals("first", value);

            try (DocumentContext dc =
                         tailer.direction(TailerDirection.NONE).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertEquals(1L, rollCycle.toSequenceNumber(tailer.index()));

            assertEquals("second", value);

            try (DocumentContext dc =
                         tailer.direction(TailerDirection.NONE).readingDocument()) {
                ValueIn valueIn = dc.wire().getValueIn();
                value = (String) valueIn.object();
            }
            assertEquals(1L, rollCycle.toSequenceNumber(tailer.index()));

            assertEquals("second", value);
        }
    }
}
