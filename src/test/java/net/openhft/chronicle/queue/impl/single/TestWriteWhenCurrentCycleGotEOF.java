/*
 * Copyright 2014-2017 Higher Frequency Trading
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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestWriteWhenCurrentCycleGotEOF extends ChronicleQueueTestBase {

    @Test
    @Ignore("Synthetic")
    public void shouldBeAbleToWrite() throws TimeoutException {
        File tmpDir = getTmpDir();
        createQueueWithOnlyHeaderFile(tmpDir);
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir).testBlockSize().build();

        ExcerptAppender appender = queue.acquireAppender();
        try (DocumentContext dc = appender.writingDocument()) {
            assertTrue(dc.isPresent());
        }
    }

    private void createQueueWithOnlyHeaderFile(File dir) {
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).testBlockSize().build();

        queue.storeForCycle(queue.cycle(), queue.epoch(), true);

        ExcerptTailer tailer = queue.acquireTailer();

        try (DocumentContext dc = tailer.readingDocument()) {
            assertFalse(dc.isPresent());
        }

        Wire wire;
        ExcerptAppender excerptAppender = queue.acquireAppender();
        try (DocumentContext dc = excerptAppender.writingDocument()) {
            wire = dc.wire();
        }

        // overwrite last record with EOF
        Bytes<?> bytes = wire.bytes();
        bytes.writeVolatileInt(bytes.writePosition() - 5, Wires.END_OF_DATA);
        bytes.writeVolatileInt(bytes.writePosition() - 1, 0);
    }
}
