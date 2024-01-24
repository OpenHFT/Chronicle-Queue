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

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableStorePutGetTest extends QueueTestCommon {
    @Test
    public void indexEntry() {
        SetTimeProvider stp = new SetTimeProvider("2020/10/15T01:01:01");
        try (SingleChronicleQueue cq = ChronicleQueue.singleBuilder(DirectoryUtils.tempDir("indexEntry"))
                .rollCycle(TEST_DAILY)
                .timeProvider(stp)
                .testBlockSize()
                .build()) {
            try (ExcerptAppender appender = cq.createAppender()) {
                try (DocumentContext dc = appender.acquireWritingDocument(false)) {
                    dc.wire().write("hello").text("world");
                }
                cq.tableStorePut("=hello", appender.lastIndexAppended());
            }

            try (ExcerptTailer tailer = cq.createTailer()) {
                long index = cq.tableStoreGet("=hello");
                assertEquals(0x487600000000L, index);
                assertTrue(tailer.moveToIndex(index));
                assertEquals(index, tailer.index());
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertEquals("hello", dc.wire().readEvent(String.class));
                    assertEquals("world", dc.wire().getValueIn().text());
                }
            }
            assertEquals("" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18550\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18550\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexMSynced: -1\n" +
                    "--- !!data #binary\n" +
                    "=hello: 79671643340800\n" +
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    400,\n" +
                    "    1717986918400\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  400,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!data #binary\n" +
                    "hello: world\n" +
                    "...\n", cq.dump().replaceAll("(?m)^#.+$\\n", ""));
        }
    }

    @Test
    public void manyEntries() {
        final File tempDir = DirectoryUtils.tempDir("manyEntries");
        try (SingleChronicleQueue cq = ChronicleQueue.singleBuilder(tempDir)
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build()) {
            final int count = 2280;
            for (int j = 0; j < count; j++) {
                cq.tableStorePut("=hello" + j, j);
            }
            for (int j = 0; j < count; j++) {
                final long l = cq.tableStoreGet("=hello" + j);
                assertEquals(j, l);
            }
        }
    }

    /**
     * While the assumption is the TableStore doesn't grow, we should test what happens if it does
     * <p>
     * (see https://github.com/OpenHFT/Chronicle-Queue/issues/1025)
     */
    @Test
    public void testCanGrowBeyondInitialSize() {
        try (SingleChronicleQueue cq = ChronicleQueue.singleBuilder(DirectoryUtils.tempDir("canGrow"))
                .rollCycle(TEST_DAILY)
                .testBlockSize()
                .build()) {
            for (int j = 0; j < 4_000; j++) {
                cq.tableStorePut("=this_is_a_long_key_to_try_and_consume_space_quicker_" + j, j);
            }
        }
    }
}
