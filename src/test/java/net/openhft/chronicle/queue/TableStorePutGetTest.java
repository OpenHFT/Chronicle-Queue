/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"deprecation", "removal"})
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
                assertEquals(0x487600000000L, index, "table store should return expected index value for stored entry key '=hello'");
                assertTrue(tailer.moveToIndex(index), "tailer should successfully move to index retrieved from table store");
                assertEquals(index, tailer.index(), "tailer should be positioned at the index retrieved from table store");
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertEquals("hello", dc.wire().readEvent(String.class), "document should contain event key 'hello' at indexed position");
                    assertEquals("world", dc.wire().getValueIn().text(), "document should contain text value 'world' for event key 'hello'");
                }
            }
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
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
                    "...\n", cq.dump().replaceAll("(?m)^#.+$\\n", ""), "queue dump should match expected structure with table store entry and message data");
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
                assertEquals(j, l, "table store should return correct value " + j + " for key '=hello" + j + "' among 2280 stored entries");
            }
        }
        IOTools.deleteDirWithFiles(tempDir);
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
            String keyPrefix = "=this_is_a_long_key_to_try_and_consume_space_quicker_";
            int count = 1_800;
            for (int j = 0; j < count; j++) {
                cq.tableStorePut(keyPrefix + j, j);
            }
            assertEquals(0L, cq.tableStoreGet(keyPrefix + 0), "tableStore: first entry");
            assertEquals(count - 1L, cq.tableStoreGet(keyPrefix + (count - 1)), "tableStore: last entry");
        }
    }
}
