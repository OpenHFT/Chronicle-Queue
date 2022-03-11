package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableStorePutGetTest extends QueueTestCommon {
    @Test
    public void indexEntry() {
        SetTimeProvider stp = new SetTimeProvider("2020/10/15T01:01:01");
        try (SingleChronicleQueue cq = ChronicleQueue.singleBuilder(DirectoryUtils.tempDir("indexEntry"))
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(stp)
                .testBlockSize()
                .build()) {
            try (ExcerptAppender appender = cq.acquireAppender()) {
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
                    "listing.modCount: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
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
        try (SingleChronicleQueue cq = ChronicleQueue.singleBuilder(DirectoryUtils.tempDir("manyEntries"))
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(64 << 10)
                .build()) {
            for (int j = 0; j < 2280; j++) {
                cq.tableStorePut("=hello" + j, j);
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
                .rollCycle(RollCycles.TEST_DAILY)
                .testBlockSize()
                .build()) {
            for (int j = 0; j < 4_000; j++) {
                cq.tableStorePut("=this_is_a_long_key_to_try_and_consume_space_quicker_" + j, j);
            }
        }
    }
}
