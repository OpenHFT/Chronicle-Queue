package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableStorePutGetTest extends QueueTestCommon {
    @Test
    public void indexEntry() {
        SetTimeProvider stp = new SetTimeProvider("2020/10/15T01:01:01");
        try (SingleChronicleQueue cq = ChronicleQueue.singleBuilder(OS.getTarget() + "/indexEntry-" + Time.uniqueId())
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
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: !int 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 176, header: 0\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18550\n" +
                    "# position: 216, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18550\n" +
                    "# position: 256, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 1\n" +
                    "# position: 288, header: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "# position: 328, header: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "# position: 368, header: 5\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "# position: 416, header: 6\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "# position: 472, header: 7\n" +
                    "--- !!data #binary\n" +
                    "=hello: 79671643340800\n" +
                    "...\n" +
                    "# 65036 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    392,\n" +
                    "    1683627180032\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 196, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  296,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 296, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  392,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 392, header: 0\n" +
                    "--- !!data #binary\n" +
                    "hello: world\n" +
                    "...\n" +
                    "# 130660 bytes remaining\n", cq.dump());
        }
    }

    @Test
    public void manyEntries() {
        try (SingleChronicleQueue cq = ChronicleQueue.singleBuilder(OS.getTarget() + "/manyEntries-" + Time.uniqueId())
                .rollCycle(RollCycles.TEST_DAILY)
                .testBlockSize()
                .build()) {
            for (int i = 0; i < 10000; i++) {
                cq.tableStorePut("=hello" + i, i);
            }
        }
    }
}
