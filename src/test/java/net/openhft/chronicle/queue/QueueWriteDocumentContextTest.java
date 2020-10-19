package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.WriteDocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueueWriteDocumentContextTest {
    static void writeThreeKeys(MarshallableOut wire) {
        try (DocumentContext dc0 = wire.acquireWritingDocument(false)) {
            for (int i = 0; i < 3; i++) {
                try (DocumentContext dc = wire.acquireWritingDocument(false)) {
                    dc.wire().write("key").int32(i);
                }
                assertTrue(dc0.isNotComplete());
            }
        }
    }

    static void writeThreeChainedKeys(MarshallableOut wire) {
        for (int i = 0; i < 3; i++) {
            try (WriteDocumentContext dc = (WriteDocumentContext) wire.acquireWritingDocument(false)) {
                dc.wire().write("key").int32(i);
                dc.chainedElement(i < 2);
            }
        }
    }

    @Test
    public void nestedPlainText() {
        String s = "/nestedPlainText";
        try (ChronicleQueue cq = createQueue(s)) {
            writeThreeKeys(cq.acquireAppender());
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
                    "listing.highestCycle: 18554\n" +
                    "# position: 216, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "# position: 256, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 1\n" +
                    "# position: 288, header: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "# position: 328, header: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "# position: 376, header: 5\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "...\n" +
                    "# 65100 bytes remaining\n" +
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
                    "key: 0\n" +
                    "key: 1\n" +
                    "key: 2\n" +
                    "...\n" +
                    "# 130656 bytes remaining\n", cq.dump());
        }
    }

    @Test
    public void chainedPlainText() {
        String s = "/chainedPlainText";
        try (ChronicleQueue cq = createQueue(s)) {
            writeThreeChainedKeys(cq.acquireAppender());
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
                    "listing.highestCycle: 18554\n" +
                    "# position: 216, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "# position: 256, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 1\n" +
                    "# position: 288, header: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "# position: 328, header: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "# position: 376, header: 5\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "...\n" +
                    "# 65100 bytes remaining\n" +
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
                    "key: 0\n" +
                    "key: 1\n" +
                    "key: 2\n" +
                    "...\n" +
                    "# 130656 bytes remaining\n", cq.dump());
        }
    }

    @NotNull
    protected SingleChronicleQueue createQueue(String s) {
        IOTools.deleteDirWithFiles(OS.getTarget() + s);
        return SingleChronicleQueueBuilder.binary(OS.getTarget() + s)
                .rollCycle(RollCycles.TEST_DAILY)
                .testBlockSize().build();
    }
}
