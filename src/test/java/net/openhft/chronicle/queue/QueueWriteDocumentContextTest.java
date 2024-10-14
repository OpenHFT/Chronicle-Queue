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

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.WriteDocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.Test;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueueWriteDocumentContextTest extends QueueTestCommon {

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
        try (ChronicleQueue cq = createQueue(s);
             final ExcerptAppender appender = cq.createAppender()) {
            Assume.assumeFalse("Ignored on hugetlbfs as byte offsets will be different due to page size", PageUtil.isHugePage(cq.file().getAbsolutePath()));
            writeThreeKeys(appender);
            assertEquals("" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 152, header: 0\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "# position: 192, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "# position: 232, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 3\n" +
                    "# position: 264, header: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "# position: 304, header: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "# position: 344, header: 5\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "# position: 392, header: 6\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "# position: 448, header: 7\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexMSynced: -1\n" +
                    "...\n" +
                    "# 130572 bytes remaining\n" +
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
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 304, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  400,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 400, header: 0\n" +
                    "--- !!data #binary\n" +
                    "key: 0\n" +
                    "key: 1\n" +
                    "key: 2\n" +
                    "...\n" +
                    "# 130644 bytes remaining\n", cq.dump());
        }
    }

    @Test
    public void chainedPlainText() {
        String s = "/chainedPlainText";
        try (ChronicleQueue cq = createQueue(s);
             final ExcerptAppender appender = cq.createAppender()) {
            Assume.assumeFalse("Ignored on hugetlbfs as byte offsets will be different due to page size", PageUtil.isHugePage(cq.file().getAbsolutePath()));
            writeThreeChainedKeys(appender);
            assertEquals("" +
                            "--- !!meta-data #binary\n" +
                            "header: !STStore {\n" +
                            "  wireType: !WireType BINARY_LIGHT,\n" +
                            "  metadata: !SCQMeta {\n" +
                            "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                            "    sourceId: 0\n" +
                            "  }\n" +
                            "}\n" +
                            "# position: 152, header: 0\n" +
                            "--- !!data #binary\n" +
                            "listing.highestCycle: 18554\n" +
                            "# position: 192, header: 1\n" +
                            "--- !!data #binary\n" +
                            "listing.lowestCycle: 18554\n" +
                            "# position: 232, header: 2\n" +
                            "--- !!data #binary\n" +
                            "listing.modCount: 3\n" +
                            "# position: 264, header: 3\n" +
                            "--- !!data #binary\n" +
                            "chronicle.write.lock: -9223372036854775808\n" +
                            "# position: 304, header: 4\n" +
                            "--- !!data #binary\n" +
                            "chronicle.append.lock: -9223372036854775808\n" +
                            "# position: 344, header: 5\n" +
                            "--- !!data #binary\n" +
                            "chronicle.lastIndexReplicated: -1\n" +
                            "# position: 392, header: 6\n" +
                            "--- !!data #binary\n" +
                            "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                            "# position: 448, header: 7\n" +
                            "--- !!data #binary\n" +
                            "chronicle.lastIndexMSynced: -1\n" +
                            "...\n" +
                            "# 130572 bytes remaining\n" +
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
                            "# position: 200, header: -1\n" +
                            "--- !!meta-data #binary\n" +
                            "index2index: [\n" +
                            "  # length: 8, used: 1\n" +
                            "  304,\n" +
                            "  0, 0, 0, 0, 0, 0, 0\n" +
                            "]\n" +
                            "# position: 304, header: -1\n" +
                            "--- !!meta-data #binary\n" +
                            "index: [\n" +
                            "  # length: 8, used: 1\n" +
                            "  400,\n" +
                            "  0, 0, 0, 0, 0, 0, 0\n" +
                            "]\n" +
                            "# position: 400, header: 0\n" +
                            "--- !!data #binary\n" +
                            "key: 0\n" +
                            "key: 1\n" +
                            "key: 2\n" +
                            "...\n" +
                            "# 130644 bytes remaining\n",
                    cq.dump());
        }
    }

    @NotNull
    protected SingleChronicleQueue createQueue(String s) {
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(tempDir(s))
                .rollCycle(TEST_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .blockSize(OS.SAFE_PAGE_SIZE);
        final SingleChronicleQueue queue = builder.build();
        return queue;
    }
}
