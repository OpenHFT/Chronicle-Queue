/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.micros.Order;
import net.openhft.chronicle.queue.micros.Side;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.TreeMap;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

/*
 * Created by Peter Lawrey on 05/03/2016.
 */
public class SingleCQFormat2Test extends ChronicleQueueTestBase {

    static {
        // init class
        SingleChronicleQueueBuilder.addAliases();
    }

    private int appendMode;
    private ThreadDump threadDump;

    private static void assertHexEquals(long a, long b) {
        if (a != b)
            assertEquals(Long.toHexString(a) + " != " + Long.toHexString(b), a, b);
    }

    @Test
    public void testMyData() {
        @NotNull File dir = getTmpDir();
        ClassAliasPool.CLASS_ALIASES.addAlias(MyData.class);

        try (@NotNull ChronicleQueue queue = binary(dir)
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE).build()) {
            @NotNull ExcerptAppender appender = queue.acquireAppender();
            try (DocumentContext dc = appender.writingDocument()) {
                @NotNull MyData name = new MyData("name", 12345, 1.2, 111);
                System.out.println(name);
                name.writeMarshallable(dc.wire());

                @NotNull MyData name2 = new MyData("name2", 12346, 1.3, 112);
                System.out.println(name2);
                name2.writeMarshallable(dc.wire());
            }
            String dump = queue.dump();
            assertTrue(dump, dump.contains("--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  464,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]"));
        }
    }

    @Test
    public void testWritingThreeMessages() throws FileNotFoundException {
        for (int m = 0; m <= 2; m++) {
            appendMode = m;

            @NotNull File dir = getTmpDir();
            dir.mkdir();

            try (@NotNull RollingChronicleQueue queue = binary(dir)
                    .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                    .indexCount(8)
                    .indexSpacing(1)
                    .build()) {

                long start = RollCycles.DAILY.toIndex(queue.cycle(), 0);
                appendMessage(queue, start, "Hello World");
                @NotNull String expectedEager = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    464,\n" +
                        "    1992864825344\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 264,\n" +
                        "    lastIndex: 1\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  lastIndexReplicated: -1\n" +
                        "}\n" +
                        "# position: 264, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  368,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 368, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  464,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 464, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130584 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager);

                appendMessage(queue, start + 1, "Another Hello World");
                @NotNull String expectedEager2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    484,\n" +
                        "    2078764171265\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 264,\n" +
                        "    lastIndex: 2\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  lastIndexReplicated: -1\n" +
                        "}\n" +
                        "# position: 264, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  368,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 368, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  464,\n" +
                        "  484,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 464, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 484, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "...\n" +
                        "# 130556 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager2);

                appendMessage(queue, start + 2, "Bye for now");

                @NotNull String expectedEager3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    512,\n" +
                        "    2199023255554\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 264,\n" +
                        "    lastIndex: 3\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  lastIndexReplicated: -1\n" +
                        "}\n" +
                        "# position: 264, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  368,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 368, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 3\n" +
                        "  464,\n" +
                        "  484,\n" +
                        "  512,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 464, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 484, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "# position: 512, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 130536 bytes remaining\n";

                checkFileContents(getFirstQueueFile(dir), expectedEager3);
            }
        }
    }

    private File getFirstQueueFile(final File file) {
        return file.listFiles((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX))[0];
    }

    public void checkFileContents(@NotNull File file, String expected) throws FileNotFoundException {
        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(file, ChronicleQueue.TEST_BLOCK_SIZE);
        bytes.readLimit(bytes.realCapacity());
        assertEquals(expected, Wires.fromSizePrefixedBlobs(bytes));
        bytes.release();
    }

    @Test
    public void testWritingTwentyMessagesTinyIndex() throws FileNotFoundException {
        for (int spacing : new int[]{1, 2, 4}) {
            @NotNull File dir = getTmpDir();
            dir.mkdir();

            try (@NotNull RollingChronicleQueue queue = binary(dir)
                    .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                    // only do this for testing
                    .indexCount(8)
                    .indexSpacing(spacing)
                    .build()) {

                long start = RollCycles.DAILY.toIndex(queue.cycle(), 0);
                @NotNull ExcerptTailer tailer = queue.createTailer();
                assertFalse(tailer.moveToIndex(start));

                appendMessage(queue, start, "Hello World");
                @NotNull String expectedEager = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    464,\n" +
                        "    1992864825344\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 264,\n" +
                        "    lastIndex: 1\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  lastIndexReplicated: -1\n" +
                        "}\n" +
                        "# position: 264, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  368,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 368, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  464,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 464, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130584 bytes remaining\n";

                checkFileContents(getFirstQueueFile(dir), expectedEager
                        .replace("indexSpacing: 1", "indexSpacing: " + spacing)
                        .replace("lastIndex: 1", "lastIndex: " + spacing));

                assertTrue(tailer.moveToIndex(start));
                for (int i = 1; i < 19; i++) {
                    assertFalse(tailer.moveToIndex(start + i));
                    appendMessage(queue, start + i, "Another Hello World " + (i + 1));
                    assertTrue(tailer.moveToIndex(start + i));
                }
                assertFalse(tailer.moveToIndex(start + 19));
                appendMessage(queue, start + 19, "Bye for now");
                assertTrue(tailer.moveToIndex(start + 19));
                assertFalse(tailer.moveToIndex(start + 20));

                @NotNull String expected1 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    1230,\n" +
                        "    5282809774099\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 264,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  lastIndexReplicated: -1\n" +
                        "}\n" +
                        "# position: 264, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 3\n" +
                        "  368,\n" +
                        "  726,\n" +
                        "  1072,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 368, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  464,\n" +
                        "  484,\n" +
                        "  514,\n" +
                        "  544,\n" +
                        "  576,\n" +
                        "  606,\n" +
                        "  636,\n" +
                        "  666\n" +
                        "]\n" +
                        "# position: 464, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 484, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 514, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 544, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 576, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 606, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 636, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 666, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 696, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 726, header: 8\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  696,\n" +
                        "  824,\n" +
                        "  855,\n" +
                        "  886,\n" +
                        "  917,\n" +
                        "  948,\n" +
                        "  979,\n" +
                        "  1010\n" +
                        "]\n" +
                        "# position: 824, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 855, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 886, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 917, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 948, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 979, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1010, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1041, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1072, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 4\n" +
                        "  1041,\n" +
                        "  1168,\n" +
                        "  1199,\n" +
                        "  1230,\n" +
                        "  0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1168, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1199, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1230, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129818 bytes remaining\n";
                @NotNull String expected2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    1134,\n" +
                        "    4870492913683\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 2,\n" +
                        "    index2Index: 264,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  lastIndexReplicated: -1\n" +
                        "}\n" +
                        "# position: 264, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  368,\n" +
                        "  974,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 368, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  464,\n" +
                        "  514,\n" +
                        "  576,\n" +
                        "  636,\n" +
                        "  696,\n" +
                        "  757,\n" +
                        "  819,\n" +
                        "  881\n" +
                        "]\n" +
                        "# position: 464, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 484, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 514, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 544, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 576, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 606, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 636, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 666, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 696, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 726, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 757, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 788, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 819, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 850, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 881, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 912, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 943, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 974, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  943,\n" +
                        "  1103,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1072, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1103, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1134, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129914 bytes remaining\n";
                @NotNull String expected3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    1036,\n" +
                        "    4449586118675\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 4,\n" +
                        "    index2Index: 264,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  lastIndexReplicated: -1\n" +
                        "}\n" +
                        "# position: 264, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  368,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 368, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 5\n" +
                        "  464,\n" +
                        "  576,\n" +
                        "  696,\n" +
                        "  819,\n" +
                        "  943,\n" +
                        "  0, 0, 0\n" +
                        "]\n" +
                        "# position: 464, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 484, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 514, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 544, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 576, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 606, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 636, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 666, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 696, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 726, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 757, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 788, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 819, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 850, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 881, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 912, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 943, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 974, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1005, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1036, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 130012 bytes remaining\n";

                @NotNull String expected = spacing == 1 ? expected1 :
                        spacing == 2 ? expected2 :
                                expected3;

                checkFileContents(getFirstQueueFile(dir), expected);
            }
        }
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
        threadDump.ignore(StoreComponentReferenceHandler.THREAD_NAME);
        threadDump.ignore(SingleChronicleQueue.DISK_SPACE_CHECKER_NAME);
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void resetAppendMode() {
        appendMode = 0;
    }

    public void appendMessage(@NotNull ChronicleQueue queue, long expectedIndex, String msg) {
        @NotNull ExcerptAppender appender = queue.acquireAppender();
        switch (appendMode) {
            case 1:
                appender.writeDocument(w -> w.write(() -> "msg").text(msg));
                break;

            case 2:
                Bytes bytes = Bytes.elasticByteBuffer();
                new BinaryWire(bytes).write(() -> "msg").text(msg);
                appender.writeBytes(bytes);
                bytes.release();

                break;

            default:
                try (DocumentContext dc = appender.writingDocument()) {
                    Wire wire = dc.wire();
                    wire.write(() -> "msg").text(msg);
                }
                break;
        }

        long index = appender.lastIndexAppended();
        assertHexEquals(expectedIndex, index);
    }

    @Test
    public void writeMap() {
        @NotNull Map<String, Object> map = new TreeMap<>();
        map.put("abc", "def");
        map.put("hello", "world");
        map.put("number", 1L);
        map.put("double", 1.28);
        @NotNull File dir = getTmpDir();
        try (@NotNull ChronicleQueue queue = binary(dir)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {
            ExcerptAppender appender = queue.acquireAppender();
            appender.writeMap(map);

            map.put("abc", "aye-bee-see");
            appender.writeMap(map);

            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    524,\n" +
                    "    2250562863105\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 264,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  lastIndexReplicated: -1\n" +
                    "}\n" +
                    "# position: 264, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 368, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  464,\n" +
                    "  524,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 464, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 524, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 130476 bytes remaining\n";
            assertEquals(expectedEager, queue.dump());

            @NotNull ExcerptTailer tailer = queue.createTailer();
            Map<String, Object> map2 = tailer.readMap();
            Map<String, Object> map3 = tailer.readMap();
            assertEquals("{abc=def, double=1.28, hello=world, number=1}", map2.toString());
            assertEquals("{abc=aye-bee-see, double=1.28, hello=world, number=1}", map3.toString());
            assertNull(tailer.readMap());
        }
    }

    @Test
    public void writeMarshallable() {
        ClassAliasPool.CLASS_ALIASES.addAlias(Order.class);
        @NotNull File dir = getTmpDir();
        try (@NotNull ChronicleQueue queue = binary(dir)
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build()) {
            @NotNull ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(new Order("Symbol", Side.Buy, 1.2345, 1e6));
            appender.writeDocument(w -> w.write("newOrder").object(new Order("Symbol2", Side.Sell, 2.999, 10e6)));
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    519,\n" +
                    "    2229088026625\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 264,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  lastIndexReplicated: -1\n" +
                    "}\n" +
                    "# position: 264, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 368, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  464,\n" +
                    "  519,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 464, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "# position: 519, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n" +
                    "# 130470 bytes remaining\n";
            assertEquals(expectedEager, queue.dump());
        }
    }

    @Test
    public void testWritingIndex() {
        @NotNull File dir = getTmpDir();
        try (@NotNull ChronicleQueue queue = ChronicleQueueBuilder.single(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build()) {
            @NotNull final ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("msg-1");
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    464,\n" +
                    "    1992864825344\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 264,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  lastIndexReplicated: -1\n" +
                    "}\n" +
                    "# position: 264, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 368, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  464,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 464, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "...\n" +
                    "# 130595 bytes remaining\n";
            assertEquals(expectedEager, queue.dump());
            for (int i = 1; i <= 16; i++)
                appender.writeText("msg-" + i);
            String expectedEager2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    709,\n" +
                    "    3045131812880\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 264,\n" +
                    "    lastIndex: 17\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  lastIndexReplicated: -1\n" +
                    "}\n" +
                    "# position: 264, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  368,\n" +
                    "  545,\n" +
                    "  719,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 368, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  464,\n" +
                    "  473,\n" +
                    "  482,\n" +
                    "  491,\n" +
                    "  500,\n" +
                    "  509,\n" +
                    "  518,\n" +
                    "  527\n" +
                    "]\n" +
                    "# position: 464, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 473, header: 1\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 482, header: 2\n" +
                    "--- !!data\n" +
                    "msg-2\n" +
                    "# position: 491, header: 3\n" +
                    "--- !!data\n" +
                    "msg-3\n" +
                    "# position: 500, header: 4\n" +
                    "--- !!data\n" +
                    "msg-4\n" +
                    "# position: 509, header: 5\n" +
                    "--- !!data\n" +
                    "msg-5\n" +
                    "# position: 518, header: 6\n" +
                    "--- !!data\n" +
                    "msg-6\n" +
                    "# position: 527, header: 7\n" +
                    "--- !!data\n" +
                    "msg-7\n" +
                    "# position: 536, header: 8\n" +
                    "--- !!data\n" +
                    "msg-8\n" +
                    "# position: 545, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  536,\n" +
                    "  640,\n" +
                    "  649,\n" +
                    "  659,\n" +
                    "  669,\n" +
                    "  679,\n" +
                    "  689,\n" +
                    "  699\n" +
                    "]\n" +
                    "# position: 640, header: 9\n" +
                    "--- !!data\n" +
                    "msg-9\n" +
                    "# position: 649, header: 10\n" +
                    "--- !!data\n" +
                    "msg-10\n" +
                    "# position: 659, header: 11\n" +
                    "--- !!data\n" +
                    "msg-11\n" +
                    "# position: 669, header: 12\n" +
                    "--- !!data\n" +
                    "msg-12\n" +
                    "# position: 679, header: 13\n" +
                    "--- !!data\n" +
                    "msg-13\n" +
                    "# position: 689, header: 14\n" +
                    "--- !!data\n" +
                    "msg-14\n" +
                    "# position: 699, header: 15\n" +
                    "--- !!data\n" +
                    "msg-15\n" +
                    "# position: 709, header: 16\n" +
                    "--- !!data\n" +
                    "msg-16\n" +
                    "# position: 719, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  709,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 130252 bytes remaining\n";
            assertEquals(expectedEager2, queue.dump());
        }
    }

    @After
    public void checkMappedFiles() {
        MappedFile.checkMappedFiles();
    }

    private static class MyData extends AbstractMarshallable {
        final String name;
        final long num;
        final double d;
        final int counter;

        MyData(String name, long num, double d, int counter) {
            this.name = name;
            this.num = num;
            this.d = d;
            this.counter = counter;
        }
    }
}
