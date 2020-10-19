/*
 * Copyright 2016-2020 https://chronicle.software
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
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.micros.Order;
import net.openhft.chronicle.queue.micros.Side;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.TreeMap;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

public class SingleCQFormat2Test extends ChronicleQueueTestBase {

    static {
        // init class
        SingleChronicleQueueBuilder.addAliases();
    }

    private int appendMode;

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
//                System.out.println(name);
                name.writeMarshallable(dc.wire());

                @NotNull MyData name2 = new MyData("name2", 12346, 1.3, 112);
//                System.out.println(name2);
                name2.writeMarshallable(dc.wire());
            }
            String dump = queue.dump();
            assertTrue(dump, dump.contains("--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  392,\n" +
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

                long start = RollCycles.DEFAULT.toIndex(queue.cycle(), 0);
                appendMessage(queue, start, "Hello World");
                @NotNull String expectedEager = "--- !!meta-data #binary\n" +
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
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130656 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager);

                appendMessage(queue, start + 1, "Another Hello World");
                @NotNull String expectedEager2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    412,\n" +
                        "    1769526525953\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 196,\n" +
                        "    lastIndex: 2\n" +
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
                        "  # length: 8, used: 2\n" +
                        "  392,\n" +
                        "  412,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 392, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 412, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "...\n" +
                        "# 130628 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager2);

                appendMessage(queue, start + 2, "Bye for now");

                @NotNull String expectedEager3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    440,\n" +
                        "    1889785610242\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 196,\n" +
                        "    lastIndex: 3\n" +
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
                        "  # length: 8, used: 3\n" +
                        "  392,\n" +
                        "  412,\n" +
                        "  440,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 392, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 412, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "# position: 440, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 130608 bytes remaining\n";

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
        assertEquals(expected, Wires.fromAlignedSizePrefixedBlobs(bytes));
        bytes.releaseLast();
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

                long start = RollCycles.DEFAULT.toIndex(queue.cycle(), 0);
                @NotNull ExcerptTailer tailer = queue.createTailer();
                assertFalse(tailer.moveToIndex(start));

                appendMessage(queue, start, "Hello World");
                @NotNull String expectedEager = "--- !!meta-data #binary\n" +
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
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130656 bytes remaining\n";

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
                        "    1176,\n" +
                        "    5050881540115\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 196,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  dataFormat: 1\n" +
                        "}\n" +
                        "# position: 196, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 3\n" +
                        "  296,\n" +
                        "  668,\n" +
                        "  1016,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 296, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  392,\n" +
                        "  412,\n" +
                        "  444,\n" +
                        "  476,\n" +
                        "  508,\n" +
                        "  540,\n" +
                        "  572,\n" +
                        "  604\n" +
                        "]\n" +
                        "# position: 392, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 412, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 444, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 476, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 508, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 540, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 572, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 604, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 636, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 668, header: 8\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  636,\n" +
                        "  760,\n" +
                        "  792,\n" +
                        "  824,\n" +
                        "  856,\n" +
                        "  888,\n" +
                        "  920,\n" +
                        "  952\n" +
                        "]\n" +
                        "# position: 760, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 792, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 824, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 856, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 888, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 920, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 952, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 984, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1016, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 4\n" +
                        "  984,\n" +
                        "  1112,\n" +
                        "  1144,\n" +
                        "  1176,\n" +
                        "  0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1112, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1144, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1176, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129872 bytes remaining\n";
                @NotNull String expected2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    1080,\n" +
                        "    4638564679699\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 2,\n" +
                        "    index2Index: 196,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  dataFormat: 1\n" +
                        "}\n" +
                        "# position: 196, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  296,\n" +
                        "  924,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 296, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  392,\n" +
                        "  444,\n" +
                        "  508,\n" +
                        "  572,\n" +
                        "  636,\n" +
                        "  700,\n" +
                        "  764,\n" +
                        "  828\n" +
                        "]\n" +
                        "# position: 392, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 412, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 444, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 476, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 508, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 540, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 572, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 604, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 636, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 668, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 700, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 732, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 764, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 796, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 828, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 860, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 892, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 924, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  892,\n" +
                        "  1048,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1016, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1048, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1080, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129968 bytes remaining\n";
                @NotNull String expected3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    988,\n" +
                        "    4243427688467\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 4,\n" +
                        "    index2Index: 196,\n" +
                        "    lastIndex: 20\n" +
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
                        "  # length: 8, used: 5\n" +
                        "  392,\n" +
                        "  508,\n" +
                        "  636,\n" +
                        "  764,\n" +
                        "  892,\n" +
                        "  0, 0, 0\n" +
                        "]\n" +
                        "# position: 392, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 412, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 444, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 476, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 508, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 540, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 572, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 604, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 636, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 668, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 700, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 732, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 764, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 796, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 828, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 860, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 892, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 924, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 956, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 988, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 130060 bytes remaining\n";

                @NotNull String expected = spacing == 1 ? expected1 :
                        spacing == 2 ? expected2 :
                                expected3;

                checkFileContents(getFirstQueueFile(dir), expected);
            }
        }
    }

    @Before
    public void resetAppendMode() {
        appendMode = 0;
    }

    public void appendMessage(@NotNull ChronicleQueue queue, long expectedIndex, String msg) {
        @NotNull ExcerptAppender appender = queue.acquireAppender();
        switch (appendMode) {
            case 1:
                appender.writeDocument(w -> w.write("msg").text(msg));
                break;

            case 2:
                Bytes bytes = Bytes.elasticByteBuffer();
                new BinaryWire(bytes).write("msg").text(msg);
                appender.writeBytes(bytes);
                bytes.releaseLast();

                break;

            default:
                try (DocumentContext dc = appender.writingDocument()) {
                    Wire wire = dc.wire();
                    wire.write("msg").text(msg);
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
                    "    452,\n" +
                    "    1941325217793\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 2\n" +
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
                    "  # length: 8, used: 2\n" +
                    "  392,\n" +
                    "  452,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 392, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 452, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 130548 bytes remaining\n";
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
                    "    448,\n" +
                    "    1924145348609\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 2\n" +
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
                    "  # length: 8, used: 2\n" +
                    "  392,\n" +
                    "  448,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 392, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "# position: 448, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n" +
                    "# 130540 bytes remaining\n";
            assertEquals(expectedEager, queue.dump());
        }
    }

    @Test
    public void testWritingIndex() {
        @NotNull File dir = getTmpDir();
        try (@NotNull ChronicleQueue queue = SingleChronicleQueueBuilder.single(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build()) {
            @NotNull final ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("msg-1");
            String expectedEager = "--- !!meta-data #binary\n" +
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
                    "msg-1\n" +
                    "...\n" +
                    "# 130664 bytes remaining\n";
            assertEquals(expectedEager, queue.dump());
            for (int i = 1; i <= 16; i++)
                appender.writeText("msg-" + i);
            String expectedEager2 = "--- !!meta-data #binary\n" +
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
                    "    676,\n" +
                    "    2903397892112\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 17\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 196, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  296,\n" +
                    "  500,\n" +
                    "  688,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 296, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  392,\n" +
                    "  404,\n" +
                    "  416,\n" +
                    "  428,\n" +
                    "  440,\n" +
                    "  452,\n" +
                    "  464,\n" +
                    "  476\n" +
                    "]\n" +
                    "# position: 392, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg-1\n" +
                    "# position: 404, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg-1\n" +
                    "# position: 416, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg-2\n" +
                    "# position: 428, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg-3\n" +
                    "# position: 440, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg-4\n" +
                    "# position: 452, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg-5\n" +
                    "# position: 464, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg-6\n" +
                    "# position: 476, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg-7\n" +
                    "# position: 488, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg-8\n" +
                    "# position: 500, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  488,\n" +
                    "  592,\n" +
                    "  604,\n" +
                    "  616,\n" +
                    "  628,\n" +
                    "  640,\n" +
                    "  652,\n" +
                    "  664\n" +
                    "]\n" +
                    "# position: 592, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg-9\n" +
                    "# position: 604, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg-10\n" +
                    "# position: 616, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg-11\n" +
                    "# position: 628, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg-12\n" +
                    "# position: 640, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg-13\n" +
                    "# position: 652, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg-14\n" +
                    "# position: 664, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg-15\n" +
                    "# position: 676, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg-16\n" +
                    "# position: 688, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  676,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 130284 bytes remaining\n";
            assertEquals(expectedEager2, queue.dump());
        }
    }

    @SuppressWarnings("unused")
    private static class MyData extends SelfDescribingMarshallable {
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
