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
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.queue.*;
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
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

/*
 * Created by Peter Lawrey on 05/03/2016.
 */
public class SingleCQFormat2Test {

    static {
        // init class
        SingleChronicleQueueBuilder.init();
    }

    private int appendMode;
    private ThreadDump threadDump;

    private static void assertHexEquals(long a, long b) {
        if (a != b)
            assertEquals(Long.toHexString(a) + " != " + Long.toHexString(b), a, b);
    }

    @Test
    public void testMyData() {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        ClassAliasPool.CLASS_ALIASES.addAlias(MyData.class);

        try (@NotNull SingleChronicleQueue queue = binary(dir)
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
                    "  584,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]"));
        }
    }

    @Test
    public void testWritingThreeMessages() throws FileNotFoundException {
        for (int m = 0; m <= 2; m++) {
            appendMode = m;

            @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
            dir.mkdir();

            try (@NotNull SingleChronicleQueue queue = binary(dir)
                    .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                    .indexCount(8)
                    .indexSpacing(1)
                    .build()) {

                long start = RollCycles.DAILY.toIndex(queue.cycle(), 0);
                appendMessage(queue, start, "Hello World");
                @NotNull String expectedEager = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    584,\n" +
                        "    2508260900864\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 386,\n" +
                        "    lastIndex: 1\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 386, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  488,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 488, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  584,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 584, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130464 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager);

                appendMessage(queue, start + 1, "Another Hello World");
                @NotNull String expectedEager2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    604,\n" +
                        "    2594160246785\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 386,\n" +
                        "    lastIndex: 2\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 386, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  488,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 488, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  584,\n" +
                        "  604,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 584, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 604, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "...\n" +
                        "# 130436 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager2);

                appendMessage(queue, start + 2, "Bye for now");

                @NotNull String expectedEager3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    632,\n" +
                        "    2714419331074\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 386,\n" +
                        "    lastIndex: 3\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 386, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  488,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 488, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 3\n" +
                        "  584,\n" +
                        "  604,\n" +
                        "  632,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 584, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 604, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "# position: 632, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 130416 bytes remaining\n";

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
    public void testWritingTwentyMessagesTinyIndex() throws FileNotFoundException, TimeoutException {
        for (int spacing : new int[]{1, 2, 4}) {
            @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
            dir.mkdir();

            try (@NotNull SingleChronicleQueue queue = binary(dir)
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
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    584,\n" +
                        "    2508260900864\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 386,\n" +
                        "    lastIndex: 1\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 386, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  488,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 488, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  584,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 584, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130464 bytes remaining\n";

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
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1344,\n" +
                        "    5772436045843\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 386,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 386, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 3\n" +
                        "  488,\n" +
                        "  844,\n" +
                        "  1184,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 488, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  584,\n" +
                        "  604,\n" +
                        "  634,\n" +
                        "  664,\n" +
                        "  694,\n" +
                        "  724,\n" +
                        "  754,\n" +
                        "  784\n" +
                        "]\n" +
                        "# position: 584, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 604, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 634, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 664, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 694, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 724, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 754, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 784, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 814, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 844, header: 8\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  814,\n" +
                        "  936,\n" +
                        "  967,\n" +
                        "  998,\n" +
                        "  1029,\n" +
                        "  1060,\n" +
                        "  1091,\n" +
                        "  1122\n" +
                        "]\n" +
                        "# position: 936, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 967, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 998, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 1029, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 1060, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 1091, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1122, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1153, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1184, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 4\n" +
                        "  1153,\n" +
                        "  1280,\n" +
                        "  1311,\n" +
                        "  1344,\n" +
                        "  0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1280, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1311, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1344, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129704 bytes remaining\n";
                @NotNull String expected2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1247,\n" +
                        "    5355824218131\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 2,\n" +
                        "    index2Index: 386,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 386, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  488,\n" +
                        "  1092,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 488, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  584,\n" +
                        "  634,\n" +
                        "  694,\n" +
                        "  754,\n" +
                        "  814,\n" +
                        "  875,\n" +
                        "  937,\n" +
                        "  999\n" +
                        "]\n" +
                        "# position: 584, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 604, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 634, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 664, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 694, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 724, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 754, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 784, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 814, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 844, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 875, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 906, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 937, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 968, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 999, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1030, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1061, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1092, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  1061,\n" +
                        "  1216,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1184, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1216, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1247, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129801 bytes remaining\n";
                @NotNull String expected3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1154,\n" +
                        "    4956392259603\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 4,\n" +
                        "    index2Index: 386,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 386, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  488,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 488, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 5\n" +
                        "  584,\n" +
                        "  694,\n" +
                        "  814,\n" +
                        "  937,\n" +
                        "  1061,\n" +
                        "  0, 0, 0\n" +
                        "]\n" +
                        "# position: 584, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 604, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 634, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 664, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 694, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 724, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 754, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 784, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 814, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 844, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 875, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 906, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 937, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 968, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 999, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1030, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1061, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1092, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1123, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1154, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129894 bytes remaining\n";

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

    public void appendMessage(@NotNull SingleChronicleQueue queue, long expectedIndex, String msg) {
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
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
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
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    644,\n" +
                    "    2765958938625\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 386,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 386, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  488,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 488, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  584,\n" +
                    "  644,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 584, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 644, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 130356 bytes remaining\n";
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
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        try (@NotNull ChronicleQueue queue = binary(dir)
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build()) {
            @NotNull ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(new Order("Symbol", Side.Buy, 1.2345, 1e6));
            appender.writeDocument(w -> w.write("newOrder").object(new Order("Symbol2", Side.Sell, 2.999, 10e6)));
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    640,\n" +
                    "    2748779069441\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 386,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 386, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  488,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 488, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  584,\n" +
                    "  640,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 584, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "# position: 640, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n" +
                    "# 130349 bytes remaining\n";
            assertEquals(expectedEager, queue.dump());
        }
    }

    @Test
    public void testWritingIndex() {
        @NotNull String dir = OS.TARGET + "/testWritingIndex-" + System.nanoTime();
        try (@NotNull ChronicleQueue queue = ChronicleQueueBuilder.single(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build()) {
            @NotNull final ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("msg-1");
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    584,\n" +
                    "    2508260900864\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 386,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 386, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  488,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 488, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  584,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 584, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "...\n" +
                    "# 130475 bytes remaining\n";
            assertEquals(expectedEager, queue.dump());
            for (int i = 1; i <= 16; i++)
                appender.writeText("msg-" + i);
            String expectedEager2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    829,\n" +
                    "    3560527888400\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 386,\n" +
                    "    lastIndex: 17\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 386, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  488,\n" +
                    "  665,\n" +
                    "  839,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 488, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  584,\n" +
                    "  593,\n" +
                    "  602,\n" +
                    "  611,\n" +
                    "  620,\n" +
                    "  629,\n" +
                    "  638,\n" +
                    "  647\n" +
                    "]\n" +
                    "# position: 584, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 593, header: 1\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 602, header: 2\n" +
                    "--- !!data\n" +
                    "msg-2\n" +
                    "# position: 611, header: 3\n" +
                    "--- !!data\n" +
                    "msg-3\n" +
                    "# position: 620, header: 4\n" +
                    "--- !!data\n" +
                    "msg-4\n" +
                    "# position: 629, header: 5\n" +
                    "--- !!data\n" +
                    "msg-5\n" +
                    "# position: 638, header: 6\n" +
                    "--- !!data\n" +
                    "msg-6\n" +
                    "# position: 647, header: 7\n" +
                    "--- !!data\n" +
                    "msg-7\n" +
                    "# position: 656, header: 8\n" +
                    "--- !!data\n" +
                    "msg-8\n" +
                    "# position: 665, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  656,\n" +
                    "  760,\n" +
                    "  769,\n" +
                    "  779,\n" +
                    "  789,\n" +
                    "  799,\n" +
                    "  809,\n" +
                    "  819\n" +
                    "]\n" +
                    "# position: 760, header: 9\n" +
                    "--- !!data\n" +
                    "msg-9\n" +
                    "# position: 769, header: 10\n" +
                    "--- !!data\n" +
                    "msg-10\n" +
                    "# position: 779, header: 11\n" +
                    "--- !!data\n" +
                    "msg-11\n" +
                    "# position: 789, header: 12\n" +
                    "--- !!data\n" +
                    "msg-12\n" +
                    "# position: 799, header: 13\n" +
                    "--- !!data\n" +
                    "msg-13\n" +
                    "# position: 809, header: 14\n" +
                    "--- !!data\n" +
                    "msg-14\n" +
                    "# position: 819, header: 15\n" +
                    "--- !!data\n" +
                    "msg-15\n" +
                    "# position: 829, header: 16\n" +
                    "--- !!data\n" +
                    "msg-16\n" +
                    "# position: 839, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  829,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 130132 bytes remaining\n";
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
