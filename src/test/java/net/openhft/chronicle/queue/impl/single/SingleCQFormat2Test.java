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
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.micros.Order;
import net.openhft.chronicle.queue.micros.Side;
import net.openhft.chronicle.queue.util.QueueUtil;
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
                .blockSize(QueueUtil.testBlockSize()).build()) {
            @NotNull ExcerptAppender appender = queue.acquireAppender();
            try (DocumentContext dc = appender.writingDocument()) {
                @NotNull MyData name = new MyData("name", 12345, 1.2, 111);
                // System.out.println(name);
                name.writeMarshallable(dc.wire());

                @NotNull MyData name2 = new MyData("name2", 12346, 1.3, 112);
                // System.out.println(name2);
                name2.writeMarshallable(dc.wire());
            }
            String dump = queue.dump();
            assertTrue(dump, dump.contains("--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  400,\n" +
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
                    .blockSize(QueueUtil.testBlockSize())
                    .indexCount(8)
                    .indexSpacing(1)
                    .build()) {

                long start = RollCycles.DEFAULT.toIndex(queue.cycle(), 0);
                appendMessage(queue, start, "Hello World");
                @NotNull String expectedEager = "" +
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
                        "msg: Hello World\n" +
                        "...\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager);

                appendMessage(queue, start + 1, "Another Hello World");
                @NotNull String expectedEager2 = "" +
                        "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    420,\n" +
                        "    1803886264321\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 200,\n" +
                        "    lastIndex: 2\n" +
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
                        "  # length: 8, used: 2\n" +
                        "  400,\n" +
                        "  420,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "...\n";
                checkFileContents(getFirstQueueFile(dir), expectedEager2);

                appendMessage(queue, start + 2, "Bye for now");

                @NotNull String expectedEager3 = "" +
                        "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  writePosition: [\n" +
                        "    448,\n" +
                        "    1924145348610\n" +
                        "  ],\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 200,\n" +
                        "    lastIndex: 3\n" +
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
                        "  # length: 8, used: 3\n" +
                        "  400,\n" +
                        "  420,\n" +
                        "  448,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n";

                checkFileContents(getFirstQueueFile(dir), expectedEager3);
            }
        }
    }

    private File getFirstQueueFile(final File file) {
        return file.listFiles((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX))[0];
    }

    public void checkFileContents(@NotNull File file, String expected) throws FileNotFoundException {

        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(file, QueueUtil.testBlockSize());
        bytes.readLimit(bytes.realCapacity());
        assertEquals(expected, Wires.fromAlignedSizePrefixedBlobs(bytes).replaceAll("(?m)^#.+$\\n", ""));
        bytes.releaseLast();
    }

    void doTestWritingTwentyMessagesTinyIndex(int spacing, String expected) throws FileNotFoundException {
        @NotNull File dir = getTmpDir();
        dir.mkdir();

        try (@NotNull RollingChronicleQueue queue = binary(dir)
                .blockSize(QueueUtil.testBlockSize())
                // only do this for testing
                .indexCount(8)
                .indexSpacing(spacing)
                .build()) {

            long start = RollCycles.DEFAULT.toIndex(queue.cycle(), 0);
            @NotNull ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.moveToIndex(start));

            appendMessage(queue, start, "Hello World");
            @NotNull String expectedEager = "" +
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
                    "msg: Hello World\n" +
                    "...\n";

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

            checkFileContents(getFirstQueueFile(dir), expected);
        }
    }

    @Test
    public void testWritingTwentyMessagesTinyIndex1() throws FileNotFoundException {
        doTestWritingTwentyMessagesTinyIndex(1, "" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    1184,\n" +
                "    5085241278483\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 8,\n" +
                "    indexSpacing: 1,\n" +
                "    index2Index: 200,\n" +
                "    lastIndex: 20\n" +
                "  },\n" +
                "  dataFormat: 1\n" +
                "}\n" +
                "--- !!meta-data #binary\n" +
                "index2index: [\n" +
                "  # length: 8, used: 3\n" +
                "  304,\n" +
                "  676,\n" +
                "  1024,\n" +
                "  0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 8, used: 8\n" +
                "  400,\n" +
                "  420,\n" +
                "  452,\n" +
                "  484,\n" +
                "  516,\n" +
                "  548,\n" +
                "  580,\n" +
                "  612\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "msg: Hello World\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 2\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 3\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 4\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 5\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 6\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 7\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 8\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 9\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 8, used: 8\n" +
                "  644,\n" +
                "  768,\n" +
                "  800,\n" +
                "  832,\n" +
                "  864,\n" +
                "  896,\n" +
                "  928,\n" +
                "  960\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 10\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 11\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 12\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 13\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 14\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 15\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 16\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 17\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 8, used: 4\n" +
                "  992,\n" +
                "  1120,\n" +
                "  1152,\n" +
                "  1184,\n" +
                "  0, 0, 0, 0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 18\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 19\n" +
                "--- !!data #binary\n" +
                "msg: Bye for now\n" +
                "...\n");
    }

    @Test
    public void testWritingTwentyMessagesTinyIndex2() throws FileNotFoundException {
        doTestWritingTwentyMessagesTinyIndex(2, "" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    1088,\n" +
                "    4672924418067\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 8,\n" +
                "    indexSpacing: 2,\n" +
                "    index2Index: 200,\n" +
                "    lastIndex: 20\n" +
                "  },\n" +
                "  dataFormat: 1\n" +
                "}\n" +
                "--- !!meta-data #binary\n" +
                "index2index: [\n" +
                "  # length: 8, used: 2\n" +
                "  304,\n" +
                "  932,\n" +
                "  0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 8, used: 8\n" +
                "  400,\n" +
                "  452,\n" +
                "  516,\n" +
                "  580,\n" +
                "  644,\n" +
                "  708,\n" +
                "  772,\n" +
                "  836\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "msg: Hello World\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 2\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 3\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 4\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 5\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 6\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 7\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 8\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 9\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 10\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 11\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 12\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 13\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 14\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 15\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 16\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 17\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 8, used: 2\n" +
                "  900,\n" +
                "  1056,\n" +
                "  0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 18\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 19\n" +
                "--- !!data #binary\n" +
                "msg: Bye for now\n" +
                "...\n");
    }

    @Test
    public void testWritingTwentyMessagesTinyIndex4() throws FileNotFoundException {
        doTestWritingTwentyMessagesTinyIndex(4, "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    996,\n" +
                "    4277787426835\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 8,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 200,\n" +
                "    lastIndex: 20\n" +
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
                "  # length: 8, used: 5\n" +
                "  400,\n" +
                "  516,\n" +
                "  644,\n" +
                "  772,\n" +
                "  900,\n" +
                "  0, 0, 0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "msg: Hello World\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 2\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 3\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 4\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 5\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 6\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 7\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 8\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 9\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 10\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 11\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 12\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 13\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 14\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 15\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 16\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 17\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 18\n" +
                "--- !!data #binary\n" +
                "msg: Another Hello World 19\n" +
                "--- !!data #binary\n" +
                "msg: Bye for now\n" +
                "...\n");
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
                Bytes<?> bytes = Bytes.elasticByteBuffer();
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
                .blockSize(QueueUtil.testBlockSize())
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build()) {
            ExcerptAppender appender = queue.acquireAppender();
            appender.writeMap(map);

            map.put("abc", "aye-bee-see");
            appender.writeMap(map);

            String expectedEager = "" +
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
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
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
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    460,\n" +
                    "    1975684956161\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 2\n" +
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
                    "  # length: 8, used: 2\n" +
                    "  400,\n" +
                    "  460,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n";
            assertEquals(expectedEager, queue.dump().replaceAll("(?m)^#.+$\\n", ""));

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
                .blockSize(QueueUtil.testBlockSize())
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build()) {
            @NotNull ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(new Order("Symbol", Side.Buy, 1.2345, 1e6));
            appender.writeDocument(w -> w.write("newOrder").object(new Order("Symbol2", Side.Sell, 2.999, 10e6)));
            String expectedEager = "" +
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
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
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
                    "...\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    456,\n" +
                    "    1958505086977\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 2\n" +
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
                    "  # length: 8, used: 2\n" +
                    "  400,\n" +
                    "  456,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n";
            assertEquals(expectedEager, queue.dump().replaceAll("(?m)^#.+$\\n", ""));
        }
    }

    @Test
    public void testWritingIndex() {
        @NotNull File dir = getTmpDir();
        try (@NotNull ChronicleQueue queue = SingleChronicleQueueBuilder.single(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(QueueUtil.testBlockSize())
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build()) {
            @NotNull final ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("msg-1");
            String expectedEager = "" +
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
                    "listing.highestCycle: 18554\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
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
                    "msg-1\n" +
                    "...\n";
            try (DocumentContext dc = appender.writingDocument()) {
                final Bytes<?> bytes = dc.wire().bytes();
                final String s = bytes.toHexString(0, bytes.writePosition());
                assertEquals("" +
                                "00000000 c1 00 00 40 b9 06 68 65  61 64 65 72 b6 08 53 43 ···@··he ader··SC\n" +
                                "00000010 51 53 74 6f 72 65 82 aa  00 00 00 cd 77 72 69 74 QStore·· ····writ\n" +
                                "00000020 65 50 6f 73 69 74 69 6f  6e 8e 01 00 00 00 00 8d ePositio n·······\n" +
                                "00000030 02 00 00 00 00 00 00 00  02 00 00 00 00 00 00 00 ········ ········\n" +
                                "00000040 90 01 00 00 00 00 00 00  00 00 00 00 90 01 00 00 ········ ········\n" +
                                "00000050 c8 69 6e 64 65 78 69 6e  67 b6 0c 53 43 51 53 49 ·indexin g··SCQSI\n" +
                                "00000060 6e 64 65 78 69 6e 67 82  4c 00 00 00 ca 69 6e 64 ndexing· L····ind\n" +
                                "00000070 65 78 43 6f 75 6e 74 a1  08 cc 69 6e 64 65 78 53 exCount· ··indexS\n" +
                                "00000080 70 61 63 69 6e 67 a1 01  cb 69 6e 64 65 78 32 49 pacing·· ·index2I\n" +
                                "00000090 6e 64 65 78 8f 8f 8f a7  c8 00 00 00 00 00 00 00 ndex···· ········\n" +
                                "000000a0 c9 6c 61 73 74 49 6e 64  65 78 8e 00 00 00 00 a7 ·lastInd ex······\n" +
                                "000000b0 01 00 00 00 00 00 00 00  ca 64 61 74 61 46 6f 72 ········ ·dataFor\n" +
                                "000000c0 6d 61 74 a1 01 00 00 00  64 00 00 40 b9 0b 69 6e mat····· d··@··in\n" +
                                "000000d0 64 65 78 32 69 6e 64 65  78 8e 01 00 00 00 00 8d dex2inde x·······\n" +
                                "000000e0 08 00 00 00 00 00 00 00  01 00 00 00 00 00 00 00 ········ ········\n" +
                                "000000f0 30 01 00 00 00 00 00 00  00 00 00 00 00 00 00 00 0······· ········\n" +
                                "00000100 00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00 ········ ········\n" +
                                "........\n" +
                                "00000130 5c 00 00 40 b9 05 69 6e  64 65 78 8f 8f 8f 8f 8d \\··@··in dex·····\n" +
                                "00000140 08 00 00 00 00 00 00 00  01 00 00 00 00 00 00 00 ········ ········\n" +
                                "00000150 90 01 00 00 00 00 00 00  00 00 00 00 00 00 00 00 ········ ········\n" +
                                "00000160 00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00 ········ ········\n" +
                                "........\n" +
                                "00000190 06 00 00 00 e5 6d 73 67  2d 31 00 00 00 00 00 00 ·····msg -1······\n",
                        s);
                dc.rollbackOnClose();
            }
            assertEquals(expectedEager, queue.dump().replaceAll("(?m)^#.+$\\n", ""));
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
