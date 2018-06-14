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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
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

    private final boolean lazyIndexing =false;
    private int appendMode;
    private ThreadDump threadDump;




    private static void assertHexEquals(long a, long b) {
        if (a != b)
            assertEquals(Long.toHexString(a) + " != " + Long.toHexString(b), a, b);
    }

    private static void expected(@NotNull ExcerptTailer tailer, String expected) {
        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue(dc.isPresent());
            Bytes bytes2 = Bytes.allocateDirect(128);
            dc.wire().copyTo(new TextWire(bytes2));
            assertEquals(expected, bytes2.toString());
        }
    }

    @Test
    public void testMyData() {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        ClassAliasPool.CLASS_ALIASES.addAlias(MyData.class);

        try (@NotNull SingleChronicleQueue queue = binary(dir)
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE).build()) {
            @NotNull ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
            try (DocumentContext dc = appender.writingDocument()) {
                @NotNull MyData name = new MyData("name", 12345, 1.2, 111);
                System.out.println(name);
                name.writeMarshallable(dc.wire());

                @NotNull MyData name2 = new MyData("name2", 12346, 1.3, 112);
                System.out.println(name2);
                name2.writeMarshallable(dc.wire());
            }
            String dump = queue.dump();
            assertTrue(dump, lazyIndexing != dump.contains("--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  640,\n" +
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
                        "    640,\n" +
                        "    2748779069440\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 1\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  544,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 544, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  640,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 640, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130408 bytes remaining\n";
                String expectedLazy = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    442,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 462,\n" +
                        "    lastIndex: 0\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 462, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  560,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 560, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 0\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "...\n" +
                        "# 130412 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), lazyIndexing ? expectedLazy : expectedEager);

                appendMessage(queue, start + 1, "Another Hello World");
                @NotNull String expectedEager2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    660,\n" +
                        "    2834678415361\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 2\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  544,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 544, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  640,\n" +
                        "  660,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 640, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 660, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "...\n" +
                        "# 130380 bytes remaining\n";
                String expectedLazy2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    656,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 462,\n" +
                        "    lastIndex: 0\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 462, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  560,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 560, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 0\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 656, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "...\n" +
                        "# 130384 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), lazyIndexing ? expectedLazy2 : expectedEager2);

                appendMessage(queue, start + 2, "Bye for now");

                @NotNull String expectedEager3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    688,\n" +
                        "    2954937499650\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 3\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  544,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 544, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 3\n" +
                        "  640,\n" +
                        "  660,\n" +
                        "  688,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 640, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 660, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "# position: 688, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 130360 bytes remaining\n";
                String expectedLazy3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    684,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 462,\n" +
                        "    lastIndex: 0\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 462, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  560,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 560, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 0\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 656, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World\n" +
                        "# position: 684, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 130364 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir), lazyIndexing ? expectedLazy3 : expectedEager3);
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
                        "    640,\n" +
                        "    2748779069440\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 1\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  544,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 544, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  640,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 640, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "...\n" +
                        "# 130408 bytes remaining\n";
                String expectedLazy = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    442,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 462,\n" +
                        "    lastIndex: 0\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 462, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  560,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 560, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 0\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "...\n" +
                        "# 130412 bytes remaining\n";
                checkFileContents(getFirstQueueFile(dir),
                        (lazyIndexing ? expectedLazy : expectedEager)
                                .replace("indexSpacing: 1", "indexSpacing: " + spacing)
                                .replace("lastIndex: 1", "lastIndex: " + spacing));

                assertTrue(tailer.moveToIndex(start));
                for (int i = 1; i < 19; i++) {
                    assertFalse(tailer.moveToIndex(start + i));
                    appendMessage(queue, start + i, "Another Hello World " + (i + 1));
                    assertTrue(tailer.moveToIndex(start + i));
//                System.out.println(queue.dump());
                }
                assertFalse(tailer.moveToIndex(start + 19));
                appendMessage(queue, start + 19, "Bye for now");
                assertTrue(tailer.moveToIndex(start + 19));
                assertFalse(tailer.moveToIndex(start + 20));

                @NotNull String expected1 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1408,\n" +
                        "    6047313952787\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 3\n" +
                        "  544,\n" +
                        "  900,\n" +
                        "  1247,\n" +
                        "  0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 544, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  640,\n" +
                        "  660,\n" +
                        "  690,\n" +
                        "  720,\n" +
                        "  750,\n" +
                        "  780,\n" +
                        "  810,\n" +
                        "  840\n" +
                        "]\n" +
                        "# position: 640, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 660, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 690, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 720, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 750, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 780, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 810, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 840, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 870, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 900, header: 8\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  870,\n" +
                        "  992,\n" +
                        "  1024,\n" +
                        "  1055,\n" +
                        "  1088,\n" +
                        "  1119,\n" +
                        "  1152,\n" +
                        "  1183\n" +
                        "]\n" +
                        "# position: 992, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 1024, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 1055, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 1088, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 1119, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 1152, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1183, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1216, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1247, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 4\n" +
                        "  1216,\n" +
                        "  1344,\n" +
                        "  1375,\n" +
                        "  1408,\n" +
                        "  0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1344, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1375, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1408, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129640 bytes remaining\n";
                @NotNull String expected2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1311,\n" +
                        "    5630702125075\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 2,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  544,\n" +
                        "  1152,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 544, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 8\n" +
                        "  640,\n" +
                        "  690,\n" +
                        "  750,\n" +
                        "  810,\n" +
                        "  870,\n" +
                        "  931,\n" +
                        "  993,\n" +
                        "  1055\n" +
                        "]\n" +
                        "# position: 640, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 660, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 690, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 720, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 750, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 780, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 810, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 840, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 870, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 900, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 931, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 962, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 993, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 1024, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 1055, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1088, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1119, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1152, header: 16\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 2\n" +
                        "  1119,\n" +
                        "  1280,\n" +
                        "  0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 1248, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1280, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1311, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129737 bytes remaining\n";
                @NotNull String expected3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1216,\n" +
                        "    5222680231955\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 4,\n" +
                        "    index2Index: 442,\n" +
                        "    lastIndex: 20\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  544,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 544, header: -1\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 5\n" +
                        "  640,\n" +
                        "  750,\n" +
                        "  870,\n" +
                        "  993,\n" +
                        "  1119,\n" +
                        "  0, 0, 0\n" +
                        "]\n" +
                        "# position: 640, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 660, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 690, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 720, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 750, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 780, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 810, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 840, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 870, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 900, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 931, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 962, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 993, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 1024, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 1055, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1088, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1119, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1152, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1183, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1216, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129832 bytes remaining\n";
                String lazy1 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1216,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 1,\n" +
                        "    index2Index: 462,\n" +
                        "    lastIndex: 0\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 462, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  560,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 560, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 0\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 656, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 686, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 716, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 746, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 776, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 806, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 836, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 866, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 896, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 927, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 960, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 991, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 1024, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 1055, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1088, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1119, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1152, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1183, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1216, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129832 bytes remaining\n";
                String lazy2 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1216,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 2,\n" +
                        "    index2Index: 462,\n" +
                        "    lastIndex: 0\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 462, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  560,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 560, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 0\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 656, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 686, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 716, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 746, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 776, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 806, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 836, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 866, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 896, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 927, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 960, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 991, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 1024, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 1055, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1088, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1119, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1152, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1183, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1216, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129832 bytes remaining\n";
                String lazy3 = "--- !!meta-data #binary\n" +
                        "header: !SCQStore {\n" +
                        "  wireType: !WireType BINARY_LIGHT,\n" +
                        "  writePosition: [\n" +
                        "    1216,\n" +
                        "    0\n" +
                        "  ],\n" +
                        "  roll: !SCQSRoll {\n" +
                        "    length: !int 86400000,\n" +
                        "    format: yyyyMMdd,\n" +
                        "    epoch: 0\n" +
                        "  },\n" +
                        "  indexing: !SCQSIndexing {\n" +
                        "    indexCount: 8,\n" +
                        "    indexSpacing: 4,\n" +
                        "    index2Index: 462,\n" +
                        "    lastIndex: 0\n" +
                        "  },\n" +
                        "  lastAcknowledgedIndexReplicated: -1,\n" +
                        "  recovery: !TimedStoreRecovery {\n" +
                        "    timeStamp: 0\n" +
                        "  },\n" +
                        "  deltaCheckpointInterval: 0,\n" +
                        "  lastIndexReplicated: -1,\n" +
                        "  sourceId: 0\n" +
                        "}\n" +
                        "# position: 442, header: 0\n" +
                        "--- !!data #binary\n" +
                        "msg: Hello World\n" +
                        "# position: 462, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index2index: [\n" +
                        "  # length: 8, used: 1\n" +
                        "  560,\n" +
                        "  0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 560, header: 0\n" +
                        "--- !!meta-data #binary\n" +
                        "index: [\n" +
                        "  # length: 8, used: 0\n" +
                        "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                        "]\n" +
                        "# position: 656, header: 1\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 2\n" +
                        "# position: 686, header: 2\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 3\n" +
                        "# position: 716, header: 3\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 4\n" +
                        "# position: 746, header: 4\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 5\n" +
                        "# position: 776, header: 5\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 6\n" +
                        "# position: 806, header: 6\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 7\n" +
                        "# position: 836, header: 7\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 8\n" +
                        "# position: 866, header: 8\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 9\n" +
                        "# position: 896, header: 9\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 10\n" +
                        "# position: 927, header: 10\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 11\n" +
                        "# position: 960, header: 11\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 12\n" +
                        "# position: 991, header: 12\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 13\n" +
                        "# position: 1024, header: 13\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 14\n" +
                        "# position: 1055, header: 14\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 15\n" +
                        "# position: 1088, header: 15\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 16\n" +
                        "# position: 1119, header: 16\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 17\n" +
                        "# position: 1152, header: 17\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 18\n" +
                        "# position: 1183, header: 18\n" +
                        "--- !!data #binary\n" +
                        "msg: Another Hello World 19\n" +
                        "# position: 1216, header: 19\n" +
                        "--- !!data #binary\n" +
                        "msg: Bye for now\n" +
                        "...\n" +
                        "# 129832 bytes remaining\n";
                assertNotEquals(lazy1, expected1);
                assertNotEquals(lazy2, expected2);
                assertNotEquals(lazy3, expected3);
                @NotNull String expected =
                        lazyIndexing ? (
                                spacing == 1 ? lazy1 :
                                        spacing == 2 ? lazy2 : lazy3)
                                : (
                                spacing == 1 ? expected1 :
                                        spacing == 2 ? expected2 : expected3);

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
        @NotNull ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
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

    public void readTwo(@NotNull ExcerptTailer tailer) {
        long start = RollCycles.DAILY.toIndex(1, 0L);
        assertEquals(start, tailer.index());
        expected(tailer, "msg: Hello world\n");
        assertEquals(start + 1, tailer.index());
        expected(tailer, "msg: Also hello world\n");
        assertEquals(start + 2, tailer.index());
        try (DocumentContext dc = tailer.readingDocument()) {
            assertFalse(dc.isPresent());
        }
        assertEquals(start + 2, tailer.index());
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
            ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
            appender.writeMap(map);

            map.put("abc", "aye-bee-see");
            appender.writeMap(map);

            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    700,\n" +
                    "    3006477107201\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  700,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 700, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 130300 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    502,\n" +
                    "    0\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 502, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 130498 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, queue.dump());

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
            @NotNull ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
            appender.writeDocument(new Order("Symbol", Side.Buy, 1.2345, 1e6));
            appender.writeDocument(w -> w.write("newOrder").object(new Order("Symbol2", Side.Sell, 2.999, 10e6)));
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    695,\n" +
                    "    2985002270721\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  640,\n" +
                    "  695,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "# position: 695, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n" +
                    "# 130294 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    497,\n" +
                    "    0\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "# position: 497, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n" +
                    "# 130492 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, queue.dump());
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
            @NotNull final ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
            appender.writeText("msg-1");
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    640,\n" +
                    "    2748779069440\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  640,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "...\n" +
                    "# 130419 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    442,\n" +
                    "    0\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "...\n" +
                    "# 130617 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, queue.dump());
            for (int i = 1; i <= 16; i++)
                appender.writeText("msg-" + i);
            String expectedEager2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    885,\n" +
                    "    3801046056976\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 17\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  544,\n" +
                    "  721,\n" +
                    "  895,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  640,\n" +
                    "  649,\n" +
                    "  658,\n" +
                    "  667,\n" +
                    "  676,\n" +
                    "  685,\n" +
                    "  694,\n" +
                    "  703\n" +
                    "]\n" +
                    "# position: 640, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 649, header: 1\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 658, header: 2\n" +
                    "--- !!data\n" +
                    "msg-2\n" +
                    "# position: 667, header: 3\n" +
                    "--- !!data\n" +
                    "msg-3\n" +
                    "# position: 676, header: 4\n" +
                    "--- !!data\n" +
                    "msg-4\n" +
                    "# position: 685, header: 5\n" +
                    "--- !!data\n" +
                    "msg-5\n" +
                    "# position: 694, header: 6\n" +
                    "--- !!data\n" +
                    "msg-6\n" +
                    "# position: 703, header: 7\n" +
                    "--- !!data\n" +
                    "msg-7\n" +
                    "# position: 712, header: 8\n" +
                    "--- !!data\n" +
                    "msg-8\n" +
                    "# position: 721, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  712,\n" +
                    "  816,\n" +
                    "  825,\n" +
                    "  835,\n" +
                    "  845,\n" +
                    "  855,\n" +
                    "  865,\n" +
                    "  875\n" +
                    "]\n" +
                    "# position: 816, header: 9\n" +
                    "--- !!data\n" +
                    "msg-9\n" +
                    "# position: 825, header: 10\n" +
                    "--- !!data\n" +
                    "msg-10\n" +
                    "# position: 835, header: 11\n" +
                    "--- !!data\n" +
                    "msg-11\n" +
                    "# position: 845, header: 12\n" +
                    "--- !!data\n" +
                    "msg-12\n" +
                    "# position: 855, header: 13\n" +
                    "--- !!data\n" +
                    "msg-13\n" +
                    "# position: 865, header: 14\n" +
                    "--- !!data\n" +
                    "msg-14\n" +
                    "# position: 875, header: 15\n" +
                    "--- !!data\n" +
                    "msg-15\n" +
                    "# position: 885, header: 16\n" +
                    "--- !!data\n" +
                    "msg-16\n" +
                    "# position: 895, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  885,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 130076 bytes remaining\n";
            String expectedLazy2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    592,\n" +
                    "    0\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  lastIndexReplicated: -1,\n" +
                    "  sourceId: 0\n" +
                    "}\n" +
                    "# position: 442, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 451, header: 1\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 460, header: 2\n" +
                    "--- !!data\n" +
                    "msg-2\n" +
                    "# position: 469, header: 3\n" +
                    "--- !!data\n" +
                    "msg-3\n" +
                    "# position: 478, header: 4\n" +
                    "--- !!data\n" +
                    "msg-4\n" +
                    "# position: 487, header: 5\n" +
                    "--- !!data\n" +
                    "msg-5\n" +
                    "# position: 496, header: 6\n" +
                    "--- !!data\n" +
                    "msg-6\n" +
                    "# position: 505, header: 7\n" +
                    "--- !!data\n" +
                    "msg-7\n" +
                    "# position: 514, header: 8\n" +
                    "--- !!data\n" +
                    "msg-8\n" +
                    "# position: 523, header: 9\n" +
                    "--- !!data\n" +
                    "msg-9\n" +
                    "# position: 532, header: 10\n" +
                    "--- !!data\n" +
                    "msg-10\n" +
                    "# position: 542, header: 11\n" +
                    "--- !!data\n" +
                    "msg-11\n" +
                    "# position: 552, header: 12\n" +
                    "--- !!data\n" +
                    "msg-12\n" +
                    "# position: 562, header: 13\n" +
                    "--- !!data\n" +
                    "msg-13\n" +
                    "# position: 572, header: 14\n" +
                    "--- !!data\n" +
                    "msg-14\n" +
                    "# position: 582, header: 15\n" +
                    "--- !!data\n" +
                    "msg-15\n" +
                    "# position: 592, header: 16\n" +
                    "--- !!data\n" +
                    "msg-16\n" +
                    "...\n" +
                    "# 130466 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy2 : expectedEager2, queue.dump());
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
