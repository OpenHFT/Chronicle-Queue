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
@RunWith(Parameterized.class)
public class SingleCQFormat2Test {

    static {
        // init class
        SingleChronicleQueueBuilder.init();
    }

    private final boolean lazyIndexing;
    private int appendMode;
    private ThreadDump threadDump;

    public SingleCQFormat2Test(String testType, boolean lazyIndexing) {
        this.lazyIndexing = lazyIndexing;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"eager", false},
                {"lazy", true}
        });
    }

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
            assertTrue(dump, lazyIndexing != dump.contains("index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  608,\n" +
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
                    "  writePosition: 608,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 2611340115968\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  608,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "...\n" +
                    "# 130440 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 408,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 428,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 428, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 130444 bytes remaining\n";
            checkFileContents(getFirstQueueFile(dir), lazyIndexing ? expectedLazy : expectedEager);

            appendMessage(queue, start + 1, "Another Hello World");
            @NotNull String expectedEager2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 628,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 2697239461889\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  608,\n" +
                    "  628,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 628, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "...\n" +
                    "# 130412 bytes remaining\n";
            String expectedLazy2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 624,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 428,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 428, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 624, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "...\n" +
                    "# 130416 bytes remaining\n";
            checkFileContents(getFirstQueueFile(dir), lazyIndexing ? expectedLazy2 : expectedEager2);

            appendMessage(queue, start + 2, "Bye for now");

            @NotNull String expectedEager3 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 656,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 2817498546178\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  608,\n" +
                    "  628,\n" +
                    "  656,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 628, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "# position: 656, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 130392 bytes remaining\n";
            String expectedLazy3 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 652,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 428,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 428, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 624, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "# position: 652, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 130396 bytes remaining\n";
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
                    "  writePosition: 608,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 2611340115968\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  608,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "...\n" +
                    "# 130440 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 408,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 428,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 428, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 130444 bytes remaining\n";
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
                    "  writePosition: 1375,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 20\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 5905580032019\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  512,\n" +
                    "  868,\n" +
                    "  1216,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  608,\n" +
                    "  628,\n" +
                    "  658,\n" +
                    "  688,\n" +
                    "  718,\n" +
                    "  748,\n" +
                    "  778,\n" +
                    "  808\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 628, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 658, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 688, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 718, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 748, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 778, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 808, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 838, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 868, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  838,\n" +
                    "  960,\n" +
                    "  991,\n" +
                    "  1024,\n" +
                    "  1055,\n" +
                    "  1088,\n" +
                    "  1119,\n" +
                    "  1152\n" +
                    "]\n" +
                    "# position: 960, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 991, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 1024, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 1055, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 1088, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 1119, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1152, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1183, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1216, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 4\n" +
                    "  1183,\n" +
                    "  1312,\n" +
                    "  1344,\n" +
                    "  1375,\n" +
                    "  0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 1312, header: 17\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1344, header: 18\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1375, header: 19\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 129673 bytes remaining\n";
            @NotNull String expected2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 1280,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 20\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n"

                    +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 5497558138899\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  512,\n" +
                    "  1119,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  608,\n" +
                    "  658,\n" +
                    "  718,\n" +
                    "  778,\n" +
                    "  838,\n" +
                    "  899,\n" +
                    "  961,\n" +
                    "  1024\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 628, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 658, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 688, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 718, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 748, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 778, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 808, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 838, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 868, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 899, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 930, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 961, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 992, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 1024, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1055, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1088, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1119, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  1088,\n" +
                    "  1247,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 1216, header: 17\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1247, header: 18\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1280, header: 19\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 129768 bytes remaining\n";
            @NotNull String expected3 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 1183,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 20\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 5080946311187\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 5\n" +
                    "  608,\n" +
                    "  718,\n" +
                    "  838,\n" +
                    "  961,\n" +
                    "  1088,\n" +
                    "  0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 628, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 658, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 688, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 718, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 748, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 778, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 808, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 838, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 868, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 899, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 930, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 961, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 992, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 1024, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1055, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1088, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1119, header: 17\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1152, header: 18\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1183, header: 19\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 129865 bytes remaining\n";
            String lazy1 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 1183,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 428,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 428, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 624, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 654, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 684, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 714, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 744, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 774, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 804, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 834, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 864, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 896, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 927, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 960, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 991, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 1024, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1055, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1088, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1119, header: 17\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1152, header: 18\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1183, header: 19\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 129865 bytes remaining\n";
            String lazy2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 1183,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 428,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 428, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 624, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 654, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 684, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 714, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 744, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 774, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 804, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 834, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 864, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 896, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 927, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 960, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 991, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 1024, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1055, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1088, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1119, header: 17\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1152, header: 18\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1183, header: 19\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 129865 bytes remaining\n";
            String lazy3 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 1183,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 428,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 428, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 624, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 654, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 684, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 714, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 744, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 774, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 804, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 834, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 864, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 896, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 927, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 960, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 991, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 1024, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1055, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1088, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1119, header: 17\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1152, header: 18\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1183, header: 19\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 129865 bytes remaining\n";
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
                    "  writePosition: 668,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 2869038153729\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  608,\n" +
                    "  668,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 668, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 130332 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 468,\n" +
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
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 468, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 130532 bytes remaining\n";
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
                    "  writePosition: 663,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 2847563317249\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  608,\n" +
                    "  663,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "# position: 663, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n" +
                    "# 130326 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 463,\n" +
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
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1E6\n" +
                    "# position: 463, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10E6\n" +
                    "}\n" +
                    "...\n" +
                    "# 130526 bytes remaining\n";
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
                    "  writePosition: 608,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 2611340115968\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  512,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  608,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "...\n" +
                    "# 130451 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 408,\n" +
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
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "...\n" +
                    "# 130651 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, queue.dump());
            for (int i = 1; i <= 16; i++)
                appender.writeText("msg-" + i);
            String expectedEager2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 853,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 408,\n" +
                    "    lastIndex: 17\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0,\n" +
                    "  encodedSequence: 3663607103504\n" +
                    "}\n" +
                    "# position: 408, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  512,\n" +
                    "  689,\n" +
                    "  863,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 512, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  608,\n" +
                    "  617,\n" +
                    "  626,\n" +
                    "  635,\n" +
                    "  644,\n" +
                    "  653,\n" +
                    "  662,\n" +
                    "  671\n" +
                    "]\n" +
                    "# position: 608, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 617, header: 1\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 626, header: 2\n" +
                    "--- !!data\n" +
                    "msg-2\n" +
                    "# position: 635, header: 3\n" +
                    "--- !!data\n" +
                    "msg-3\n" +
                    "# position: 644, header: 4\n" +
                    "--- !!data\n" +
                    "msg-4\n" +
                    "# position: 653, header: 5\n" +
                    "--- !!data\n" +
                    "msg-5\n" +
                    "# position: 662, header: 6\n" +
                    "--- !!data\n" +
                    "msg-6\n" +
                    "# position: 671, header: 7\n" +
                    "--- !!data\n" +
                    "msg-7\n" +
                    "# position: 680, header: 8\n" +
                    "--- !!data\n" +
                    "msg-8\n" +
                    "# position: 689, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  680,\n" +
                    "  784,\n" +
                    "  793,\n" +
                    "  803,\n" +
                    "  813,\n" +
                    "  823,\n" +
                    "  833,\n" +
                    "  843\n" +
                    "]\n" +
                    "# position: 784, header: 9\n" +
                    "--- !!data\n" +
                    "msg-9\n" +
                    "# position: 793, header: 10\n" +
                    "--- !!data\n" +
                    "msg-10\n" +
                    "# position: 803, header: 11\n" +
                    "--- !!data\n" +
                    "msg-11\n" +
                    "# position: 813, header: 12\n" +
                    "--- !!data\n" +
                    "msg-12\n" +
                    "# position: 823, header: 13\n" +
                    "--- !!data\n" +
                    "msg-13\n" +
                    "# position: 833, header: 14\n" +
                    "--- !!data\n" +
                    "msg-14\n" +
                    "# position: 843, header: 15\n" +
                    "--- !!data\n" +
                    "msg-15\n" +
                    "# position: 853, header: 16\n" +
                    "--- !!data\n" +
                    "msg-16\n" +
                    "# position: 863, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  853,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 130108 bytes remaining\n";
            String expectedLazy2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 558,\n" +
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
                    "  encodedSequence: 0\n" +
                    "}\n" +
                    "# position: 408, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 417, header: 1\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 426, header: 2\n" +
                    "--- !!data\n" +
                    "msg-2\n" +
                    "# position: 435, header: 3\n" +
                    "--- !!data\n" +
                    "msg-3\n" +
                    "# position: 444, header: 4\n" +
                    "--- !!data\n" +
                    "msg-4\n" +
                    "# position: 453, header: 5\n" +
                    "--- !!data\n" +
                    "msg-5\n" +
                    "# position: 462, header: 6\n" +
                    "--- !!data\n" +
                    "msg-6\n" +
                    "# position: 471, header: 7\n" +
                    "--- !!data\n" +
                    "msg-7\n" +
                    "# position: 480, header: 8\n" +
                    "--- !!data\n" +
                    "msg-8\n" +
                    "# position: 489, header: 9\n" +
                    "--- !!data\n" +
                    "msg-9\n" +
                    "# position: 498, header: 10\n" +
                    "--- !!data\n" +
                    "msg-10\n" +
                    "# position: 508, header: 11\n" +
                    "--- !!data\n" +
                    "msg-11\n" +
                    "# position: 518, header: 12\n" +
                    "--- !!data\n" +
                    "msg-12\n" +
                    "# position: 528, header: 13\n" +
                    "--- !!data\n" +
                    "msg-13\n" +
                    "# position: 538, header: 14\n" +
                    "--- !!data\n" +
                    "msg-14\n" +
                    "# position: 548, header: 15\n" +
                    "--- !!data\n" +
                    "msg-15\n" +
                    "# position: 558, header: 16\n" +
                    "--- !!data\n" +
                    "msg-16\n" +
                    "...\n" +
                    "# 130500 bytes remaining\n";
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
