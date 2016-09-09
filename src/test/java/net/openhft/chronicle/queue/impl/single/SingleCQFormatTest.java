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
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.micros.Order;
import net.openhft.chronicle.queue.micros.Side;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

/**
 * Created by Peter on 05/03/2016.
 */
public class SingleCQFormatTest {

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

    private static void expected(@NotNull ExcerptTailer tailer, String expected) {
        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue(dc.isPresent());
            Bytes bytes2 = Bytes.allocateDirect(128);
            dc.wire().copyTo(new TextWire(bytes2));
            assertEquals(expected, bytes2.toString());
        }
    }

    @Test
    public void testEmptyDirectory() {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();
        @NotNull SingleChronicleQueue queue = binary(dir).build();
        assertEquals(Integer.MAX_VALUE, queue.firstCycle());
        assertEquals(Long.MAX_VALUE, queue.firstIndex());
        assertEquals(Integer.MIN_VALUE, queue.lastCycle());
        queue.close();

        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testInvalidFile() throws FileNotFoundException {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700102" + SingleChronicleQueue.SUFFIX), ChronicleQueue.TEST_BLOCK_SIZE);
        bytes.write8bit("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>");

        @NotNull SingleChronicleQueue queue = binary(dir)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build();
        assertEquals(1, queue.firstCycle());
        assertEquals(1, queue.lastCycle());
        try {
            @NotNull ExcerptTailer tailer = queue.createTailer();
            tailer.toEnd();
            fail();
        } catch (Exception e) {
            assertEquals("java.io.StreamCorruptedException: Unexpected magic number 783f3c37",
                    e.toString());
        }
        queue.close();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testNoHeader() throws FileNotFoundException {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), ChronicleQueue.TEST_BLOCK_SIZE);

        bytes.release();
        @NotNull SingleChronicleQueue queue = binary(dir)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build();

        testQueue(queue);
        queue.close();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO add controls on how to wait, for how long and what action to take to fix it.
    @Test(expected = TimeoutException.class)
    @Ignore("Long running")
    public void testDeadHeader() throws FileNotFoundException {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), ChronicleQueue.TEST_BLOCK_SIZE);
        bytes.writeInt(Wires.NOT_COMPLETE | Wires.META_DATA);
        bytes.release();
        @Nullable SingleChronicleQueue queue = null;
        try {
            queue = binary(dir)
                    .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                    .build();

            testQueue(queue);
        } finally {
            Closeable.closeQuietly(queue);
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        }
    }

    private void testQueue(@NotNull SingleChronicleQueue queue) {
        @NotNull ExcerptTailer tailer = queue.createTailer();
        try (DocumentContext dc = tailer.readingDocument()) {
            assertFalse(dc.isPresent());
        }
    }

    @Test
    public void testCompleteHeader() throws FileNotFoundException {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), ChronicleQueue.TEST_BLOCK_SIZE);
        @NotNull Wire wire = new BinaryWire(bytes);
        try (DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().writeEventName(() -> "header").typePrefix(SingleChronicleQueueStore.class).marshallable(w -> {
                w.write(() -> "wireType").object(WireType.BINARY);
                w.write(() -> "writePosition").int64forBinding(0);
                w.write(() -> "roll").typedMarshallable(new SCQRoll(RollCycles.DAILY, 0));
                w.write(() -> "indexing").typedMarshallable(new SCQIndexing(WireType.BINARY, 32 << 10, 32));
                w.write(() -> "lastAcknowledgedIndexReplicated").int64forBinding(0);
            });
        }

        assertEquals("--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  wireType: !WireType BINARY,\n" +
                "  writePosition: 0,\n" +
                "  roll: !SCQSRoll {\n" +
                "    length: !int 86400000,\n" +
                "    format: yyyyMMdd,\n" +
                "    epoch: 0\n" +
                "  },\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: !int 32768,\n" +
                "    indexSpacing: 32,\n" +
                "    index2Index: 0,\n" +
                "    lastIndex: 0\n" +
                "  },\n" +
                "  lastAcknowledgedIndexReplicated: 0\n" +
                "}\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));
        bytes.release();

        @NotNull SingleChronicleQueue queue = binary(dir)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build();
        testQueue(queue);

        queue.close();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCompleteHeader2() throws FileNotFoundException {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101-02" + SingleChronicleQueue.SUFFIX), ChronicleQueue.TEST_BLOCK_SIZE);
        @NotNull Wire wire = new BinaryWire(bytes);
        try (DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().writeEventName(() -> "header").typedMarshallable(
                    new SingleChronicleQueueStore(RollCycles.HOURLY, WireType.BINARY, bytes, 60 *
                            60 * 1000, 4 << 10, 4, new TimedStoreRecovery(WireType.BINARY), -1));
        }

        assertEquals("--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  wireType: !WireType BINARY,\n" +
                "  writePosition: 0,\n" +
                "  roll: !SCQSRoll {\n" +
                "    length: !int 3600000,\n" +
                "    format: yyyyMMdd-HH,\n" +
                "    epoch: !int 3600000\n" +
                "  },\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: !short 4096,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 0,\n" +
                "    lastIndex: 0\n" +
                "  },\n" +
                "  lastAcknowledgedIndexReplicated: -1,\n" +
                "  recovery: !TimedStoreRecovery {\n" +
                "    timeStamp: 0\n" +
                "  },\n" +
                "  deltaCheckpointInterval: !byte -1\n" +
                "}\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));
        bytes.release();

        @NotNull SingleChronicleQueue queue = binary(dir)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .rollCycle(RollCycles.HOURLY)
                .build();
        testQueue(queue);
        assertEquals(2, queue.firstCycle());
        queue.close();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testIncompleteHeader() throws FileNotFoundException {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        @NotNull MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), ChronicleQueue.TEST_BLOCK_SIZE);
        @NotNull Wire wire = new BinaryWire(bytes);
        try (DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().writeEventName(() -> "header")
                    .typePrefix(SingleChronicleQueueStore.class).marshallable(
                    w -> w.write(() -> "wireType").object(WireType.BINARY));
        }

        bytes.release();
        try (@NotNull SingleChronicleQueue queue = binary(dir)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build()) {
            testQueue(queue);
            fail();
        } catch (Exception e) {
//            e.printStackTrace();
            assertEquals("net.openhft.chronicle.core.io.IORuntimeException: net.openhft.chronicle.core.io.IORuntimeException: field writePosition required",
                    e.toString());
        }
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
            assertTrue(dump, dump.contains("index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  576,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]"));
        }
    }

    @Test
    public void testTwoMessages() throws FileNotFoundException {
        @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();
        @NotNull RollCycles cycle = RollCycles.DAILY;

        {
            @NotNull MappedBytes mappedBytes = MappedBytes.mappedBytes(new File(dir, "19700102" + SingleChronicleQueue.SUFFIX), ChronicleQueue.TEST_BLOCK_SIZE);
            @NotNull Wire wire = new BinaryWire(mappedBytes);
            try (DocumentContext dc = wire.writingDocument(true)) {
                dc.wire().writeEventName(() -> "header").typedMarshallable(
                        new SingleChronicleQueueStore(cycle, WireType.BINARY, mappedBytes, 0,
                                cycle.defaultIndexCount(), cycle.defaultIndexSpacing(), new
                                TimedStoreRecovery(WireType.BINARY), -1));
            }
            try (DocumentContext dc = wire.writingDocument(false)) {
                dc.wire().writeEventName("msg").text("Hello world");
            }
            try (DocumentContext dc = wire.writingDocument(false)) {
                dc.wire().writeEventName("msg").text("Also hello world");
            }

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 0,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !short 16384,\n" +
                    "    indexSpacing: 16,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: !byte -1\n" +
                    "}\n" +
                    "# position: 370, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello world\n" +
                    "# position: 391, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Also hello world\n", Wires.fromSizePrefixedBlobs(mappedBytes
                    .readPosition(0)));
            mappedBytes.release();
        }

        @NotNull SingleChronicleQueue queue = binary(dir)
                .rollCycle(cycle)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build();
        @NotNull ExcerptTailer tailer = queue.createTailer();
        readTwo(tailer);
        tailer.toStart();
        readTwo(tailer);

        // TODO no direction
        tailer.direction(TailerDirection.NONE).toStart();

        long start = queue.firstIndex();
        assertEquals(start, tailer.index());
        expected(tailer, "msg: Hello world\n");
        assertEquals(start, tailer.index());
        expected(tailer, "msg: Hello world\n");

/* TODO FIX.
        assertEquals(start + 1, queue.lastIndex());

        tailer.direction(TailerDirection.BACKWARD).toEnd();

        assertEquals(start + 1, tailer.index());
        expected(tailer, "msg: Also hello world\n");
        assertEquals(start, tailer.index());
        expected(tailer, "msg: Hello world\n");
        assertEquals(start - 1, tailer.index());
*/

        queue.close();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWritingThreeMessages() throws FileNotFoundException {
        for (int m = 0; m <= 2; m++) {
            appendMode = m;

            @NotNull File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
            dir.mkdir();

            @NotNull SingleChronicleQueue queue = binary(dir)
                    .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                    .indexCount(8)
                    .indexSpacing(1)
                    .build();

            long start = RollCycles.DAILY.toIndex(queue.cycle(), 0);
            appendMessage(queue, start, "Hello World");
            @NotNull String expected1 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 576,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  576,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "...\n" +
                    "# 327080 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected1);

            appendMessage(queue, start + 1, "Another Hello World");
            @NotNull String expected2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 596,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  576,\n" +
                    "  596,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 596, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "...\n" +
                    "# 327052 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected2);

            appendMessage(queue, start + 2, "Bye for now");

            @NotNull String expected = "--- !!meta-data #binary\n" +
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
                    "    index2Index: 377,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  576,\n" +
                    "  596,\n" +
                    "  624,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 596, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "# position: 624, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 327032 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected);
        }
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

            @NotNull SingleChronicleQueue queue = binary(dir)
                    .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                    // only do this for testing
                    .indexCount(8)
                    .indexSpacing(spacing)
                    .build();

            long start = RollCycles.DAILY.toIndex(queue.cycle(), 0);
            @NotNull ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.moveToIndex(start));

            appendMessage(queue, start, "Hello World");
            @NotNull String expected00 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 576,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  576,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "...\n" +
                    "# 327080 bytes remaining\n";
            checkFileContents(dir.listFiles()[0],
                    expected00.replace("indexSpacing: 1", "indexSpacing: " + spacing)
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
                    "  writePosition: 1344,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 20\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  480,\n" +
                    "  836,\n" +
                    "  1183,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  576,\n" +
                    "  596,\n" +
                    "  626,\n" +
                    "  656,\n" +
                    "  686,\n" +
                    "  716,\n" +
                    "  746,\n" +
                    "  776\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 596, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 626, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 656, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 686, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 716, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 746, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 776, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 806, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 836, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  806,\n" +
                    "  928,\n" +
                    "  960,\n" +
                    "  991,\n" +
                    "  1024,\n" +
                    "  1055,\n" +
                    "  1088,\n" +
                    "  1119\n" +
                    "]\n" +
                    "# position: 928, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 960, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 991, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 1024, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 1055, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 1088, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1119, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1152, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1183, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 4\n" +
                    "  1152,\n" +
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
                    "# 326312 bytes remaining\n";
            @NotNull String expected2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 1247,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 20\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  480,\n" +
                    "  1088,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  576,\n" +
                    "  626,\n" +
                    "  686,\n" +
                    "  746,\n" +
                    "  806,\n" +
                    "  867,\n" +
                    "  929,\n" +
                    "  991\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 596, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 626, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 656, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 686, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 716, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 746, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 776, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 806, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 836, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 867, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 898, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 929, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 960, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 991, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1024, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1055, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1088, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  1055,\n" +
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
                    "# 326409 bytes remaining\n";
            @NotNull String expected3 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 1152,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 20\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 5\n" +
                    "  576,\n" +
                    "  686,\n" +
                    "  806,\n" +
                    "  929,\n" +
                    "  1055,\n" +
                    "  0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 596, header: 1\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 626, header: 2\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 656, header: 3\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 686, header: 4\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 716, header: 5\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 746, header: 6\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 776, header: 7\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 806, header: 8\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 836, header: 9\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 867, header: 10\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 898, header: 11\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 929, header: 12\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 960, header: 13\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 991, header: 14\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 1024, header: 15\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 1055, header: 16\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1088, header: 17\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1119, header: 18\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1152, header: 19\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 326504 bytes remaining\n";
            @NotNull String expected = spacing == 1 ? expected1 :
                    spacing == 2 ? expected2 : expected3;

            checkFileContents(dir.listFiles()[0], expected);
        }
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
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
        try (@NotNull ChronicleQueue queue = binary(dir).blockSize(ChronicleQueue.TEST_BLOCK_SIZE).build()) {
            ExcerptAppender appender = queue.acquireAppender().lazyIndexing(true);
            appender.writeMap(map);

            map.put("abc", "aye-bee-see");
            appender.writeMap(map);

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 437,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !short 16384,\n" +
                    "    indexSpacing: 16,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: 0\n" +
                    "--- !!data #binary\n" +
                    "abc: def\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "# position: 437, header: 1\n" +
                    "--- !!data #binary\n" +
                    "abc: aye-bee-see\n" +
                    "double: 1.28\n" +
                    "hello: world\n" +
                    "number: 1\n" +
                    "...\n" +
                    "# 654851 bytes remaining\n", queue.dump());

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
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 640,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  576,\n" +
                    "  640,\n" +
                    "  0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "symbol: Symbol\n" +
                    "side: Buy\n" +
                    "limitPrice: 1.2345\n" +
                    "quantity: 1000000.0\n" +
                    "# position: 640, header: 1\n" +
                    "--- !!data #binary\n" +
                    "newOrder: !Order {\n" +
                    "  symbol: Symbol2,\n" +
                    "  side: Sell,\n" +
                    "  limitPrice: 2.999,\n" +
                    "  quantity: 10000000.0\n" +
                    "}\n" +
                    "...\n" +
                    "# 326952 bytes remaining\n", queue.dump());
        }
    }

    @Test
    public void testWritingIndex() {
        @NotNull String dir = OS.TARGET + "/testWritingIndex-" + System.nanoTime();
        try (@NotNull ChronicleQueue queue = ChronicleQueueBuilder.single(dir)
                .rollCycle(RollCycles.TEST_DAILY)
                .blockSize(ChronicleQueue.TEST_BLOCK_SIZE)
                .build()) {
            @NotNull final ExcerptAppender appender = queue.acquireAppender();
            appender.writeText("msg-1");
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 576,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 1\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  576,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "...\n" +
                    "# 327091 bytes remaining\n", queue.dump());
            for (int i = 1; i <= 16; i++)
                appender.writeText("msg-" + i);
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 821,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 17\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  480,\n" +
                    "  657,\n" +
                    "  831,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  576,\n" +
                    "  585,\n" +
                    "  594,\n" +
                    "  603,\n" +
                    "  612,\n" +
                    "  621,\n" +
                    "  630,\n" +
                    "  639\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 585, header: 1\n" +
                    "--- !!data\n" +
                    "msg-1\n" +
                    "# position: 594, header: 2\n" +
                    "--- !!data\n" +
                    "msg-2\n" +
                    "# position: 603, header: 3\n" +
                    "--- !!data\n" +
                    "msg-3\n" +
                    "# position: 612, header: 4\n" +
                    "--- !!data\n" +
                    "msg-4\n" +
                    "# position: 621, header: 5\n" +
                    "--- !!data\n" +
                    "msg-5\n" +
                    "# position: 630, header: 6\n" +
                    "--- !!data\n" +
                    "msg-6\n" +
                    "# position: 639, header: 7\n" +
                    "--- !!data\n" +
                    "msg-7\n" +
                    "# position: 648, header: 8\n" +
                    "--- !!data\n" +
                    "msg-8\n" +
                    "# position: 657, header: 8\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  648,\n" +
                    "  752,\n" +
                    "  761,\n" +
                    "  771,\n" +
                    "  781,\n" +
                    "  791,\n" +
                    "  801,\n" +
                    "  811\n" +
                    "]\n" +
                    "# position: 752, header: 9\n" +
                    "--- !!data\n" +
                    "msg-9\n" +
                    "# position: 761, header: 10\n" +
                    "--- !!data\n" +
                    "msg-10\n" +
                    "# position: 771, header: 11\n" +
                    "--- !!data\n" +
                    "msg-11\n" +
                    "# position: 781, header: 12\n" +
                    "--- !!data\n" +
                    "msg-12\n" +
                    "# position: 791, header: 13\n" +
                    "--- !!data\n" +
                    "msg-13\n" +
                    "# position: 801, header: 14\n" +
                    "--- !!data\n" +
                    "msg-14\n" +
                    "# position: 811, header: 15\n" +
                    "--- !!data\n" +
                    "msg-15\n" +
                    "# position: 821, header: 16\n" +
                    "--- !!data\n" +
                    "msg-16\n" +
                    "# position: 831, header: 16\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  821,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 326748 bytes remaining\n", queue.dump());
        }
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
