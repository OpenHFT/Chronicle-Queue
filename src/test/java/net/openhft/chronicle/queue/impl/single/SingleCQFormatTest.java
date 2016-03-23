/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * Created by Peter on 05/03/2016.
 */
public class SingleCQFormatTest {
    private static final int TEST_CHUNK_SIZE = 4 << 20; // 4 MB

    static {
        // init class
        SingleChronicleQueueBuilder.init();
    }

    int appendMode;

    public static void assertHexEquals(long a, long b) {
        if (a != b)
            assertEquals(Long.toHexString(a) + " != " + Long.toHexString(b), a, b);
    }

    public static void expected(ExcerptTailer tailer, String expected) {
        try (DocumentContext dc = tailer.readingDocument()) {
            assertTrue(dc.isPresent());
            Bytes bytes2 = Bytes.allocateDirect(128);
            dc.wire().copyTo(new TextWire(bytes2));
            assertEquals(expected, bytes2.toString());
        }
    }

    @Test
    public void testEmptyDirectory() {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).build();
        assertEquals(Integer.MAX_VALUE, queue.firstCycle());
        assertEquals(Long.MAX_VALUE, queue.firstIndex());
        assertEquals(Integer.MIN_VALUE, queue.lastCycle());
        assertEquals(Long.MIN_VALUE, queue.lastIndex());
        queue.close();

        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testInvalidFile() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700102" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);
        bytes.write8bit("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>");

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(TEST_CHUNK_SIZE)
                .build();
        assertEquals(1, queue.firstCycle());
        assertEquals(1, queue.lastCycle());
        try {
            ExcerptTailer tailer = queue.createTailer();
            tailer.toEnd();
            fail();
        } catch (Exception e) {
            assertEquals("java.io.StreamCorruptedException: Unexpected magic number 783f3c37",
                    e.toString());
        }
        queue.close();
        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testNoHeader() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);

        bytes.close();
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(TEST_CHUNK_SIZE)
                .build();

        testQueue(queue);
        queue.close();
        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    // TODO add controls on how to wait, for how long and what action to take to fix it.
    @Test(expected = TimeoutException.class)
    @Ignore("Long running")
    public void testDeadHeader() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);
        bytes.writeInt(Wires.NOT_READY | Wires.META_DATA | Wires.UNKNOWN_LENGTH);
        bytes.close();
        SingleChronicleQueue queue = null;
        try {
            queue = SingleChronicleQueueBuilder.binary(dir)
                    .blockSize(TEST_CHUNK_SIZE)
                    .build();

            testQueue(queue);
        } finally {
            Closeable.closeQuietly(queue);
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        }
    }

    public void testQueue(SingleChronicleQueue queue) {
        ExcerptTailer tailer = queue.createTailer();
        try (DocumentContext dc = tailer.readingDocument()) {
            assertFalse(dc.isPresent());
        }
    }

    @Test
    public void testCompleteHeader() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);
        Wire wire = new BinaryWire(bytes);
        try (DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().writeEventName(() -> "header").typePrefix(SingleChronicleQueueStore.class).marshallable(w -> {
                w.write(() -> "wireType").object(WireType.BINARY);
                w.write(() -> "writePosition").int64forBinding(0);
                w.write(() -> "roll").typedMarshallable(new SingleChronicleQueueStore.Roll(RollCycles.DAILY, 0));
                w.write(() -> "indexing").typedMarshallable(new SingleChronicleQueueStore.Indexing(WireType.BINARY, 32 << 10, 32));
            });
        }

        assertEquals("--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  wireType: !WireType BINARY,\n" +
                "  writePosition: 0,\n" +
                "  roll: !SCQSRoll {\n" +
                "    length: 86400000,\n" +
                "    format: yyyyMMdd,\n" +
                "    epoch: 0\n" +
                "  },\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: !int 32768,\n" +
                "    indexSpacing: 32,\n" +
                "    index2Index: 0,\n" +
                "    lastIndex: 0\n" +
                "  }\n" +
                "}\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));
        bytes.close();

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(TEST_CHUNK_SIZE)
                .build();
        testQueue(queue);

        queue.close();
        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testCompleteHeader2() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101-02" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);
        Wire wire = new BinaryWire(bytes);
        try (DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().writeEventName(() -> "header").typedMarshallable(
                    new SingleChronicleQueueStore(RollCycles.HOURLY, WireType.BINARY, bytes, 60 * 60 * 1000, 4 << 10, 4));
        }

        assertEquals("--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  wireType: !WireType BINARY,\n" +
                "  writePosition: 0,\n" +
                "  roll: !SCQSRoll {\n" +
                "    length: 3600000,\n" +
                "    format: yyyyMMdd-HH,\n" +
                "    epoch: 3600000\n" +
                "  },\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: !int 4096,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 0,\n" +
                "    lastIndex: 0\n" +
                "  }\n" +
                "}\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));
        bytes.close();

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(TEST_CHUNK_SIZE)
                .rollCycle(RollCycles.HOURLY)
                .build();
        testQueue(queue);
        assertEquals(2, queue.firstCycle());
        queue.close();
        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testIncompleteHeader() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);
        Wire wire = new BinaryWire(bytes);
        try (DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().writeEventName(() -> "header").typePrefix(SingleChronicleQueueStore.class).marshallable(w -> {
                w.write(() -> "wireType").object(WireType.BINARY);
            });
        }

        bytes.close();
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(TEST_CHUNK_SIZE)
                .build();
        try {
            testQueue(queue);
            fail();
        } catch (Exception e) {
//            e.printStackTrace();
            assertEquals("net.openhft.chronicle.core.io.IORuntimeException: net.openhft.chronicle.core.io.IORuntimeException: field writePosition required",
                    e.toString());
        }
        queue.close();
        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testTwoMessages() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();
        RollCycles cycle = RollCycles.DAILY;

        {
            MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700102" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);
            Wire wire = new BinaryWire(bytes);
            try (DocumentContext dc = wire.writingDocument(true)) {
                dc.wire().writeEventName(() -> "header").typedMarshallable(
                        new SingleChronicleQueueStore(cycle, WireType.BINARY, bytes, 0, cycle.defaultIndexCount(), cycle.defaultIndexSpacing()));
            }
            try (DocumentContext dc = wire.writingDocument(false)) {
                dc.wire().writeEventName(() -> "msg").text("Hello world");
            }
            try (DocumentContext dc = wire.writingDocument(false)) {
                dc.wire().writeEventName(() -> "msg").text("Also hello world");
            }

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 0,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !int 8192,\n" +
                    "    indexSpacing: 64,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 227\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello world\n" +
                    "# position: 248\n" +
                    "--- !!data #binary\n" +
                    "msg: Also hello world\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));
            bytes.close();
        }

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .rollCycle(cycle)
                .blockSize(TEST_CHUNK_SIZE)
                .build();
        ExcerptTailer tailer = queue.createTailer();
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

        tailer.direction(TailerDirection.BACKWARD).toEnd();

        assertEquals(start+1, tailer.index());
        expected(tailer, "msg: Also hello world\n");
        assertEquals(start, tailer.index());
        expected(tailer, "msg: Hello world\n");
        // TODO stuck for now as we cannot roll back further.
        assertEquals(start, tailer.index());

        queue.close();
        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testWritingThreeMessages() throws FileNotFoundException {
        for (int m = 0; m <= 2; m++) {
            appendMode = m;

            File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
            dir.mkdir();

            SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                    .blockSize(TEST_CHUNK_SIZE)
                    .indexCount(8)
                    .indexSpacing(1)
                    .build();

            long start = RollCycles.DAILY.toIndex(queue.cycle(), 0);
            appendMessage(queue, start, "Hello World");
            String expected1 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 435,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 245,\n" +
                    "    lastIndex: 1\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 245\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  343,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 343\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  225,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "...\n" +
                    "# 5242441 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected1);

            appendMessage(queue, start + 1, "Another Hello World");
            String expected2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 463,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 245,\n" +
                    "    lastIndex: 2\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 245\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  343,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 343\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  225,\n" +
                    "  435,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 435\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "...\n" +
                    "# 5242413 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected2);

            appendMessage(queue, start + 2, "Bye for now");

            String expected = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 483,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 245,\n" +
                    "    lastIndex: 3\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 245\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  343,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 343\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  225,\n" +
                    "  435,\n" +
                    "  463,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 435\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "# position: 463\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5242393 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected);
        }
    }

    public void checkFileContents(File file, String expected) throws FileNotFoundException {
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, TEST_CHUNK_SIZE)) {
            bytes.readLimit(bytes.realCapacity());
            assertEquals(expected, Wires.fromSizePrefixedBlobs(bytes));
        }
    }

    @Test
    public void testWritingTwentyMessagesTinyIndex() throws FileNotFoundException, TimeoutException {
        for (int spacing : new int[]{1, 2, 4}) {
            File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
            dir.mkdir();

            SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                    .blockSize(TEST_CHUNK_SIZE)
                    // only do this for testing
                    .indexCount(8)
                    .indexSpacing(spacing)
                    .build();

            long start = RollCycles.DAILY.toIndex(queue.cycle(), 0);
            ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.moveToIndex(start));
            String expected0 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 323,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 0\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "...\n" +
                    "# 5242553 bytes remaining\n";
            checkFileContents(dir.listFiles()[0],
                    expected0.replace("indexSpacing: 1", "indexSpacing: " + spacing));
            appendMessage(queue, start, "Hello World");
            String expected00 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 435,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 1\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  343,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 343\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  323,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "...\n" +
                    "# 5242441 bytes remaining\n";
            checkFileContents(dir.listFiles()[0],
                    expected00.replace("indexSpacing: 1", "indexSpacing: " + spacing));

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

            String expected1 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 1189,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 20\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  343,\n" +
                    "  675,\n" +
                    "  1015,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 343\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  323,\n" +
                    "  435,\n" +
                    "  465,\n" +
                    "  495,\n" +
                    "  525,\n" +
                    "  555,\n" +
                    "  585,\n" +
                    "  615\n" +
                    "]\n" +
                    "# position: 435\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 465\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 495\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 525\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 555\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 585\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 615\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 645\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 675\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  645,\n" +
                    "  767,\n" +
                    "  798,\n" +
                    "  829,\n" +
                    "  860,\n" +
                    "  891,\n" +
                    "  922,\n" +
                    "  953\n" +
                    "]\n" +
                    "# position: 767\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 798\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 829\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 860\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 891\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 922\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 953\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 984\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1015\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 4\n" +
                    "  984,\n" +
                    "  1107,\n" +
                    "  1138,\n" +
                    "  1169,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 1107\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1138\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1169\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5241687 bytes remaining\n";
            String expected2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 1097,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 20\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  343,\n" +
                    "  892,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 343\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 8\n" +
                    "  323,\n" +
                    "  465,\n" +
                    "  525,\n" +
                    "  585,\n" +
                    "  645,\n" +
                    "  706,\n" +
                    "  768,\n" +
                    "  830\n" +
                    "]\n" +
                    "# position: 435\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 465\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 495\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 525\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 555\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 585\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 615\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 645\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 675\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 706\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 737\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 768\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 799\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 830\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 861\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 892\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 2\n" +
                    "  984,\n" +
                    "  1046,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 984\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 1015\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1046\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1077\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5241779 bytes remaining\n";
            String expected4 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 1005,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 225,\n" +
                    "    lastIndex: 20\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 225\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  343,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 323\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 343\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 5\n" +
                    "  323,\n" +
                    "  525,\n" +
                    "  645,\n" +
                    "  768,\n" +
                    "  892,\n" +
                    "  # ^ used ^\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 435\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 465\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 495\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 525\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 555\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 585\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 615\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 645\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 675\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 706\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 737\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 768\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 799\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 830\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 861\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 892\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 923\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 954\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 985\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5241871 bytes remaining\n";
            String expected = spacing == 1 ? expected1 :
                    spacing == 2 ? expected2 : expected4;
            checkFileContents(dir.listFiles()[0], expected);
        }
    }

    @Before
    public void resetAppendMode() {
        appendMode = 0;
    }

    public void appendMessage(SingleChronicleQueue queue, long expectedIndex, String msg) {
        ExcerptAppender appender = queue.createAppender();
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
                    dc.wire().write(() -> "msg").text(msg);
                }
                break;
        }
        if ((expectedIndex & 0xff) == 0x08)
            Thread.yield();
        long index = appender.lastIndexAppended();
        assertHexEquals(expectedIndex, index);
    }

    public void readTwo(ExcerptTailer tailer) {
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
}
