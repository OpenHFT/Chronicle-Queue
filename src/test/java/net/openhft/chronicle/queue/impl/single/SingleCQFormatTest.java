package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
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
    @Test(expected = java.lang.IllegalStateException.class)
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
                w.write(() -> "roll").typedMarshallable(new SingleChronicleQueueStore.Roll(RollCycles.DAILY, 0, WireType.BINARY));
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
/*        tailer.toStart().direction(TailerDirection.NONE);

        long start = 1L << 40;
        assertEquals(start, tailer.index());
        expected(tailer, "msg: Hello world\n");
        assertEquals(start, tailer.index());
        expected(tailer, "msg: Hello world\n");*/

        // TODO backward from the end
/*        tailer.toEnd().direction(TailerDirection.BACKWARD);

        long start = 1L << 40;
        assertEquals(start+2, tailer.index());
        expected(tailer, "msg: Also hello world\n");
        assertEquals(start+1, tailer.index());
        expected(tailer, "msg: Hello world\n");
        assertEquals(start, tailer.index());*/

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
                    "  writePosition: 419,\n" +
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
                    "  335,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 335\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  225,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "...\n" +
                    "# 5242457 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected1);

            appendMessage(queue, start + 1, "Another Hello World");
            String expected2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 447,\n" +
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
                    "  335,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 335\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  225,\n" +
                    "  419,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 419\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "...\n" +
                    "# 5242429 bytes remaining\n";
            checkFileContents(dir.listFiles()[0], expected2);

            appendMessage(queue, start + 2, "Bye for now");

            String expected = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 467,\n" +
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
                    "  335,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 335\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  225,\n" +
                    "  419,\n" +
                    "  447,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 419\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World\n" +
                    "# position: 447\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5242409 bytes remaining\n";
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
            checkFileContents(dir.listFiles()[0],
                    "--- !!meta-data #binary\n" +
                            "header: !SCQStore {\n" +
                            "  wireType: !WireType BINARY,\n" +
                            "  writePosition: 315,\n" +
                            "  roll: !SCQSRoll {\n" +
                            "    length: 86400000,\n" +
                            "    format: yyyyMMdd,\n" +
                            "    epoch: 0\n" +
                            "  },\n" +
                            "  indexing: !SCQSIndexing {\n" +
                            "    indexCount: 8,\n" +
                            "    indexSpacing: " + spacing + ",\n" +
                            "    index2Index: 225,\n" +
                            "    lastIndex: 0\n" +
                            "  }\n" +
                            "}\n" +
                            "# position: 225\n" +
                            "--- !!meta-data #binary\n" +
                            "index2index: [\n" +
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
                            "# 5242561 bytes remaining\n");
            appendMessage(queue, start, "Hello World");
            checkFileContents(dir.listFiles()[0],
                    "--- !!meta-data #binary\n" +
                            "header: !SCQStore {\n" +
                            "  wireType: !WireType BINARY,\n" +
                            "  writePosition: 419,\n" +
                            "  roll: !SCQSRoll {\n" +
                            "    length: 86400000,\n" +
                            "    format: yyyyMMdd,\n" +
                            "    epoch: 0\n" +
                            "  },\n" +
                            "  indexing: !SCQSIndexing {\n" +
                            "    indexCount: 8,\n" +
                            "    indexSpacing: " + spacing + ",\n" +
                            "    index2Index: 225,\n" +
                            "    lastIndex: 1\n" +
                            "  }\n" +
                            "}\n" +
                            "# position: 225\n" +
                            "--- !!meta-data #binary\n" +
                            "index2index: [\n" +
                            "  335,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0\n" +
                            "]\n" +
                            "# position: 315\n" +
                            "--- !!data #binary\n" +
                            "msg: Hello World\n" +
                            "# position: 335\n" +
                            "--- !!meta-data #binary\n" +
                            "index: [\n" +
                            "  315,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0,\n" +
                            "  0\n" +
                            "]\n" +
                            "...\n" +
                            "# 5242457 bytes remaining\n");

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
                    "  writePosition: 1157,\n" +
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
                    "  335,\n" +
                    "  659,\n" +
                    "  991,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 315\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 335\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  315,\n" +
                    "  419,\n" +
                    "  449,\n" +
                    "  479,\n" +
                    "  509,\n" +
                    "  539,\n" +
                    "  569,\n" +
                    "  599\n" +
                    "]\n" +
                    "# position: 419\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 449\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 479\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 509\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 539\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 569\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 599\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 629\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 659\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  629,\n" +
                    "  743,\n" +
                    "  774,\n" +
                    "  805,\n" +
                    "  836,\n" +
                    "  867,\n" +
                    "  898,\n" +
                    "  929\n" +
                    "]\n" +
                    "# position: 743\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 774\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 805\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 836\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 867\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 898\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 929\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 960\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 991\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  960,\n" +
                    "  1075,\n" +
                    "  1106,\n" +
                    "  1137,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 1075\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1106\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1137\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5241719 bytes remaining\n";
            String expected2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 1073,\n" +
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
                    "  335,\n" +
                    "  876,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 315\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 335\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  315,\n" +
                    "  449,\n" +
                    "  509,\n" +
                    "  569,\n" +
                    "  629,\n" +
                    "  690,\n" +
                    "  752,\n" +
                    "  814\n" +
                    "]\n" +
                    "# position: 419\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 449\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 479\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 509\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 539\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 569\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 599\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 629\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 659\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 690\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 721\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 752\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 783\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 814\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 845\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 876\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  960,\n" +
                    "  1022,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 960\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 991\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 1022\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 1053\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5241803 bytes remaining\n";
            String expected4 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY,\n" +
                    "  writePosition: 989,\n" +
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
                    "  335,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 315\n" +
                    "--- !!data #binary\n" +
                    "msg: Hello World\n" +
                    "# position: 335\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  315,\n" +
                    "  509,\n" +
                    "  629,\n" +
                    "  752,\n" +
                    "  876,\n" +
                    "  0,\n" +
                    "  0,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 419\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 2\n" +
                    "# position: 449\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 3\n" +
                    "# position: 479\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 4\n" +
                    "# position: 509\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 5\n" +
                    "# position: 539\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 6\n" +
                    "# position: 569\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 7\n" +
                    "# position: 599\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 8\n" +
                    "# position: 629\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 9\n" +
                    "# position: 659\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 10\n" +
                    "# position: 690\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 11\n" +
                    "# position: 721\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 12\n" +
                    "# position: 752\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 13\n" +
                    "# position: 783\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 14\n" +
                    "# position: 814\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 15\n" +
                    "# position: 845\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 16\n" +
                    "# position: 876\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 17\n" +
                    "# position: 907\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 18\n" +
                    "# position: 938\n" +
                    "--- !!data #binary\n" +
                    "msg: Another Hello World 19\n" +
                    "# position: 969\n" +
                    "--- !!data #binary\n" +
                    "msg: Bye for now\n" +
                    "...\n" +
                    "# 5241887 bytes remaining\n";
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
