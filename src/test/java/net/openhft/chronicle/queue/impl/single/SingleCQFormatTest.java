/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.HexDumpBytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.util.QueueUtil;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.*;

public class SingleCQFormatTest extends ChronicleQueueTestBase {
    static {
        SingleChronicleQueueBuilder.addAliases();
    }

    @Test
    public void testEmptyDirectory() {
        final File dir = new File(OS.getTarget(), getClass().getSimpleName() + "-" + Time.uniqueId());
        dir.mkdir();
        try (RollingChronicleQueue queue = binary(dir).testBlockSize().build()) {
            assertEquals(Integer.MAX_VALUE, queue.firstCycle());
            assertEquals(Long.MAX_VALUE, queue.firstIndex());
            assertEquals(Integer.MIN_VALUE, queue.lastCycle());
        }

        IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
    }

    @Test
    public void testInvalidFile() throws FileNotFoundException {
        // based on the file name
        expectException("Overriding roll cycle from TEST4_DAILY to DAILY");

        final File dir = new File(OS.getTarget() + "/deleteme-" + Time.uniqueId());
        dir.mkdir();

        try (MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700102" + SingleChronicleQueue.SUFFIX), 64 << 10)) {
            bytes.write8bit("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\" ?>");

            try (RollingChronicleQueue queue = binary(dir)
                    .rollCycle(RollCycles.TEST4_DAILY)
                    .testBlockSize()
                    .build()) {
                assertEquals(1, queue.firstCycle());
                assertEquals(1, queue.lastCycle());
                try {
                    final ExcerptTailer tailer = queue.createTailer();
                    tailer.toEnd();
                    fail();
                } catch (Exception e) {
                    assertEquals("java.io.StreamCorruptedException: Unexpected magic number 783f3c37",
                            e.toString());
                }
            }
        }
        System.gc();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testNoHeader() throws IOException {
        final File dir = new File(OS.getTarget() + "/deleteme-" + Time.uniqueId());
        dir.mkdir();

        final File file = new File(dir, "19700101" + SingleChronicleQueue.SUFFIX);
        try (FileOutputStream fos = new FileOutputStream(file)) {
            byte[] bytes = new byte[1024];
            for (int i = 0; i < 128; i++) {
                fos.write(bytes);
            }
        }

        try (ChronicleQueue queue = binary(dir)
                .rollCycle(RollCycles.DAILY)
                .timeoutMS(500L)
                .testBlockSize()
                .build()) {
            testQueue(queue);
        } finally {
            try {
                IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testDeadHeader() throws IOException {
        final File dir = getTmpDir();

        dir.mkdirs();
        final File file = new File(dir, "19700101" + SingleChronicleQueue.SUFFIX);
        file.createNewFile();
        final MappedBytes bytes = MappedBytes.mappedBytes(file, QueueUtil.testBlockSize());
        try {
            bytes.writeInt(Wires.NOT_COMPLETE | Wires.META_DATA);
        } finally {
            bytes.releaseLast();
        }

        try (ChronicleQueue queue = binary(dir).timeoutMS(500L)
                .testBlockSize()
                .blockSize(QueueUtil.testBlockSize())
                .build()) {
            testQueue(queue);
        } finally {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        }

    }

    private void testQueue(@NotNull final ChronicleQueue queue) {
        try (ExcerptTailer tailer = queue.createTailer();
             DocumentContext dc = tailer.readingDocument()) {
            assertFalse(dc.isPresent());
        }
    }

    // "see https://github.com/OpenHFT/Chronicle-Queue/issues/719")
    @Test
    public void testCompleteHeader() throws FileNotFoundException {
        expectException("reading control code as text");
        expectException("closable tracing disabled");

        // too many hacks are required to make the (artificial) code below release resources correctly
        AbstractCloseable.disableCloseableTracing();

        final File dir = getTmpDir();
        dir.mkdirs();

        final File file = new File(dir, "19700101T4" + SingleChronicleQueue.SUFFIX);


        testWritingTo(new HexDumpBytes());

        try (MappedBytes bytes = MappedBytes.mappedBytes(file, QueueUtil.testBlockSize() * 2L)) {
            testWritingTo(bytes);
        }

        try (ChronicleQueue queue = binary(dir)
                .rollCycle(RollCycles.TEST4_DAILY)
                .testBlockSize()
                .build()) {
            testQueue(queue);
        }

        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testWritingTo(Bytes<?> bytes) {
        try (SCQIndexing marshallable = new SCQIndexing(WireType.BINARY, 32, 4)) {
            Wire wire = new BinaryWire(bytes);
            wire.usePadding(true);
            try (DocumentContext dc = wire.writingDocument(true)) {
                dc.wire().writeEventName("header").typePrefix(SingleChronicleQueueStore.class).marshallable(w -> {
                    w.write("wireType").object(WireType.BINARY);
                    w.write("writePosition").int64forBinding(0);
                    w.write("roll").typedMarshallable(new SCQRoll(RollCycles.TEST4_DAILY, 0, null, null));
                    w.write("indexing").typedMarshallable(marshallable);
                    w.write("lastAcknowledgedIndexReplicated").int64forBinding(0);
                });
            }
        }

        final String expected = "" +
                "00000000 24 01 00 40 b9 06 68 65  61 64 65 72 b6 08 53 43 $··@··he ader··SC\n" +
                "00000010 51 53 74 6f 72 65 82 0d  01 00 00 c8 77 69 72 65 QStore·· ····wire\n" +
                "00000020 54 79 70 65 b6 08 57 69  72 65 54 79 70 65 e6 42 Type··Wi reType·B\n" +
                "00000030 49 4e 41 52 59 cd 77 72  69 74 65 50 6f 73 69 74 INARY·wr itePosit\n" +
                "00000040 69 6f 6e 8f 8f 8f 8f a7  00 00 00 00 00 00 00 00 ion····· ········\n" +
                "00000050 c4 72 6f 6c 6c b6 08 53  43 51 53 52 6f 6c 6c 82 ·roll··S CQSRoll·\n" +
                "00000060 28 00 00 00 c6 6c 65 6e  67 74 68 a6 00 5c 26 05 (····len gth··\\&·\n" +
                "00000070 c6 66 6f 72 6d 61 74 ec  79 79 79 79 4d 4d 64 64 ·format· yyyyMMdd\n" +
                "00000080 27 54 34 27 c5 65 70 6f  63 68 a1 00 c8 69 6e 64 'T4'·epo ch···ind\n" +
                "00000090 65 78 69 6e 67 b6 0c 53  43 51 53 49 6e 64 65 78 exing··S CQSIndex\n" +
                "000000a0 69 6e 67 82 50 00 00 00  ca 69 6e 64 65 78 43 6f ing·P··· ·indexCo\n" +
                "000000b0 75 6e 74 a1 20 cc 69 6e  64 65 78 53 70 61 63 69 unt· ·in dexSpaci\n" +
                "000000c0 6e 67 a1 04 cb 69 6e 64  65 78 32 49 6e 64 65 78 ng···ind ex2Index\n" +
                "000000d0 8e 02 00 00 00 00 00 a7  00 00 00 00 00 00 00 00 ········ ········\n" +
                "000000e0 c9 6c 61 73 74 49 6e 64  65 78 8e 00 00 00 00 a7 ·lastInd ex······\n" +
                "000000f0 00 00 00 00 00 00 00 00  df 6c 61 73 74 41 63 6b ········ ·lastAck\n" +
                "00000100 6e 6f 77 6c 65 64 67 65  64 49 6e 64 65 78 52 65 nowledge dIndexRe\n" +
                "00000110 70 6c 69 63 61 74 65 64  8e 02 00 00 00 00 00 a7 plicated ········\n" +
                "00000120 00 00 00 00 00 00 00 00                          ········         \n";
        final String expectedHexDump = "" +
                "24 01 00 40                                     # msg-length\n" +
                "b9 06 68 65 61 64 65 72                         # header: (event)\n" +
                "b6 08 53 43 51 53 74 6f 72 65                   # SCQStore\n" +
                "82 0d 01 00 00                                  # SingleCQFormatTest$$Lambda\n" +
                "c8 77 69 72 65 54 79 70 65                      # wireType:\n" +
                "b6 08 57 69 72 65 54 79 70 65                   # WireType\n" +
                "e6 42 49 4e 41 52 59                            # BINARY\n" +
                "cd 77 72 69 74 65 50 6f 73 69 74 69 6f 6e       # writePosition:\n" +
                "8f 8f 8f 8f                                     # int64 for binding\n" +
                "a7 00 00 00 00 00 00 00 00                      # 0\n" +
                "c4 72 6f 6c 6c                                  # roll:\n" +
                "b6 08 53 43 51 53 52 6f 6c 6c                   # SCQSRoll\n" +
                "82 28 00 00 00                                  # SCQRoll\n" +
                "c6 6c 65 6e 67 74 68                            # length:\n" +
                "a6 00 5c 26 05                                  # 86400000\n" +
                "c6 66 6f 72 6d 61 74                            # format:\n" +
                "ec 79 79 79 79 4d 4d 64 64 27 54 34 27          # yyyyMMdd'T4'\n" +
                "c5 65 70 6f 63 68                               # epoch:\n" +
                "a1 00                                           # 0\n" +
                "c8 69 6e 64 65 78 69 6e 67                      # indexing:\n" +
                "b6 0c 53 43 51 53 49 6e 64 65 78 69 6e 67       # SCQSIndexing\n" +
                "82 50 00 00 00                                  # SCQIndexing\n" +
                "ca 69 6e 64 65 78 43 6f 75 6e 74                # indexCount:\n" +
                "a1 20                                           # 32\n" +
                "cc 69 6e 64 65 78 53 70 61 63 69 6e 67          # indexSpacing:\n" +
                "a1 04                                           # 4\n" +
                "cb 69 6e 64 65 78 32 49 6e 64 65 78             # index2Index:\n" +
                "                                                # int64 for binding\n" +
                "8e 02 00 00 00 00 00                            # int64 for binding\n" +
                "a7 00 00 00 00 00 00 00 00                      # 0\n" +
                "c9 6c 61 73 74 49 6e 64 65 78                   # lastIndex:\n" +
                "                                                # int64 for binding\n" +
                "8e 00 00 00 00                                  # int64 for binding\n" +
                "a7 00 00 00 00 00 00 00 00                      # 0\n" +
                "df 6c 61 73 74 41 63 6b 6e 6f 77 6c 65 64 67 65 # lastAcknowledgedIndexReplicated:\n" +
                "64 49 6e 64 65 78 52 65 70 6c 69 63 61 74 65 64 # int64 for binding\n" +
                "8e 02 00 00 00 00 00 a7 00 00 00 00 00 00 00 00 # 0\n";
        assertEquals(bytes instanceof HexDumpBytes
                        ? expectedHexDump
                        : expected,
                bytes.toHexString().replaceAll("Lambda.*", "Lambda"));

        assertEquals("" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  wireType: !WireType BINARY,\n" +
                "  writePosition: 0,\n" +
                "  roll: !SCQSRoll {\n" +
                "    length: 86400000,\n" +
                "    format: yyyyMMdd'T4',\n" +
                "    epoch: 0\n" +
                "  },\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 0,\n" +
                "    lastIndex: 0\n" +
                "  },\n" +
                "  lastAcknowledgedIndexReplicated: 0\n" +
                "}\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));
    }

    @Test
    public void testCompleteHeader2() throws FileNotFoundException {
        final File dir = new File(OS.getTarget(), getClass().getSimpleName() + "-" + Time.uniqueId());
        dir.mkdir();

        final MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101-02" + SingleChronicleQueue.SUFFIX), QueueUtil.testBlockSize() * 2L);

        final Wire wire = new BinaryWire(bytes);
        wire.usePadding(true);
        try (final SingleChronicleQueueStore store = new SingleChronicleQueueStore(RollCycles.HOURLY, WireType.BINARY, bytes, 4 << 10, 4)) {
            try (DocumentContext dc = wire.writingDocument(true)) {
                dc.wire().write("header").typedMarshallable(store);
            }
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    0,\n" +
                    "    0\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: !short 4096,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));
        }

        try (RollingChronicleQueue queue = binary(dir)
                .testBlockSize()
                .rollCycle(RollCycles.HOURLY)
                .build()) {
            testQueue(queue);
            assertEquals(2, queue.firstCycle());
        }

        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testIncompleteHeader() throws FileNotFoundException {
        final File dir = new File(OS.getTarget(), getClass().getSimpleName() + "-" + Time.uniqueId());
        dir.mkdir();

        try (MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101T4" + SingleChronicleQueue.SUFFIX), QueueUtil.testBlockSize())) {
            final Wire wire = new BinaryWire(bytes);
            wire.usePadding(true);
            try (DocumentContext dc = wire.writingDocument(true)) {
                dc.wire().writeEventName("header")
                        .typePrefix(SingleChronicleQueueStore.class).marshallable(
                                w -> w.write("wireType").object(WireType.BINARY));
            }
        }

        try (ChronicleQueue queue = binary(dir)
                .rollCycle(RollCycles.TEST4_DAILY)
                .blockSize(QueueUtil.testBlockSize())
                .build()) {
            testQueue(queue);
            fail();
        } catch (Exception e) {
            // e.printStackTrace();
            assertEquals("net.openhft.chronicle.core.io.IORuntimeException: net.openhft.chronicle.core.io.IORuntimeException: field writePosition required",
                    e.toString());
        }
        System.gc();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}