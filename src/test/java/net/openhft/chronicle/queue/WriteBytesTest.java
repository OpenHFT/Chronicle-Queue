/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;

/*
 * Created by daniel on 16/05/2016.
 */
public class WriteBytesTest {
    final Bytes outgoingBytes = Bytes.elasticByteBuffer();
    private final byte[] incomingMsgBytes = new byte[100];
    private final byte[] outgoingMsgBytes = new byte[100];

    @Test
    public void testWriteBytes() {
        File dir = DirectoryUtils.tempDir("WriteBytesTest");
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            ExcerptTailer tailer = queue.createTailer();

            outgoingMsgBytes[0] = 'A';
            outgoingBytes.write(outgoingMsgBytes);
            postOneMessage(appender);
            fetchOneMessage(tailer, incomingMsgBytes);
            System.out.println(new String(incomingMsgBytes));

            outgoingBytes.clear();

            outgoingMsgBytes[0] = 'A';
            outgoingMsgBytes[1] = 'B';
            outgoingBytes.write(outgoingMsgBytes);

            postOneMessage(appender);
            fetchOneMessage(tailer, incomingMsgBytes);
            System.out.println(new String(incomingMsgBytes));

        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }

    @Ignore("flaky test")
    @Test
    public void testWriteBytesAndDump() {
        File dir = DirectoryUtils.tempDir("WriteBytesTestAndDump");
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .rollCycle(TEST4_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
                byte finalI = (byte) i;
                appender.writeBytes(b ->
                        b.writeLong(finalI * 0x0101010101010101L));
            }

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: [\n" +
                    "    4368,\n" +
                    "    18760417149183\n" +
                    "  ],\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 442,\n" +
                    "    lastIndex: 256\n" +
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
                    "  # length: 32, used: 2\n" +
                    "  736,\n" +
                    "  2572,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 736, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  1024,\n" +
                    "  1072,\n" +
                    "  1120,\n" +
                    "  1168,\n" +
                    "  1216,\n" +
                    "  1264,\n" +
                    "  1312,\n" +
                    "  1360,\n" +
                    "  1408,\n" +
                    "  1456,\n" +
                    "  1504,\n" +
                    "  1552,\n" +
                    "  1600,\n" +
                    "  1648,\n" +
                    "  1696,\n" +
                    "  1744,\n" +
                    "  1792,\n" +
                    "  1840,\n" +
                    "  1888,\n" +
                    "  1936,\n" +
                    "  1984,\n" +
                    "  2032,\n" +
                    "  2080,\n" +
                    "  2128,\n" +
                    "  2176,\n" +
                    "  2224,\n" +
                    "  2272,\n" +
                    "  2320,\n" +
                    "  2368,\n" +
                    "  2416,\n" +
                    "  2464,\n" +
                    "  2512\n" +
                    "]\n" +
                    "# position: 1024, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000400             80 7f 7f 7f  7f 7f 7f 7f                 ···· ····    \n" +
                    "# position: 1036, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000410 81 80 80 80 80 80 80 80                          ········         \n" +
                    "# position: 1048, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000410                                      82 81 81 81              ····\n" +
                    "00000420 81 81 81 81                                      ····             \n" +
                    "# position: 1060, header: 3\n" +
                    "--- !!data #binary\n" +
                    "00000420                          83 82 82 82 82 82 82 82          ········\n" +
                    "# position: 1072, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 1084, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 1096, header: 6\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x86\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 1108, header: 7\n" +
                    "--- !!data #binary\n" +
                    "00000450                          87 86 86 86 86 86 86 86          ········\n" +
                    "# position: 1120, header: 8\n" +
                    "--- !!data #binary\n" +
                    "00000460             88 87 87 87  87 87 87 87                 ···· ····    \n" +
                    "# position: 1132, header: 9\n" +
                    "--- !!data #binary\n" +
                    "00000470 89 88 88 88 88 88 88 88                          ········         \n" +
                    "# position: 1144, header: 10\n" +
                    "--- !!data #binary\n" +
                    "\"\\x89\\x89\\x89\\x89\\x89\\x89\\x89\"\n" +
                    "# position: 1156, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "\"\\x8A\\x8A\\x8A\\x8A\\x8A\\x8A\"\n" +
                    "# position: 1168, header: 12\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 1180, header: 13\n" +
                    "--- !!data #binary\n" +
                    "000004a0 8d 8c 8c 8c 8c 8c 8c 8c                          ········         \n" +
                    "# position: 1192, header: 14\n" +
                    "--- !!data #binary\n" +
                    "000004a0                                      8e 8d 8d 8d              ····\n" +
                    "000004b0 8d 8d 8d 8d                                      ····             \n" +
                    "# position: 1204, header: 15\n" +
                    "--- !!data #binary\n" +
                    "000004b0                          8f 8e 8e 8e 8e 8e 8e 8e          ········\n" +
                    "# position: 1216, header: 16\n" +
                    "--- !!data #binary\n" +
                    "-1.4156185439721035E-29\n" +
                    "# position: 1228, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "-5.702071897398123E-29\n" +
                    "# # EndOfFile\n" +
                    "# position: 1240, header: 18\n" +
                    "--- !!data #binary\n" +
                    "-753555055760.82\n" +
                    "# position: 1252, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_2\n" +
                    "-48698841.79\n" +
                    "# position: 1264, header: 20\n" +
                    "--- !!data #binary\n" +
                    "-8422085917.3268\n" +
                    "# position: 1276, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_4\n" +
                    "-541098.2421\n" +
                    "# position: 1288, header: 22\n" +
                    "--- !!data #binary\n" +
                    "-93086212.770454\n" +
                    "# position: 1300, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_6\n" +
                    "-5952.080663\n" +
                    "# position: 1312, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 1324, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 1336, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 1348, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# position: 1360, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# position: 1372, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# position: 1384, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 1396, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 1408, header: 32\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 1420, header: 33\n" +
                    "--- !!data #binary\n" +
                    "!int 160\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# position: 1432, header: 34\n" +
                    "--- !!data #binary\n" +
                    "!int 41377\n" +
                    "!int 161\n" +
                    "!int 161\n" +
                    "!int -1\n" +
                    "# position: 1444, header: 35\n" +
                    "--- !!data #binary\n" +
                    "2728567458\n" +
                    "!int 41634\n" +
                    "# position: 1456, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "2745410467\n" +
                    "# # EndOfFile\n" +
                    "# position: 1468, header: 37\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte -92\n" +
                    "!byte -92\n" +
                    "!byte 0\n" +
                    "# position: 1480, header: 38\n" +
                    "--- !!data #binary\n" +
                    "!int -1515870811\n" +
                    "!short -23131\n" +
                    "# position: 1492, header: 39\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "!int -1499027802\n" +
                    "# # EndOfFile\n" +
                    "# position: 1504, header: 40\n" +
                    "--- !!data #binary\n" +
                    "!int 167\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# position: 1516, header: 41\n" +
                    "--- !!data #binary\n" +
                    "!int 43176\n" +
                    "!int 168\n" +
                    "!int 168\n" +
                    "!int -1\n" +
                    "# position: 1528, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # SET_LOW_INT16\n" +
                    "!int 43433\n" +
                    "!int 43433\n" +
                    "# position: 1540, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1552, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1564, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1576, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1588, header: 47\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1600, header: 48\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1612, header: 49\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1624, header: 50\n" +
                    "--- !!data #binary\n" +
                    "00000650                                      b2 b1 b1 b1              ····\n" +
                    "00000660 b1 b1 b1 b1                                      ····             \n" +
                    "# position: 1636, header: 51\n" +
                    "--- !!data #binary\n" +
                    "00000660                          b3 b2 b2 b2 b2 b2 b2 b2          ········\n" +
                    "# position: 1648, header: 52\n" +
                    "--- !!data #binary\n" +
                    "00000670             b4 b3 b3 b3  b3 b3 b3 b3                 ···· ····    \n" +
                    "# position: 1660, header: 53\n" +
                    "--- !!data #binary\n" +
                    "00000680 b5 b4 b4 b4 b4 b4 b4 b4                          ········         \n" +
                    "# position: 1672, header: 54\n" +
                    "--- !!data #binary\n" +
                    "00000680                                      b6 b5 b5 b5              ····\n" +
                    "00000690 b5 b5 b5 b5                                      ····             \n" +
                    "# position: 1684, header: 55\n" +
                    "--- !!data #binary\n" +
                    "00000690                          b7 b6 b6 b6 b6 b6 b6 b6          ········\n" +
                    "# position: 1696, header: 56\n" +
                    "--- !!data #binary\n" +
                    "000006a0             b8 b7 b7 b7  b7 b7 b7 b7                 ···· ····    \n" +
                    "# position: 1708, header: 57\n" +
                    "--- !!data #binary\n" +
                    "000006b0 b9 b8 b8 b8 b8 b8 b8 b8                          ········         \n" +
                    "# position: 1720, header: 58\n" +
                    "--- !!data #binary\n" +
                    "\"-252662577519802\": \n" +
                    "# position: 1732, header: 59\n" +
                    "--- !!data #binary\n" +
                    "!!null \"\"\n" +
                    "\"-2008556674363\": \n" +
                    "# position: 1744, header: 60\n" +
                    "--- !!data #binary\n" +
                    "000006d0             bc bb bb bb  bb bb bb bb                 ···· ····    \n" +
                    "# position: 1756, header: 61\n" +
                    "--- !!data #binary\n" +
                    "000006e0 bd bc bc bc bc bc bc bc                          ········         \n" +
                    "# position: 1768, header: 62\n" +
                    "--- !!data #binary\n" +
                    "000006e0                                      be bd bd bd              ····\n" +
                    "000006f0 bd bd bd bd                                      ····             \n" +
                    "# position: 1780, header: 63\n" +
                    "--- !!data #binary\n" +
                    "000006f0                          bf be be be be be be be          ········\n" +
                    "# position: 1792, header: 64\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1804, header: 65\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC0\": \"\": \"\": \"\": \"\": \"\": \"\": \n" +
                    "# position: 1816, header: 66\n" +
                    "--- !!data #binary\n" +
                    "00000710                                      c2 c1 c1 c1              ····\n" +
                    "00000720 c1 c1 c1 c1                                      ····             \n" +
                    "# position: 1828, header: 67\n" +
                    "--- !!data #binary\n" +
                    "00000720                          c3 c2 c2 c2 c2 c2 c2 c2          ········\n" +
                    "# position: 1840, header: 68\n" +
                    "--- !!data #binary\n" +
                    "00000730             c4 c3 c3 c3  c3 c3 c3 c3                 ···· ····    \n" +
                    "# position: 1852, header: 69\n" +
                    "--- !!data #binary\n" +
                    "00000740 c5 c4 c4 c4 c4 c4 c4 c4                          ········         \n" +
                    "# position: 1864, header: 70\n" +
                    "--- !!data #binary\n" +
                    "00000740                                      c6 c5 c5 c5              ····\n" +
                    "00000750 c5 c5 c5 c5                                      ····             \n" +
                    "# position: 1876, header: 71\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\": \n" +
                    "# position: 1888, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000760             c8 c7 c7 c7  c7 c7 c7 c7                 ···· ····    \n" +
                    "# position: 1900, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000770 c9 c8 c8 c8 c8 c8 c8 c8                          ········         \n" +
                    "# position: 1912, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000770                                      ca c9 c9 c9              ····\n" +
                    "00000780 c9 c9 c9 c9                                      ····             \n" +
                    "# position: 1924, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000780                          cb ca ca ca ca ca ca ca          ········\n" +
                    "# position: 1936, header: 76\n" +
                    "--- !!data #binary\n" +
                    "00000790             cc cb cb cb  cb cb cb cb                 ···· ····    \n" +
                    "# position: 1948, header: 77\n" +
                    "--- !!data #binary\n" +
                    "000007a0 cd cc cc cc cc cc cc cc                          ········         \n" +
                    "# position: 1960, header: 78\n" +
                    "--- !!data #binary\n" +
                    "000007a0                                      ce cd cd cd              ····\n" +
                    "000007b0 cd cd cd cd                                      ····             \n" +
                    "# position: 1972, header: 79\n" +
                    "--- !!data #binary\n" +
                    "000007b0                          cf ce ce ce ce ce ce ce          ········\n" +
                    "# position: 1984, header: 80\n" +
                    "--- !!data #binary\n" +
                    "000007c0             d0 cf cf cf  cf cf cf cf                 ···· ····    \n" +
                    "# position: 1996, header: 81\n" +
                    "--- !!data #binary\n" +
                    "000007d0 d1 d0 d0 d0 d0 d0 d0 d0                          ········         \n" +
                    "# position: 2008, header: 82\n" +
                    "--- !!data #binary\n" +
                    "000007d0                                      d2 d1 d1 d1              ····\n" +
                    "000007e0 d1 d1 d1 d1                                      ····             \n" +
                    "# position: 2020, header: 83\n" +
                    "--- !!data #binary\n" +
                    "000007e0                          d3 d2 d2 d2 d2 d2 d2 d2          ········\n" +
                    "# position: 2032, header: 84\n" +
                    "--- !!data #binary\n" +
                    "000007f0             d4 d3 d3 d3  d3 d3 d3 d3                 ···· ····    \n" +
                    "# position: 2044, header: 85\n" +
                    "--- !!data #binary\n" +
                    "00000800 d5 d4 d4 d4 d4 d4 d4 d4                          ········         \n" +
                    "# position: 2056, header: 86\n" +
                    "--- !!data #binary\n" +
                    "00000800                                      d6 d5 d5 d5              ····\n" +
                    "00000810 d5 d5 d5 d5                                      ····             \n" +
                    "# position: 2068, header: 87\n" +
                    "--- !!data #binary\n" +
                    "00000810                          d7 d6 d6 d6 d6 d6 d6 d6          ········\n" +
                    "# position: 2080, header: 88\n" +
                    "--- !!data #binary\n" +
                    "00000820             d8 d7 d7 d7  d7 d7 d7 d7                 ···· ····    \n" +
                    "# position: 2092, header: 89\n" +
                    "--- !!data #binary\n" +
                    "00000830 d9 d8 d8 d8 d8 d8 d8 d8                          ········         \n" +
                    "# position: 2104, header: 90\n" +
                    "--- !!data #binary\n" +
                    "00000830                                      da d9 d9 d9              ····\n" +
                    "00000840 d9 d9 d9 d9                                      ····             \n" +
                    "# position: 2116, header: 91\n" +
                    "--- !!data #binary\n" +
                    "00000840                          db da da da da da da da          ········\n" +
                    "# position: 2128, header: 92\n" +
                    "--- !!data #binary\n" +
                    "00000850             dc db db db  db db db db                 ···· ····    \n" +
                    "# position: 2140, header: 93\n" +
                    "--- !!data #binary\n" +
                    "00000860 dd dc dc dc dc dc dc dc                          ········         \n" +
                    "# position: 2152, header: 94\n" +
                    "--- !!data #binary\n" +
                    "00000860                                      de dd dd dd              ····\n" +
                    "00000870 dd dd dd dd                                      ····             \n" +
                    "# position: 2164, header: 95\n" +
                    "--- !!data #binary\n" +
                    "00000870                          df de de de de de de de          ········\n" +
                    "# position: 2176, header: 96\n" +
                    "--- !!data #binary\n" +
                    "00000880             e0 df df df  df df df df                 ···· ····    \n" +
                    "# position: 2188, header: 97\n" +
                    "--- !!data #binary\n" +
                    "00000890 e1 e0 e0 e0 e0 e0 e0 e0                          ········         \n" +
                    "# position: 2200, header: 98\n" +
                    "--- !!data #binary\n" +
                    "00000890                                      e2 e1 e1 e1              ····\n" +
                    "000008a0 e1 e1 e1 e1                                      ····             \n" +
                    "# position: 2212, header: 99\n" +
                    "--- !!data #binary\n" +
                    "000008a0                          e3 e2 e2 e2 e2 e2 e2 e2          ········\n" +
                    "# position: 2224, header: 100\n" +
                    "--- !!data #binary\n" +
                    "000008b0             e4 e3 e3 e3  e3 e3 e3 e3                 ···· ····    \n" +
                    "# position: 2236, header: 101\n" +
                    "--- !!data #binary\n" +
                    "000008c0 e5 e4 e4 e4 e4 e4 e4 e4                          ········         \n" +
                    "# position: 2248, header: 102\n" +
                    "--- !!data #binary\n" +
                    "000008c0                                      e6 e5 e5 e5              ····\n" +
                    "000008d0 e5 e5 e5 e5                                      ····             \n" +
                    "# position: 2260, header: 103\n" +
                    "--- !!data #binary\n" +
                    "000008d0                          e7 e6 e6 e6 e6 e6 e6 e6          ········\n" +
                    "# position: 2272, header: 104\n" +
                    "--- !!data #binary\n" +
                    "000008e0             e8 e7 e7 e7  e7 e7 e7 e7                 ···· ····    \n" +
                    "# position: 2284, header: 105\n" +
                    "--- !!data #binary\n" +
                    "000008f0 e9 e8 e8 e8 e8 e8 e8 e8                          ········         \n" +
                    "# position: 2296, header: 106\n" +
                    "--- !!data #binary\n" +
                    "000008f0                                      ea e9 e9 e9              ····\n" +
                    "00000900 e9 e9 e9 e9                                      ····             \n" +
                    "# position: 2308, header: 107\n" +
                    "--- !!data #binary\n" +
                    "00000900                          eb ea ea ea ea ea ea ea          ········\n" +
                    "# position: 2320, header: 108\n" +
                    "--- !!data #binary\n" +
                    "00000910             ec eb eb eb  eb eb eb eb                 ···· ····    \n" +
                    "# position: 2332, header: 109\n" +
                    "--- !!data #binary\n" +
                    "00000920 ed ec ec ec ec ec ec ec                          ········         \n" +
                    "# position: 2344, header: 110\n" +
                    "--- !!data #binary\n" +
                    "00000920                                      ee ed ed ed              ····\n" +
                    "00000930 ed ed ed ed                                      ····             \n" +
                    "# position: 2356, header: 111\n" +
                    "--- !!data #binary\n" +
                    "00000930                          ef ee ee ee ee ee ee ee          ········\n" +
                    "# position: 2368, header: 112\n" +
                    "--- !!data #binary\n" +
                    "00000940             f0 ef ef ef  ef ef ef ef                 ···· ····    \n" +
                    "# position: 2380, header: 113\n" +
                    "--- !!data #binary\n" +
                    "00000950 f1 f0 f0 f0 f0 f0 f0 f0                          ········         \n" +
                    "# position: 2392, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000950                                      f2 f1 f1 f1              ····\n" +
                    "00000960 f1 f1 f1 f1                                      ····             \n" +
                    "# position: 2404, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000960                          f3 f2 f2 f2 f2 f2 f2 f2          ········\n" +
                    "# position: 2416, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000970             f4 f3 f3 f3  f3 f3 f3 f3                 ···· ····    \n" +
                    "# position: 2428, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000980 f5 f4 f4 f4 f4 f4 f4 f4                          ········         \n" +
                    "# position: 2440, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000980                                      f6 f5 f5 f5              ····\n" +
                    "00000990 f5 f5 f5 f5                                      ····             \n" +
                    "# position: 2452, header: 119\n" +
                    "--- !!data #binary\n" +
                    "00000990                          f7 f6 f6 f6 f6 f6 f6 f6          ········\n" +
                    "# position: 2464, header: 120\n" +
                    "--- !!data #binary\n" +
                    "000009a0             f8 f7 f7 f7  f7 f7 f7 f7                 ···· ····    \n" +
                    "# position: 2476, header: 121\n" +
                    "--- !!data #binary\n" +
                    "000009b0 f9 f8 f8 f8 f8 f8 f8 f8                          ········         \n" +
                    "# position: 2488, header: 122\n" +
                    "--- !!data #binary\n" +
                    "000009b0                                      fa f9 f9 f9              ····\n" +
                    "000009c0 f9 f9 f9 f9                                      ····             \n" +
                    "# position: 2500, header: 123\n" +
                    "--- !!data #binary\n" +
                    "000009c0                          fb fa fa fa fa fa fa fa          ········\n" +
                    "# position: 2512, header: 124\n" +
                    "--- !!data #binary\n" +
                    "000009d0             fc fb fb fb  fb fb fb fb                 ···· ····    \n" +
                    "# position: 2524, header: 125\n" +
                    "--- !!data #binary\n" +
                    "000009e0 fd fc fc fc fc fc fc fc                          ········         \n" +
                    "# position: 2536, header: 126\n" +
                    "--- !!data #binary\n" +
                    "000009e0                                      fe fd fd fd              ····\n" +
                    "000009f0 fd fd fd fd                                      ····             \n" +
                    "# position: 2548, header: 127\n" +
                    "--- !!data #binary\n" +
                    "000009f0                          ff fe fe fe fe fe fe fe          ········\n" +
                    "# position: 2560, header: 128\n" +
                    "--- !!data #binary\n" +
                    "00000a00             00 00 00 00  00 00 00 00                 ···· ····    \n" +
                    "# position: 2572, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  2560,\n" +
                    "  2892,\n" +
                    "  2940,\n" +
                    "  2988,\n" +
                    "  3036,\n" +
                    "  3084,\n" +
                    "  3132,\n" +
                    "  3180,\n" +
                    "  3228,\n" +
                    "  3276,\n" +
                    "  3324,\n" +
                    "  3372,\n" +
                    "  3420,\n" +
                    "  3468,\n" +
                    "  3516,\n" +
                    "  3564,\n" +
                    "  3612,\n" +
                    "  3660,\n" +
                    "  3708,\n" +
                    "  3756,\n" +
                    "  3804,\n" +
                    "  3852,\n" +
                    "  3900,\n" +
                    "  3948,\n" +
                    "  3996,\n" +
                    "  4044,\n" +
                    "  4092,\n" +
                    "  4140,\n" +
                    "  4188,\n" +
                    "  4236,\n" +
                    "  4284,\n" +
                    "  4332\n" +
                    "]\n" +
                    "# position: 2856, header: 129\n" +
                    "--- !!data #binary\n" +
                    "00000b20                                      01 01 01 01              ····\n" +
                    "00000b30 01 01 01 01                                      ····             \n" +
                    "# position: 2868, header: 130\n" +
                    "--- !!data #binary\n" +
                    "00000b30                          02 02 02 02 02 02 02 02          ········\n" +
                    "# position: 2880, header: 131\n" +
                    "--- !!data #binary\n" +
                    "00000b40             03 03 03 03  03 03 03 03                 ···· ····    \n" +
                    "# position: 2892, header: 132\n" +
                    "--- !!data #binary\n" +
                    "00000b50 04 04 04 04 04 04 04 04                          ········         \n" +
                    "# position: 2904, header: 133\n" +
                    "--- !!data #binary\n" +
                    "00000b50                                      05 05 05 05              ····\n" +
                    "00000b60 05 05 05 05                                      ····             \n" +
                    "# position: 2916, header: 134\n" +
                    "--- !!data #binary\n" +
                    "00000b60                          06 06 06 06 06 06 06 06          ········\n" +
                    "# position: 2928, header: 135\n" +
                    "--- !!data #binary\n" +
                    "00000b70             07 07 07 07  07 07 07 07                 ···· ····    \n" +
                    "# position: 2940, header: 136\n" +
                    "--- !!data #binary\n" +
                    "00000b80 08 08 08 08 08 08 08 08                          ········         \n" +
                    "# position: 2952, header: 137\n" +
                    "--- !!data #binary\n" +
                    "00000b80                                      09 09 09 09              ····\n" +
                    "00000b90 09 09 09 09                                      ····             \n" +
                    "# position: 2964, header: 138\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2976, header: 139\n" +
                    "--- !!data #binary\n" +
                    "00000ba0             0b 0b 0b 0b  0b 0b 0b 0b                 ···· ····    \n" +
                    "# position: 2988, header: 140\n" +
                    "--- !!data #binary\n" +
                    "00000bb0 0c 0c 0c 0c 0c 0c 0c 0c                          ········         \n" +
                    "# position: 3000, header: 141\n" +
                    "--- !!data #binary\n" +
                    "00000bb0                                      0d 0d 0d 0d              ····\n" +
                    "00000bc0 0d 0d 0d 0d                                      ····             \n" +
                    "# position: 3012, header: 142\n" +
                    "--- !!data #binary\n" +
                    "00000bc0                          0e 0e 0e 0e 0e 0e 0e 0e          ········\n" +
                    "# position: 3024, header: 143\n" +
                    "--- !!data #binary\n" +
                    "00000bd0             0f 0f 0f 0f  0f 0f 0f 0f                 ···· ····    \n" +
                    "# position: 3036, header: 144\n" +
                    "--- !!data #binary\n" +
                    "00000be0 10 10 10 10 10 10 10 10                          ········         \n" +
                    "# position: 3048, header: 145\n" +
                    "--- !!data #binary\n" +
                    "00000be0                                      11 11 11 11              ····\n" +
                    "00000bf0 11 11 11 11                                      ····             \n" +
                    "# position: 3060, header: 146\n" +
                    "--- !!data #binary\n" +
                    "00000bf0                          12 12 12 12 12 12 12 12          ········\n" +
                    "# position: 3072, header: 147\n" +
                    "--- !!data #binary\n" +
                    "00000c00             13 13 13 13  13 13 13 13                 ···· ····    \n" +
                    "# position: 3084, header: 148\n" +
                    "--- !!data #binary\n" +
                    "00000c10 14 14 14 14 14 14 14 14                          ········         \n" +
                    "# position: 3096, header: 149\n" +
                    "--- !!data #binary\n" +
                    "00000c10                                      15 15 15 15              ····\n" +
                    "00000c20 15 15 15 15                                      ····             \n" +
                    "# position: 3108, header: 150\n" +
                    "--- !!data #binary\n" +
                    "00000c20                          16 16 16 16 16 16 16 16          ········\n" +
                    "# position: 3120, header: 151\n" +
                    "--- !!data #binary\n" +
                    "00000c30             17 17 17 17  17 17 17 17                 ···· ····    \n" +
                    "# position: 3132, header: 152\n" +
                    "--- !!data #binary\n" +
                    "00000c40 18 18 18 18 18 18 18 18                          ········         \n" +
                    "# position: 3144, header: 153\n" +
                    "--- !!data #binary\n" +
                    "00000c40                                      19 19 19 19              ····\n" +
                    "00000c50 19 19 19 19                                      ····             \n" +
                    "# position: 3156, header: 154\n" +
                    "--- !!data #binary\n" +
                    "00000c50                          1a 1a 1a 1a 1a 1a 1a 1a          ········\n" +
                    "# position: 3168, header: 155\n" +
                    "--- !!data #binary\n" +
                    "00000c60             1b 1b 1b 1b  1b 1b 1b 1b                 ···· ····    \n" +
                    "# position: 3180, header: 156\n" +
                    "--- !!data #binary\n" +
                    "00000c70 1c 1c 1c 1c 1c 1c 1c 1c                          ········         \n" +
                    "# position: 3192, header: 157\n" +
                    "--- !!data #binary\n" +
                    "00000c70                                      1d 1d 1d 1d              ····\n" +
                    "00000c80 1d 1d 1d 1d                                      ····             \n" +
                    "# position: 3204, header: 158\n" +
                    "--- !!data #binary\n" +
                    "00000c80                          1e 1e 1e 1e 1e 1e 1e 1e          ········\n" +
                    "# position: 3216, header: 159\n" +
                    "--- !!data #binary\n" +
                    "00000c90             1f 1f 1f 1f  1f 1f 1f 1f                 ···· ····    \n" +
                    "# position: 3228, header: 160\n" +
                    "--- !!data\n" +
                    "        \n" +
                    "# position: 3240, header: 161\n" +
                    "--- !!data\n" +
                    "!!!!!!!!\n" +
                    "# position: 3252, header: 162\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\"\"\"\"\n" +
                    "# position: 3264, header: 163\n" +
                    "--- !!data\n" +
                    "########\n" +
                    "# position: 3276, header: 164\n" +
                    "--- !!data\n" +
                    "$$$$$$$$\n" +
                    "# position: 3288, header: 165\n" +
                    "--- !!data\n" +
                    "%%%%%%%%\n" +
                    "# position: 3300, header: 166\n" +
                    "--- !!data\n" +
                    "&&&&&&&&\n" +
                    "# position: 3312, header: 167\n" +
                    "--- !!data\n" +
                    "''''''''\n" +
                    "# position: 3324, header: 168\n" +
                    "--- !!data\n" +
                    "((((((((\n" +
                    "# position: 3336, header: 169\n" +
                    "--- !!data\n" +
                    "))))))))\n" +
                    "# position: 3348, header: 170\n" +
                    "--- !!data\n" +
                    "********\n" +
                    "# position: 3360, header: 171\n" +
                    "--- !!data\n" +
                    "++++++++\n" +
                    "# position: 3372, header: 172\n" +
                    "--- !!data\n" +
                    ",,,,,,,,\n" +
                    "# position: 3384, header: 173\n" +
                    "--- !!data\n" +
                    "--------\n" +
                    "# position: 3396, header: 174\n" +
                    "--- !!data\n" +
                    "........\n" +
                    "# position: 3408, header: 175\n" +
                    "--- !!data\n" +
                    "////////\n" +
                    "# position: 3420, header: 176\n" +
                    "--- !!data\n" +
                    "00000000\n" +
                    "# position: 3432, header: 177\n" +
                    "--- !!data\n" +
                    "11111111\n" +
                    "# position: 3444, header: 178\n" +
                    "--- !!data\n" +
                    "22222222\n" +
                    "# position: 3456, header: 179\n" +
                    "--- !!data\n" +
                    "33333333\n" +
                    "# position: 3468, header: 180\n" +
                    "--- !!data\n" +
                    "44444444\n" +
                    "# position: 3480, header: 181\n" +
                    "--- !!data\n" +
                    "55555555\n" +
                    "# position: 3492, header: 182\n" +
                    "--- !!data\n" +
                    "66666666\n" +
                    "# position: 3504, header: 183\n" +
                    "--- !!data\n" +
                    "77777777\n" +
                    "# position: 3516, header: 184\n" +
                    "--- !!data\n" +
                    "88888888\n" +
                    "# position: 3528, header: 185\n" +
                    "--- !!data\n" +
                    "99999999\n" +
                    "# position: 3540, header: 186\n" +
                    "--- !!data\n" +
                    "::::::::\n" +
                    "# position: 3552, header: 187\n" +
                    "--- !!data\n" +
                    ";;;;;;;;\n" +
                    "# position: 3564, header: 188\n" +
                    "--- !!data\n" +
                    "<<<<<<<<\n" +
                    "# position: 3576, header: 189\n" +
                    "--- !!data\n" +
                    "========\n" +
                    "# position: 3588, header: 190\n" +
                    "--- !!data\n" +
                    ">>>>>>>>\n" +
                    "# position: 3600, header: 191\n" +
                    "--- !!data\n" +
                    "????????\n" +
                    "# position: 3612, header: 192\n" +
                    "--- !!data\n" +
                    "@@@@@@@@\n" +
                    "# position: 3624, header: 193\n" +
                    "--- !!data\n" +
                    "AAAAAAAA\n" +
                    "# position: 3636, header: 194\n" +
                    "--- !!data\n" +
                    "BBBBBBBB\n" +
                    "# position: 3648, header: 195\n" +
                    "--- !!data\n" +
                    "CCCCCCCC\n" +
                    "# position: 3660, header: 196\n" +
                    "--- !!data\n" +
                    "DDDDDDDD\n" +
                    "# position: 3672, header: 197\n" +
                    "--- !!data\n" +
                    "EEEEEEEE\n" +
                    "# position: 3684, header: 198\n" +
                    "--- !!data\n" +
                    "FFFFFFFF\n" +
                    "# position: 3696, header: 199\n" +
                    "--- !!data\n" +
                    "GGGGGGGG\n" +
                    "# position: 3708, header: 200\n" +
                    "--- !!data\n" +
                    "HHHHHHHH\n" +
                    "# position: 3720, header: 201\n" +
                    "--- !!data\n" +
                    "IIIIIIII\n" +
                    "# position: 3732, header: 202\n" +
                    "--- !!data\n" +
                    "JJJJJJJJ\n" +
                    "# position: 3744, header: 203\n" +
                    "--- !!data\n" +
                    "KKKKKKKK\n" +
                    "# position: 3756, header: 204\n" +
                    "--- !!data\n" +
                    "LLLLLLLL\n" +
                    "# position: 3768, header: 205\n" +
                    "--- !!data\n" +
                    "MMMMMMMM\n" +
                    "# position: 3780, header: 206\n" +
                    "--- !!data\n" +
                    "NNNNNNNN\n" +
                    "# position: 3792, header: 207\n" +
                    "--- !!data\n" +
                    "OOOOOOOO\n" +
                    "# position: 3804, header: 208\n" +
                    "--- !!data\n" +
                    "PPPPPPPP\n" +
                    "# position: 3816, header: 209\n" +
                    "--- !!data\n" +
                    "QQQQQQQQ\n" +
                    "# position: 3828, header: 210\n" +
                    "--- !!data\n" +
                    "RRRRRRRR\n" +
                    "# position: 3840, header: 211\n" +
                    "--- !!data\n" +
                    "SSSSSSSS\n" +
                    "# position: 3852, header: 212\n" +
                    "--- !!data\n" +
                    "TTTTTTTT\n" +
                    "# position: 3864, header: 213\n" +
                    "--- !!data\n" +
                    "UUUUUUUU\n" +
                    "# position: 3876, header: 214\n" +
                    "--- !!data\n" +
                    "VVVVVVVV\n" +
                    "# position: 3888, header: 215\n" +
                    "--- !!data\n" +
                    "WWWWWWWW\n" +
                    "# position: 3900, header: 216\n" +
                    "--- !!data\n" +
                    "XXXXXXXX\n" +
                    "# position: 3912, header: 217\n" +
                    "--- !!data\n" +
                    "YYYYYYYY\n" +
                    "# position: 3924, header: 218\n" +
                    "--- !!data\n" +
                    "ZZZZZZZZ\n" +
                    "# position: 3936, header: 219\n" +
                    "--- !!data\n" +
                    "[[[[[[[[\n" +
                    "# position: 3948, header: 220\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\\\\\\\\\n" +
                    "# position: 3960, header: 221\n" +
                    "--- !!data\n" +
                    "]]]]]]]]\n" +
                    "# position: 3972, header: 222\n" +
                    "--- !!data\n" +
                    "^^^^^^^^\n" +
                    "# position: 3984, header: 223\n" +
                    "--- !!data\n" +
                    "________\n" +
                    "# position: 3996, header: 224\n" +
                    "--- !!data\n" +
                    "````````\n" +
                    "# position: 4008, header: 225\n" +
                    "--- !!data\n" +
                    "aaaaaaaa\n" +
                    "# position: 4020, header: 226\n" +
                    "--- !!data\n" +
                    "bbbbbbbb\n" +
                    "# position: 4032, header: 227\n" +
                    "--- !!data\n" +
                    "cccccccc\n" +
                    "# position: 4044, header: 228\n" +
                    "--- !!data\n" +
                    "dddddddd\n" +
                    "# position: 4056, header: 229\n" +
                    "--- !!data\n" +
                    "eeeeeeee\n" +
                    "# position: 4068, header: 230\n" +
                    "--- !!data\n" +
                    "ffffffff\n" +
                    "# position: 4080, header: 231\n" +
                    "--- !!data\n" +
                    "gggggggg\n" +
                    "# position: 4092, header: 232\n" +
                    "--- !!data\n" +
                    "hhhhhhhh\n" +
                    "# position: 4104, header: 233\n" +
                    "--- !!data\n" +
                    "iiiiiiii\n" +
                    "# position: 4116, header: 234\n" +
                    "--- !!data\n" +
                    "jjjjjjjj\n" +
                    "# position: 4128, header: 235\n" +
                    "--- !!data\n" +
                    "kkkkkkkk\n" +
                    "# position: 4140, header: 236\n" +
                    "--- !!data\n" +
                    "llllllll\n" +
                    "# position: 4152, header: 237\n" +
                    "--- !!data\n" +
                    "mmmmmmmm\n" +
                    "# position: 4164, header: 238\n" +
                    "--- !!data\n" +
                    "nnnnnnnn\n" +
                    "# position: 4176, header: 239\n" +
                    "--- !!data\n" +
                    "oooooooo\n" +
                    "# position: 4188, header: 240\n" +
                    "--- !!data\n" +
                    "pppppppp\n" +
                    "# position: 4200, header: 241\n" +
                    "--- !!data\n" +
                    "qqqqqqqq\n" +
                    "# position: 4212, header: 242\n" +
                    "--- !!data\n" +
                    "rrrrrrrr\n" +
                    "# position: 4224, header: 243\n" +
                    "--- !!data\n" +
                    "ssssssss\n" +
                    "# position: 4236, header: 244\n" +
                    "--- !!data\n" +
                    "tttttttt\n" +
                    "# position: 4248, header: 245\n" +
                    "--- !!data\n" +
                    "uuuuuuuu\n" +
                    "# position: 4260, header: 246\n" +
                    "--- !!data\n" +
                    "vvvvvvvv\n" +
                    "# position: 4272, header: 247\n" +
                    "--- !!data\n" +
                    "wwwwwwww\n" +
                    "# position: 4284, header: 248\n" +
                    "--- !!data\n" +
                    "xxxxxxxx\n" +
                    "# position: 4296, header: 249\n" +
                    "--- !!data\n" +
                    "yyyyyyyy\n" +
                    "# position: 4308, header: 250\n" +
                    "--- !!data\n" +
                    "zzzzzzzz\n" +
                    "# position: 4320, header: 251\n" +
                    "--- !!data\n" +
                    "{{{{{{{{\n" +
                    "# position: 4332, header: 252\n" +
                    "--- !!data\n" +
                    "||||||||\n" +
                    "# position: 4344, header: 253\n" +
                    "--- !!data\n" +
                    "}}}}}}}}\n" +
                    "# position: 4356, header: 254\n" +
                    "--- !!data\n" +
                    "~~~~~~~~\n" +
                    "# position: 4368, header: 255\n" +
                    "--- !!data\n" +
                    "\u007F\u007F\u007F\u007F\u007F\u007F\u007F\u007F\n" +
                    "...\n" +
                    "# 126688 bytes remaining\n", queue.dump());

        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }

    public boolean postOneMessage(@NotNull ExcerptAppender appender) {
        appender.writeBytes(outgoingBytes);
        return true;
    }

    public int fetchOneMessage(@NotNull ExcerptTailer tailer, @NotNull byte[] using) {
        try (DocumentContext dc = tailer.readingDocument()) {
            return !dc.isPresent() ? -1 : dc.wire().bytes().read(using);
        }
    }

    @After
    public void checkRegisteredBytes() {
        outgoingBytes.release();
        BytesUtil.checkRegisteredBytes();
    }
} 