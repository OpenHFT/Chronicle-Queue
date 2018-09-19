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
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;

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
                    "  writePosition: [\n" +
                    "    4112,\n" +
                    "    17660905521407\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 184,\n" +
                    "    lastIndex: 256\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 184, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 2\n" +
                    "  480,\n" +
                    "  2316,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  768,\n" +
                    "  816,\n" +
                    "  864,\n" +
                    "  912,\n" +
                    "  960,\n" +
                    "  1008,\n" +
                    "  1056,\n" +
                    "  1104,\n" +
                    "  1152,\n" +
                    "  1200,\n" +
                    "  1248,\n" +
                    "  1296,\n" +
                    "  1344,\n" +
                    "  1392,\n" +
                    "  1440,\n" +
                    "  1488,\n" +
                    "  1536,\n" +
                    "  1584,\n" +
                    "  1632,\n" +
                    "  1680,\n" +
                    "  1728,\n" +
                    "  1776,\n" +
                    "  1824,\n" +
                    "  1872,\n" +
                    "  1920,\n" +
                    "  1968,\n" +
                    "  2016,\n" +
                    "  2064,\n" +
                    "  2112,\n" +
                    "  2160,\n" +
                    "  2208,\n" +
                    "  2256\n" +
                    "]\n" +
                    "# position: 768, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000300             80 7f 7f 7f  7f 7f 7f 7f                 ···· ····    \n" +
                    "# position: 780, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000310 81 80 80 80 80 80 80 80                          ········         \n" +
                    "# position: 792, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000310                                      82 81 81 81              ····\n" +
                    "00000320 81 81 81 81                                      ····             \n" +
                    "# position: 804, header: 3\n" +
                    "--- !!data #binary\n" +
                    "00000320                          83 82 82 82 82 82 82 82          ········\n" +
                    "# position: 816, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 828, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 840, header: 6\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x86\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 852, header: 7\n" +
                    "--- !!data #binary\n" +
                    "00000350                          87 86 86 86 86 86 86 86          ········\n" +
                    "# position: 864, header: 8\n" +
                    "--- !!data #binary\n" +
                    "00000360             88 87 87 87  87 87 87 87                 ···· ····    \n" +
                    "# position: 876, header: 9\n" +
                    "--- !!data #binary\n" +
                    "00000370 89 88 88 88 88 88 88 88                          ········         \n" +
                    "# position: 888, header: 10\n" +
                    "--- !!data #binary\n" +
                    "\"\\x89\\x89\\x89\\x89\\x89\\x89\\x89\"\n" +
                    "# position: 900, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "\"\\x8A\\x8A\\x8A\\x8A\\x8A\\x8A\"\n" +
                    "# position: 912, header: 12\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 924, header: 13\n" +
                    "--- !!data #binary\n" +
                    "000003a0 8d 8c 8c 8c 8c 8c 8c 8c                          ········         \n" +
                    "# position: 936, header: 14\n" +
                    "--- !!data #binary\n" +
                    "000003a0                                      8e 8d 8d 8d              ····\n" +
                    "000003b0 8d 8d 8d 8d                                      ····             \n" +
                    "# position: 948, header: 15\n" +
                    "--- !!data #binary\n" +
                    "000003b0                          8f 8e 8e 8e 8e 8e 8e 8e          ········\n" +
                    "# position: 960, header: 16\n" +
                    "--- !!data #binary\n" +
                    "-1.4156185439721035E-29\n" +
                    "# position: 972, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "-5.702071897398123E-29\n" +
                    "# # EndOfFile\n" +
                    "# position: 984, header: 18\n" +
                    "--- !!data #binary\n" +
                    "-753555055760.82\n" +
                    "# position: 996, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_2\n" +
                    "-48698841.79\n" +
                    "# position: 1008, header: 20\n" +
                    "--- !!data #binary\n" +
                    "-8422085917.3268\n" +
                    "# position: 1020, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_4\n" +
                    "-541098.2421\n" +
                    "# position: 1032, header: 22\n" +
                    "--- !!data #binary\n" +
                    "-93086212.770454\n" +
                    "# position: 1044, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_6\n" +
                    "-5952.080663\n" +
                    "# position: 1056, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 1068, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 1080, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 1092, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# position: 1104, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# position: 1116, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# position: 1128, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 1140, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 1152, header: 32\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 1164, header: 33\n" +
                    "--- !!data #binary\n" +
                    "!int 160\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# position: 1176, header: 34\n" +
                    "--- !!data #binary\n" +
                    "!int 41377\n" +
                    "!int 161\n" +
                    "!int 161\n" +
                    "!int -1\n" +
                    "# position: 1188, header: 35\n" +
                    "--- !!data #binary\n" +
                    "2728567458\n" +
                    "!int 41634\n" +
                    "# position: 1200, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "2745410467\n" +
                    "# # EndOfFile\n" +
                    "# position: 1212, header: 37\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte -92\n" +
                    "!byte -92\n" +
                    "!byte 0\n" +
                    "# position: 1224, header: 38\n" +
                    "--- !!data #binary\n" +
                    "!int -1515870811\n" +
                    "!short -23131\n" +
                    "# position: 1236, header: 39\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "!int -1499027802\n" +
                    "# # EndOfFile\n" +
                    "# position: 1248, header: 40\n" +
                    "--- !!data #binary\n" +
                    "!int 167\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# position: 1260, header: 41\n" +
                    "--- !!data #binary\n" +
                    "!int 43176\n" +
                    "!int 168\n" +
                    "!int 168\n" +
                    "!int -1\n" +
                    "# position: 1272, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # SET_LOW_INT16\n" +
                    "!int 43433\n" +
                    "!int 43433\n" +
                    "# position: 1284, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1296, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1308, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1320, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1332, header: 47\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1344, header: 48\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1356, header: 49\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1368, header: 50\n" +
                    "--- !!data #binary\n" +
                    "00000550                                      b2 b1 b1 b1              ····\n" +
                    "00000560 b1 b1 b1 b1                                      ····             \n" +
                    "# position: 1380, header: 51\n" +
                    "--- !!data #binary\n" +
                    "00000560                          b3 b2 b2 b2 b2 b2 b2 b2          ········\n" +
                    "# position: 1392, header: 52\n" +
                    "--- !!data #binary\n" +
                    "00000570             b4 b3 b3 b3  b3 b3 b3 b3                 ···· ····    \n" +
                    "# position: 1404, header: 53\n" +
                    "--- !!data #binary\n" +
                    "00000580 b5 b4 b4 b4 b4 b4 b4 b4                          ········         \n" +
                    "# position: 1416, header: 54\n" +
                    "--- !!data #binary\n" +
                    "00000580                                      b6 b5 b5 b5              ····\n" +
                    "00000590 b5 b5 b5 b5                                      ····             \n" +
                    "# position: 1428, header: 55\n" +
                    "--- !!data #binary\n" +
                    "00000590                          b7 b6 b6 b6 b6 b6 b6 b6          ········\n" +
                    "# position: 1440, header: 56\n" +
                    "--- !!data #binary\n" +
                    "000005a0             b8 b7 b7 b7  b7 b7 b7 b7                 ···· ····    \n" +
                    "# position: 1452, header: 57\n" +
                    "--- !!data #binary\n" +
                    "000005b0 b9 b8 b8 b8 b8 b8 b8 b8                          ········         \n" +
                    "# position: 1464, header: 58\n" +
                    "--- !!data #binary\n" +
                    "\"-252662577519802\": \n" +
                    "# position: 1476, header: 59\n" +
                    "--- !!data #binary\n" +
                    "!!null \"\"\n" +
                    "\"-2008556674363\": \n" +
                    "# position: 1488, header: 60\n" +
                    "--- !!data #binary\n" +
                    "000005d0             bc bb bb bb  bb bb bb bb                 ···· ····    \n" +
                    "# position: 1500, header: 61\n" +
                    "--- !!data #binary\n" +
                    "000005e0 bd bc bc bc bc bc bc bc                          ········         \n" +
                    "# position: 1512, header: 62\n" +
                    "--- !!data #binary\n" +
                    "000005e0                                      be bd bd bd              ····\n" +
                    "000005f0 bd bd bd bd                                      ····             \n" +
                    "# position: 1524, header: 63\n" +
                    "--- !!data #binary\n" +
                    "000005f0                          bf be be be be be be be          ········\n" +
                    "# position: 1536, header: 64\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1548, header: 65\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC0\": \"\": \"\": \"\": \"\": \"\": \"\": \n" +
                    "# position: 1560, header: 66\n" +
                    "--- !!data #binary\n" +
                    "00000610                                      c2 c1 c1 c1              ····\n" +
                    "00000620 c1 c1 c1 c1                                      ····             \n" +
                    "# position: 1572, header: 67\n" +
                    "--- !!data #binary\n" +
                    "00000620                          c3 c2 c2 c2 c2 c2 c2 c2          ········\n" +
                    "# position: 1584, header: 68\n" +
                    "--- !!data #binary\n" +
                    "00000630             c4 c3 c3 c3  c3 c3 c3 c3                 ···· ····    \n" +
                    "# position: 1596, header: 69\n" +
                    "--- !!data #binary\n" +
                    "00000640 c5 c4 c4 c4 c4 c4 c4 c4                          ········         \n" +
                    "# position: 1608, header: 70\n" +
                    "--- !!data #binary\n" +
                    "00000640                                      c6 c5 c5 c5              ····\n" +
                    "00000650 c5 c5 c5 c5                                      ····             \n" +
                    "# position: 1620, header: 71\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\": \n" +
                    "# position: 1632, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000660             c8 c7 c7 c7  c7 c7 c7 c7                 ···· ····    \n" +
                    "# position: 1644, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000670 c9 c8 c8 c8 c8 c8 c8 c8                          ········         \n" +
                    "# position: 1656, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000670                                      ca c9 c9 c9              ····\n" +
                    "00000680 c9 c9 c9 c9                                      ····             \n" +
                    "# position: 1668, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000680                          cb ca ca ca ca ca ca ca          ········\n" +
                    "# position: 1680, header: 76\n" +
                    "--- !!data #binary\n" +
                    "00000690             cc cb cb cb  cb cb cb cb                 ···· ····    \n" +
                    "# position: 1692, header: 77\n" +
                    "--- !!data #binary\n" +
                    "000006a0 cd cc cc cc cc cc cc cc                          ········         \n" +
                    "# position: 1704, header: 78\n" +
                    "--- !!data #binary\n" +
                    "000006a0                                      ce cd cd cd              ····\n" +
                    "000006b0 cd cd cd cd                                      ····             \n" +
                    "# position: 1716, header: 79\n" +
                    "--- !!data #binary\n" +
                    "000006b0                          cf ce ce ce ce ce ce ce          ········\n" +
                    "# position: 1728, header: 80\n" +
                    "--- !!data #binary\n" +
                    "000006c0             d0 cf cf cf  cf cf cf cf                 ···· ····    \n" +
                    "# position: 1740, header: 81\n" +
                    "--- !!data #binary\n" +
                    "000006d0 d1 d0 d0 d0 d0 d0 d0 d0                          ········         \n" +
                    "# position: 1752, header: 82\n" +
                    "--- !!data #binary\n" +
                    "000006d0                                      d2 d1 d1 d1              ····\n" +
                    "000006e0 d1 d1 d1 d1                                      ····             \n" +
                    "# position: 1764, header: 83\n" +
                    "--- !!data #binary\n" +
                    "000006e0                          d3 d2 d2 d2 d2 d2 d2 d2          ········\n" +
                    "# position: 1776, header: 84\n" +
                    "--- !!data #binary\n" +
                    "000006f0             d4 d3 d3 d3  d3 d3 d3 d3                 ···· ····    \n" +
                    "# position: 1788, header: 85\n" +
                    "--- !!data #binary\n" +
                    "00000700 d5 d4 d4 d4 d4 d4 d4 d4                          ········         \n" +
                    "# position: 1800, header: 86\n" +
                    "--- !!data #binary\n" +
                    "00000700                                      d6 d5 d5 d5              ····\n" +
                    "00000710 d5 d5 d5 d5                                      ····             \n" +
                    "# position: 1812, header: 87\n" +
                    "--- !!data #binary\n" +
                    "00000710                          d7 d6 d6 d6 d6 d6 d6 d6          ········\n" +
                    "# position: 1824, header: 88\n" +
                    "--- !!data #binary\n" +
                    "00000720             d8 d7 d7 d7  d7 d7 d7 d7                 ···· ····    \n" +
                    "# position: 1836, header: 89\n" +
                    "--- !!data #binary\n" +
                    "00000730 d9 d8 d8 d8 d8 d8 d8 d8                          ········         \n" +
                    "# position: 1848, header: 90\n" +
                    "--- !!data #binary\n" +
                    "00000730                                      da d9 d9 d9              ····\n" +
                    "00000740 d9 d9 d9 d9                                      ····             \n" +
                    "# position: 1860, header: 91\n" +
                    "--- !!data #binary\n" +
                    "00000740                          db da da da da da da da          ········\n" +
                    "# position: 1872, header: 92\n" +
                    "--- !!data #binary\n" +
                    "00000750             dc db db db  db db db db                 ···· ····    \n" +
                    "# position: 1884, header: 93\n" +
                    "--- !!data #binary\n" +
                    "00000760 dd dc dc dc dc dc dc dc                          ········         \n" +
                    "# position: 1896, header: 94\n" +
                    "--- !!data #binary\n" +
                    "00000760                                      de dd dd dd              ····\n" +
                    "00000770 dd dd dd dd                                      ····             \n" +
                    "# position: 1908, header: 95\n" +
                    "--- !!data #binary\n" +
                    "00000770                          df de de de de de de de          ········\n" +
                    "# position: 1920, header: 96\n" +
                    "--- !!data #binary\n" +
                    "00000780             e0 df df df  df df df df                 ···· ····    \n" +
                    "# position: 1932, header: 97\n" +
                    "--- !!data #binary\n" +
                    "00000790 e1 e0 e0 e0 e0 e0 e0 e0                          ········         \n" +
                    "# position: 1944, header: 98\n" +
                    "--- !!data #binary\n" +
                    "00000790                                      e2 e1 e1 e1              ····\n" +
                    "000007a0 e1 e1 e1 e1                                      ····             \n" +
                    "# position: 1956, header: 99\n" +
                    "--- !!data #binary\n" +
                    "000007a0                          e3 e2 e2 e2 e2 e2 e2 e2          ········\n" +
                    "# position: 1968, header: 100\n" +
                    "--- !!data #binary\n" +
                    "000007b0             e4 e3 e3 e3  e3 e3 e3 e3                 ···· ····    \n" +
                    "# position: 1980, header: 101\n" +
                    "--- !!data #binary\n" +
                    "000007c0 e5 e4 e4 e4 e4 e4 e4 e4                          ········         \n" +
                    "# position: 1992, header: 102\n" +
                    "--- !!data #binary\n" +
                    "000007c0                                      e6 e5 e5 e5              ····\n" +
                    "000007d0 e5 e5 e5 e5                                      ····             \n" +
                    "# position: 2004, header: 103\n" +
                    "--- !!data #binary\n" +
                    "000007d0                          e7 e6 e6 e6 e6 e6 e6 e6          ········\n" +
                    "# position: 2016, header: 104\n" +
                    "--- !!data #binary\n" +
                    "000007e0             e8 e7 e7 e7  e7 e7 e7 e7                 ···· ····    \n" +
                    "# position: 2028, header: 105\n" +
                    "--- !!data #binary\n" +
                    "000007f0 e9 e8 e8 e8 e8 e8 e8 e8                          ········         \n" +
                    "# position: 2040, header: 106\n" +
                    "--- !!data #binary\n" +
                    "000007f0                                      ea e9 e9 e9              ····\n" +
                    "00000800 e9 e9 e9 e9                                      ····             \n" +
                    "# position: 2052, header: 107\n" +
                    "--- !!data #binary\n" +
                    "00000800                          eb ea ea ea ea ea ea ea          ········\n" +
                    "# position: 2064, header: 108\n" +
                    "--- !!data #binary\n" +
                    "00000810             ec eb eb eb  eb eb eb eb                 ···· ····    \n" +
                    "# position: 2076, header: 109\n" +
                    "--- !!data #binary\n" +
                    "00000820 ed ec ec ec ec ec ec ec                          ········         \n" +
                    "# position: 2088, header: 110\n" +
                    "--- !!data #binary\n" +
                    "00000820                                      ee ed ed ed              ····\n" +
                    "00000830 ed ed ed ed                                      ····             \n" +
                    "# position: 2100, header: 111\n" +
                    "--- !!data #binary\n" +
                    "00000830                          ef ee ee ee ee ee ee ee          ········\n" +
                    "# position: 2112, header: 112\n" +
                    "--- !!data #binary\n" +
                    "00000840             f0 ef ef ef  ef ef ef ef                 ···· ····    \n" +
                    "# position: 2124, header: 113\n" +
                    "--- !!data #binary\n" +
                    "00000850 f1 f0 f0 f0 f0 f0 f0 f0                          ········         \n" +
                    "# position: 2136, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000850                                      f2 f1 f1 f1              ····\n" +
                    "00000860 f1 f1 f1 f1                                      ····             \n" +
                    "# position: 2148, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000860                          f3 f2 f2 f2 f2 f2 f2 f2          ········\n" +
                    "# position: 2160, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000870             f4 f3 f3 f3  f3 f3 f3 f3                 ···· ····    \n" +
                    "# position: 2172, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000880 f5 f4 f4 f4 f4 f4 f4 f4                          ········         \n" +
                    "# position: 2184, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000880                                      f6 f5 f5 f5              ····\n" +
                    "00000890 f5 f5 f5 f5                                      ····             \n" +
                    "# position: 2196, header: 119\n" +
                    "--- !!data #binary\n" +
                    "00000890                          f7 f6 f6 f6 f6 f6 f6 f6          ········\n" +
                    "# position: 2208, header: 120\n" +
                    "--- !!data #binary\n" +
                    "000008a0             f8 f7 f7 f7  f7 f7 f7 f7                 ···· ····    \n" +
                    "# position: 2220, header: 121\n" +
                    "--- !!data #binary\n" +
                    "000008b0 f9 f8 f8 f8 f8 f8 f8 f8                          ········         \n" +
                    "# position: 2232, header: 122\n" +
                    "--- !!data #binary\n" +
                    "000008b0                                      fa f9 f9 f9              ····\n" +
                    "000008c0 f9 f9 f9 f9                                      ····             \n" +
                    "# position: 2244, header: 123\n" +
                    "--- !!data #binary\n" +
                    "000008c0                          fb fa fa fa fa fa fa fa          ········\n" +
                    "# position: 2256, header: 124\n" +
                    "--- !!data #binary\n" +
                    "000008d0             fc fb fb fb  fb fb fb fb                 ···· ····    \n" +
                    "# position: 2268, header: 125\n" +
                    "--- !!data #binary\n" +
                    "000008e0 fd fc fc fc fc fc fc fc                          ········         \n" +
                    "# position: 2280, header: 126\n" +
                    "--- !!data #binary\n" +
                    "000008e0                                      fe fd fd fd              ····\n" +
                    "000008f0 fd fd fd fd                                      ····             \n" +
                    "# position: 2292, header: 127\n" +
                    "--- !!data #binary\n" +
                    "000008f0                          ff fe fe fe fe fe fe fe          ········\n" +
                    "# position: 2304, header: 128\n" +
                    "--- !!data #binary\n" +
                    "00000900             00 00 00 00  00 00 00 00                 ···· ····    \n" +
                    "# position: 2316, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  2304,\n" +
                    "  2636,\n" +
                    "  2684,\n" +
                    "  2732,\n" +
                    "  2780,\n" +
                    "  2828,\n" +
                    "  2876,\n" +
                    "  2924,\n" +
                    "  2972,\n" +
                    "  3020,\n" +
                    "  3068,\n" +
                    "  3116,\n" +
                    "  3164,\n" +
                    "  3212,\n" +
                    "  3260,\n" +
                    "  3308,\n" +
                    "  3356,\n" +
                    "  3404,\n" +
                    "  3452,\n" +
                    "  3500,\n" +
                    "  3548,\n" +
                    "  3596,\n" +
                    "  3644,\n" +
                    "  3692,\n" +
                    "  3740,\n" +
                    "  3788,\n" +
                    "  3836,\n" +
                    "  3884,\n" +
                    "  3932,\n" +
                    "  3980,\n" +
                    "  4028,\n" +
                    "  4076\n" +
                    "]\n" +
                    "# position: 2600, header: 129\n" +
                    "--- !!data #binary\n" +
                    "00000a20                                      01 01 01 01              ····\n" +
                    "00000a30 01 01 01 01                                      ····             \n" +
                    "# position: 2612, header: 130\n" +
                    "--- !!data #binary\n" +
                    "00000a30                          02 02 02 02 02 02 02 02          ········\n" +
                    "# position: 2624, header: 131\n" +
                    "--- !!data #binary\n" +
                    "00000a40             03 03 03 03  03 03 03 03                 ···· ····    \n" +
                    "# position: 2636, header: 132\n" +
                    "--- !!data #binary\n" +
                    "00000a50 04 04 04 04 04 04 04 04                          ········         \n" +
                    "# position: 2648, header: 133\n" +
                    "--- !!data #binary\n" +
                    "00000a50                                      05 05 05 05              ····\n" +
                    "00000a60 05 05 05 05                                      ····             \n" +
                    "# position: 2660, header: 134\n" +
                    "--- !!data #binary\n" +
                    "00000a60                          06 06 06 06 06 06 06 06          ········\n" +
                    "# position: 2672, header: 135\n" +
                    "--- !!data #binary\n" +
                    "00000a70             07 07 07 07  07 07 07 07                 ···· ····    \n" +
                    "# position: 2684, header: 136\n" +
                    "--- !!data #binary\n" +
                    "00000a80 08 08 08 08 08 08 08 08                          ········         \n" +
                    "# position: 2696, header: 137\n" +
                    "--- !!data #binary\n" +
                    "00000a80                                      09 09 09 09              ····\n" +
                    "00000a90 09 09 09 09                                      ····             \n" +
                    "# position: 2708, header: 138\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2720, header: 139\n" +
                    "--- !!data #binary\n" +
                    "00000aa0             0b 0b 0b 0b  0b 0b 0b 0b                 ···· ····    \n" +
                    "# position: 2732, header: 140\n" +
                    "--- !!data #binary\n" +
                    "00000ab0 0c 0c 0c 0c 0c 0c 0c 0c                          ········         \n" +
                    "# position: 2744, header: 141\n" +
                    "--- !!data #binary\n" +
                    "00000ab0                                      0d 0d 0d 0d              ····\n" +
                    "00000ac0 0d 0d 0d 0d                                      ····             \n" +
                    "# position: 2756, header: 142\n" +
                    "--- !!data #binary\n" +
                    "00000ac0                          0e 0e 0e 0e 0e 0e 0e 0e          ········\n" +
                    "# position: 2768, header: 143\n" +
                    "--- !!data #binary\n" +
                    "00000ad0             0f 0f 0f 0f  0f 0f 0f 0f                 ···· ····    \n" +
                    "# position: 2780, header: 144\n" +
                    "--- !!data #binary\n" +
                    "00000ae0 10 10 10 10 10 10 10 10                          ········         \n" +
                    "# position: 2792, header: 145\n" +
                    "--- !!data #binary\n" +
                    "00000ae0                                      11 11 11 11              ····\n" +
                    "00000af0 11 11 11 11                                      ····             \n" +
                    "# position: 2804, header: 146\n" +
                    "--- !!data #binary\n" +
                    "00000af0                          12 12 12 12 12 12 12 12          ········\n" +
                    "# position: 2816, header: 147\n" +
                    "--- !!data #binary\n" +
                    "00000b00             13 13 13 13  13 13 13 13                 ···· ····    \n" +
                    "# position: 2828, header: 148\n" +
                    "--- !!data #binary\n" +
                    "00000b10 14 14 14 14 14 14 14 14                          ········         \n" +
                    "# position: 2840, header: 149\n" +
                    "--- !!data #binary\n" +
                    "00000b10                                      15 15 15 15              ····\n" +
                    "00000b20 15 15 15 15                                      ····             \n" +
                    "# position: 2852, header: 150\n" +
                    "--- !!data #binary\n" +
                    "00000b20                          16 16 16 16 16 16 16 16          ········\n" +
                    "# position: 2864, header: 151\n" +
                    "--- !!data #binary\n" +
                    "00000b30             17 17 17 17  17 17 17 17                 ···· ····    \n" +
                    "# position: 2876, header: 152\n" +
                    "--- !!data #binary\n" +
                    "00000b40 18 18 18 18 18 18 18 18                          ········         \n" +
                    "# position: 2888, header: 153\n" +
                    "--- !!data #binary\n" +
                    "00000b40                                      19 19 19 19              ····\n" +
                    "00000b50 19 19 19 19                                      ····             \n" +
                    "# position: 2900, header: 154\n" +
                    "--- !!data #binary\n" +
                    "00000b50                          1a 1a 1a 1a 1a 1a 1a 1a          ········\n" +
                    "# position: 2912, header: 155\n" +
                    "--- !!data #binary\n" +
                    "00000b60             1b 1b 1b 1b  1b 1b 1b 1b                 ···· ····    \n" +
                    "# position: 2924, header: 156\n" +
                    "--- !!data #binary\n" +
                    "00000b70 1c 1c 1c 1c 1c 1c 1c 1c                          ········         \n" +
                    "# position: 2936, header: 157\n" +
                    "--- !!data #binary\n" +
                    "00000b70                                      1d 1d 1d 1d              ····\n" +
                    "00000b80 1d 1d 1d 1d                                      ····             \n" +
                    "# position: 2948, header: 158\n" +
                    "--- !!data #binary\n" +
                    "00000b80                          1e 1e 1e 1e 1e 1e 1e 1e          ········\n" +
                    "# position: 2960, header: 159\n" +
                    "--- !!data #binary\n" +
                    "00000b90             1f 1f 1f 1f  1f 1f 1f 1f                 ···· ····    \n" +
                    "# position: 2972, header: 160\n" +
                    "--- !!data\n" +
                    "        \n" +
                    "# position: 2984, header: 161\n" +
                    "--- !!data\n" +
                    "!!!!!!!!\n" +
                    "# position: 2996, header: 162\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\"\"\"\"\n" +
                    "# position: 3008, header: 163\n" +
                    "--- !!data\n" +
                    "########\n" +
                    "# position: 3020, header: 164\n" +
                    "--- !!data\n" +
                    "$$$$$$$$\n" +
                    "# position: 3032, header: 165\n" +
                    "--- !!data\n" +
                    "%%%%%%%%\n" +
                    "# position: 3044, header: 166\n" +
                    "--- !!data\n" +
                    "&&&&&&&&\n" +
                    "# position: 3056, header: 167\n" +
                    "--- !!data\n" +
                    "''''''''\n" +
                    "# position: 3068, header: 168\n" +
                    "--- !!data\n" +
                    "((((((((\n" +
                    "# position: 3080, header: 169\n" +
                    "--- !!data\n" +
                    "))))))))\n" +
                    "# position: 3092, header: 170\n" +
                    "--- !!data\n" +
                    "********\n" +
                    "# position: 3104, header: 171\n" +
                    "--- !!data\n" +
                    "++++++++\n" +
                    "# position: 3116, header: 172\n" +
                    "--- !!data\n" +
                    ",,,,,,,,\n" +
                    "# position: 3128, header: 173\n" +
                    "--- !!data\n" +
                    "--------\n" +
                    "# position: 3140, header: 174\n" +
                    "--- !!data\n" +
                    "........\n" +
                    "# position: 3152, header: 175\n" +
                    "--- !!data\n" +
                    "////////\n" +
                    "# position: 3164, header: 176\n" +
                    "--- !!data\n" +
                    "00000000\n" +
                    "# position: 3176, header: 177\n" +
                    "--- !!data\n" +
                    "11111111\n" +
                    "# position: 3188, header: 178\n" +
                    "--- !!data\n" +
                    "22222222\n" +
                    "# position: 3200, header: 179\n" +
                    "--- !!data\n" +
                    "33333333\n" +
                    "# position: 3212, header: 180\n" +
                    "--- !!data\n" +
                    "44444444\n" +
                    "# position: 3224, header: 181\n" +
                    "--- !!data\n" +
                    "55555555\n" +
                    "# position: 3236, header: 182\n" +
                    "--- !!data\n" +
                    "66666666\n" +
                    "# position: 3248, header: 183\n" +
                    "--- !!data\n" +
                    "77777777\n" +
                    "# position: 3260, header: 184\n" +
                    "--- !!data\n" +
                    "88888888\n" +
                    "# position: 3272, header: 185\n" +
                    "--- !!data\n" +
                    "99999999\n" +
                    "# position: 3284, header: 186\n" +
                    "--- !!data\n" +
                    "::::::::\n" +
                    "# position: 3296, header: 187\n" +
                    "--- !!data\n" +
                    ";;;;;;;;\n" +
                    "# position: 3308, header: 188\n" +
                    "--- !!data\n" +
                    "<<<<<<<<\n" +
                    "# position: 3320, header: 189\n" +
                    "--- !!data\n" +
                    "========\n" +
                    "# position: 3332, header: 190\n" +
                    "--- !!data\n" +
                    ">>>>>>>>\n" +
                    "# position: 3344, header: 191\n" +
                    "--- !!data\n" +
                    "????????\n" +
                    "# position: 3356, header: 192\n" +
                    "--- !!data\n" +
                    "@@@@@@@@\n" +
                    "# position: 3368, header: 193\n" +
                    "--- !!data\n" +
                    "AAAAAAAA\n" +
                    "# position: 3380, header: 194\n" +
                    "--- !!data\n" +
                    "BBBBBBBB\n" +
                    "# position: 3392, header: 195\n" +
                    "--- !!data\n" +
                    "CCCCCCCC\n" +
                    "# position: 3404, header: 196\n" +
                    "--- !!data\n" +
                    "DDDDDDDD\n" +
                    "# position: 3416, header: 197\n" +
                    "--- !!data\n" +
                    "EEEEEEEE\n" +
                    "# position: 3428, header: 198\n" +
                    "--- !!data\n" +
                    "FFFFFFFF\n" +
                    "# position: 3440, header: 199\n" +
                    "--- !!data\n" +
                    "GGGGGGGG\n" +
                    "# position: 3452, header: 200\n" +
                    "--- !!data\n" +
                    "HHHHHHHH\n" +
                    "# position: 3464, header: 201\n" +
                    "--- !!data\n" +
                    "IIIIIIII\n" +
                    "# position: 3476, header: 202\n" +
                    "--- !!data\n" +
                    "JJJJJJJJ\n" +
                    "# position: 3488, header: 203\n" +
                    "--- !!data\n" +
                    "KKKKKKKK\n" +
                    "# position: 3500, header: 204\n" +
                    "--- !!data\n" +
                    "LLLLLLLL\n" +
                    "# position: 3512, header: 205\n" +
                    "--- !!data\n" +
                    "MMMMMMMM\n" +
                    "# position: 3524, header: 206\n" +
                    "--- !!data\n" +
                    "NNNNNNNN\n" +
                    "# position: 3536, header: 207\n" +
                    "--- !!data\n" +
                    "OOOOOOOO\n" +
                    "# position: 3548, header: 208\n" +
                    "--- !!data\n" +
                    "PPPPPPPP\n" +
                    "# position: 3560, header: 209\n" +
                    "--- !!data\n" +
                    "QQQQQQQQ\n" +
                    "# position: 3572, header: 210\n" +
                    "--- !!data\n" +
                    "RRRRRRRR\n" +
                    "# position: 3584, header: 211\n" +
                    "--- !!data\n" +
                    "SSSSSSSS\n" +
                    "# position: 3596, header: 212\n" +
                    "--- !!data\n" +
                    "TTTTTTTT\n" +
                    "# position: 3608, header: 213\n" +
                    "--- !!data\n" +
                    "UUUUUUUU\n" +
                    "# position: 3620, header: 214\n" +
                    "--- !!data\n" +
                    "VVVVVVVV\n" +
                    "# position: 3632, header: 215\n" +
                    "--- !!data\n" +
                    "WWWWWWWW\n" +
                    "# position: 3644, header: 216\n" +
                    "--- !!data\n" +
                    "XXXXXXXX\n" +
                    "# position: 3656, header: 217\n" +
                    "--- !!data\n" +
                    "YYYYYYYY\n" +
                    "# position: 3668, header: 218\n" +
                    "--- !!data\n" +
                    "ZZZZZZZZ\n" +
                    "# position: 3680, header: 219\n" +
                    "--- !!data\n" +
                    "[[[[[[[[\n" +
                    "# position: 3692, header: 220\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\\\\\\\\\n" +
                    "# position: 3704, header: 221\n" +
                    "--- !!data\n" +
                    "]]]]]]]]\n" +
                    "# position: 3716, header: 222\n" +
                    "--- !!data\n" +
                    "^^^^^^^^\n" +
                    "# position: 3728, header: 223\n" +
                    "--- !!data\n" +
                    "________\n" +
                    "# position: 3740, header: 224\n" +
                    "--- !!data\n" +
                    "````````\n" +
                    "# position: 3752, header: 225\n" +
                    "--- !!data\n" +
                    "aaaaaaaa\n" +
                    "# position: 3764, header: 226\n" +
                    "--- !!data\n" +
                    "bbbbbbbb\n" +
                    "# position: 3776, header: 227\n" +
                    "--- !!data\n" +
                    "cccccccc\n" +
                    "# position: 3788, header: 228\n" +
                    "--- !!data\n" +
                    "dddddddd\n" +
                    "# position: 3800, header: 229\n" +
                    "--- !!data\n" +
                    "eeeeeeee\n" +
                    "# position: 3812, header: 230\n" +
                    "--- !!data\n" +
                    "ffffffff\n" +
                    "# position: 3824, header: 231\n" +
                    "--- !!data\n" +
                    "gggggggg\n" +
                    "# position: 3836, header: 232\n" +
                    "--- !!data\n" +
                    "hhhhhhhh\n" +
                    "# position: 3848, header: 233\n" +
                    "--- !!data\n" +
                    "iiiiiiii\n" +
                    "# position: 3860, header: 234\n" +
                    "--- !!data\n" +
                    "jjjjjjjj\n" +
                    "# position: 3872, header: 235\n" +
                    "--- !!data\n" +
                    "kkkkkkkk\n" +
                    "# position: 3884, header: 236\n" +
                    "--- !!data\n" +
                    "llllllll\n" +
                    "# position: 3896, header: 237\n" +
                    "--- !!data\n" +
                    "mmmmmmmm\n" +
                    "# position: 3908, header: 238\n" +
                    "--- !!data\n" +
                    "nnnnnnnn\n" +
                    "# position: 3920, header: 239\n" +
                    "--- !!data\n" +
                    "oooooooo\n" +
                    "# position: 3932, header: 240\n" +
                    "--- !!data\n" +
                    "pppppppp\n" +
                    "# position: 3944, header: 241\n" +
                    "--- !!data\n" +
                    "qqqqqqqq\n" +
                    "# position: 3956, header: 242\n" +
                    "--- !!data\n" +
                    "rrrrrrrr\n" +
                    "# position: 3968, header: 243\n" +
                    "--- !!data\n" +
                    "ssssssss\n" +
                    "# position: 3980, header: 244\n" +
                    "--- !!data\n" +
                    "tttttttt\n" +
                    "# position: 3992, header: 245\n" +
                    "--- !!data\n" +
                    "uuuuuuuu\n" +
                    "# position: 4004, header: 246\n" +
                    "--- !!data\n" +
                    "vvvvvvvv\n" +
                    "# position: 4016, header: 247\n" +
                    "--- !!data\n" +
                    "wwwwwwww\n" +
                    "# position: 4028, header: 248\n" +
                    "--- !!data\n" +
                    "xxxxxxxx\n" +
                    "# position: 4040, header: 249\n" +
                    "--- !!data\n" +
                    "yyyyyyyy\n" +
                    "# position: 4052, header: 250\n" +
                    "--- !!data\n" +
                    "zzzzzzzz\n" +
                    "# position: 4064, header: 251\n" +
                    "--- !!data\n" +
                    "{{{{{{{{\n" +
                    "# position: 4076, header: 252\n" +
                    "--- !!data\n" +
                    "||||||||\n" +
                    "# position: 4088, header: 253\n" +
                    "--- !!data\n" +
                    "}}}}}}}}\n" +
                    "# position: 4100, header: 254\n" +
                    "--- !!data\n" +
                    "~~~~~~~~\n" +
                    "# position: 4112, header: 255\n" +
                    "--- !!data\n" +
                    "\u007F\u007F\u007F\u007F\u007F\u007F\u007F\u007F\n" +
                    "...\n" +
                    "# 126944 bytes remaining\n", queue.dump());

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