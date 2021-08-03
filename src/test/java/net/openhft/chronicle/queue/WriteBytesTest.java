/*
 * Copyright 2016-2020 https://chronicle.software
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
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;

@RequiredForClient
public class WriteBytesTest extends ChronicleQueueTestBase {
    final Bytes<?> outgoingBytes = Bytes.elasticByteBuffer();
    private final byte[] incomingMsgBytes = new byte[100];
    private final byte[] outgoingMsgBytes = new byte[100];

    @Test
    public void testWriteBytes() {
        File dir = getTmpDir();
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            ExcerptTailer tailer = queue.createTailer();

            outgoingMsgBytes[0] = 'A';
            outgoingBytes.write(outgoingMsgBytes);
            postOneMessage(appender);
            fetchOneMessage(tailer, incomingMsgBytes);
           // System.out.println(new String(incomingMsgBytes));

            outgoingBytes.clear();

            outgoingMsgBytes[0] = 'A';
            outgoingMsgBytes[1] = 'B';
            outgoingBytes.write(outgoingMsgBytes);

            postOneMessage(appender);
            fetchOneMessage(tailer, incomingMsgBytes);
           // System.out.println(new String(incomingMsgBytes));

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
        File dir = getTmpDir();
        try (ChronicleQueue queue = binary(dir)
                .testBlockSize()
                .rollCycle(TEST4_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"))
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
                byte finalI = (byte) i;
                appender.writeBytes(b ->
                        b.writeLong(finalI * 0x0101010101010101L));
            }

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: !int 86400000, format: yyyyMMdd'T4', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 176, header: 0\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "# position: 216, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "# position: 256, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 1\n" +
                    "# position: 288, header: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "# position: 328, header: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "# position: 368, header: 5\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "# position: 416, header: 6\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "...\n" +
                    "# 130596 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    4120,\n" +
                    "    17695265259775\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 256\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 196, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 2\n" +
                    "  488,\n" +
                    "  2324,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 488, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  776,\n" +
                    "  824,\n" +
                    "  872,\n" +
                    "  920,\n" +
                    "  968,\n" +
                    "  1016,\n" +
                    "  1064,\n" +
                    "  1112,\n" +
                    "  1160,\n" +
                    "  1208,\n" +
                    "  1256,\n" +
                    "  1304,\n" +
                    "  1352,\n" +
                    "  1400,\n" +
                    "  1448,\n" +
                    "  1496,\n" +
                    "  1544,\n" +
                    "  1592,\n" +
                    "  1640,\n" +
                    "  1688,\n" +
                    "  1736,\n" +
                    "  1784,\n" +
                    "  1832,\n" +
                    "  1880,\n" +
                    "  1928,\n" +
                    "  1976,\n" +
                    "  2024,\n" +
                    "  2072,\n" +
                    "  2120,\n" +
                    "  2168,\n" +
                    "  2216,\n" +
                    "  2264\n" +
                    "]\n" +
                    "# position: 776, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000300                                      80 7f 7f 7f              ····\n" +
                    "00000310 7f 7f 7f 7f                                      ····             \n" +
                    "# position: 788, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000310                          81 80 80 80 80 80 80 80          ········\n" +
                    "# position: 800, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000320             82 81 81 81  81 81 81 81                 ···· ····    \n" +
                    "# position: 812, header: 3\n" +
                    "--- !!data #binary\n" +
                    "00000330 83 82 82 82 82 82 82 82                          ········         \n" +
                    "# position: 824, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 836, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 848, header: 6\n" +
                    "--- !!data #binary\n" +
                    "# # BYTES_MARSHALLABLE\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 860, header: 7\n" +
                    "--- !!data #binary\n" +
                    "00000360 87 86 86 86 86 86 86 86                          ········         \n" +
                    "# position: 872, header: 8\n" +
                    "--- !!data #binary\n" +
                    "00000360                                      88 87 87 87              ····\n" +
                    "00000370 87 87 87 87                                      ····             \n" +
                    "# position: 884, header: 9\n" +
                    "--- !!data #binary\n" +
                    "00000370                          89 88 88 88 88 88 88 88          ········\n" +
                    "# position: 896, header: 10\n" +
                    "--- !!data #binary\n" +
                    "\"\\x89\\x89\\x89\\x89\\x89\\x89\\x89\"\n" +
                    "# position: 908, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "\"\\x8A\\x8A\\x8A\\x8A\\x8A\\x8A\"\n" +
                    "# position: 920, header: 12\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 932, header: 13\n" +
                    "--- !!data #binary\n" +
                    "000003a0                          8d 8c 8c 8c 8c 8c 8c 8c          ········\n" +
                    "# position: 944, header: 14\n" +
                    "--- !!data #binary\n" +
                    "000003b0             8e 8d 8d 8d  8d 8d 8d 8d                 ···· ····    \n" +
                    "# position: 956, header: 15\n" +
                    "--- !!data #binary\n" +
                    "000003c0 8f 8e 8e 8e 8e 8e 8e 8e                          ········         \n" +
                    "# position: 968, header: 16\n" +
                    "--- !!data #binary\n" +
                    "-1.4156185439721035E-29\n" +
                    "# position: 980, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "-5.702071897398123E-29\n" +
                    "# # EndOfFile\n" +
                    "# position: 992, header: 18\n" +
                    "--- !!data #binary\n" +
                    "-753555055760.82\n" +
                    "# position: 1004, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_2\n" +
                    "-48698841.79\n" +
                    "# position: 1016, header: 20\n" +
                    "--- !!data #binary\n" +
                    "-8422085917.3268\n" +
                    "# position: 1028, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_4\n" +
                    "-541098.24210\n" +
                    "# position: 1040, header: 22\n" +
                    "--- !!data #binary\n" +
                    "-93086212.770454\n" +
                    "# position: 1052, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_6\n" +
                    "-5952.080663\n" +
                    "# position: 1064, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 1076, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 1088, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 1100, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# position: 1112, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# position: 1124, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# position: 1136, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 1148, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 1160, header: 32\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 1172, header: 33\n" +
                    "--- !!data #binary\n" +
                    "!int 160\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# position: 1184, header: 34\n" +
                    "--- !!data #binary\n" +
                    "!int 41377\n" +
                    "!int 161\n" +
                    "!int 161\n" +
                    "!int -1\n" +
                    "# position: 1196, header: 35\n" +
                    "--- !!data #binary\n" +
                    "2728567458\n" +
                    "!int 41634\n" +
                    "# position: 1208, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "2745410467\n" +
                    "# # EndOfFile\n" +
                    "# position: 1220, header: 37\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte -92\n" +
                    "!byte -92\n" +
                    "!byte 0\n" +
                    "# position: 1232, header: 38\n" +
                    "--- !!data #binary\n" +
                    "!int -1515870811\n" +
                    "!short -23131\n" +
                    "# position: 1244, header: 39\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "!int -1499027802\n" +
                    "# # EndOfFile\n" +
                    "# position: 1256, header: 40\n" +
                    "--- !!data #binary\n" +
                    "!int 167\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# position: 1268, header: 41\n" +
                    "--- !!data #binary\n" +
                    "!int 43176\n" +
                    "!int 168\n" +
                    "!int 168\n" +
                    "!int -1\n" +
                    "# position: 1280, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # SET_LOW_INT16\n" +
                    "!int 43433\n" +
                    "!int 43433\n" +
                    "# position: 1292, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1304, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1316, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1328, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1340, header: 47\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1352, header: 48\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1364, header: 49\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1376, header: 50\n" +
                    "--- !!data #binary\n" +
                    "00000560             b2 b1 b1 b1  b1 b1 b1 b1                 ···· ····    \n" +
                    "# position: 1388, header: 51\n" +
                    "--- !!data #binary\n" +
                    "00000570 b3 b2 b2 b2 b2 b2 b2 b2                          ········         \n" +
                    "# position: 1400, header: 52\n" +
                    "--- !!data #binary\n" +
                    "00000570                                      b4 b3 b3 b3              ····\n" +
                    "00000580 b3 b3 b3 b3                                      ····             \n" +
                    "# position: 1412, header: 53\n" +
                    "--- !!data #binary\n" +
                    "00000580                          b5 b4 b4 b4 b4 b4 b4 b4          ········\n" +
                    "# position: 1424, header: 54\n" +
                    "--- !!data #binary\n" +
                    "00000590             b6 b5 b5 b5  b5 b5 b5 b5                 ···· ····    \n" +
                    "# position: 1436, header: 55\n" +
                    "--- !!data #binary\n" +
                    "000005a0 b7 b6 b6 b6 b6 b6 b6 b6                          ········         \n" +
                    "# position: 1448, header: 56\n" +
                    "--- !!data #binary\n" +
                    "000005a0                                      b8 b7 b7 b7              ····\n" +
                    "000005b0 b7 b7 b7 b7                                      ····             \n" +
                    "# position: 1460, header: 57\n" +
                    "--- !!data #binary\n" +
                    "000005b0                          b9 b8 b8 b8 b8 b8 b8 b8          ········\n" +
                    "# position: 1472, header: 58\n" +
                    "--- !!data #binary\n" +
                    "\"-252662577519802\": \n" +
                    "# position: 1484, header: 59\n" +
                    "--- !!data #binary\n" +
                    "!!null \"\"\n" +
                    "\"-2008556674363\": \n" +
                    "# position: 1496, header: 60\n" +
                    "--- !!data #binary\n" +
                    "000005d0                                      bc bb bb bb              ····\n" +
                    "000005e0 bb bb bb bb                                      ····             \n" +
                    "# position: 1508, header: 61\n" +
                    "--- !!data #binary\n" +
                    "000005e0                          bd bc bc bc bc bc bc bc          ········\n" +
                    "# position: 1520, header: 62\n" +
                    "--- !!data #binary\n" +
                    "000005f0             be bd bd bd  bd bd bd bd                 ···· ····    \n" +
                    "# position: 1532, header: 63\n" +
                    "--- !!data #binary\n" +
                    "00000600 bf be be be be be be be                          ········         \n" +
                    "# position: 1544, header: 64\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1556, header: 65\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC0\": \"\": \"\": \"\": \"\": \"\": \"\": \n" +
                    "# position: 1568, header: 66\n" +
                    "--- !!data #binary\n" +
                    "00000620             c2 c1 c1 c1  c1 c1 c1 c1                 ···· ····    \n" +
                    "# position: 1580, header: 67\n" +
                    "--- !!data #binary\n" +
                    "00000630 c3 c2 c2 c2 c2 c2 c2 c2                          ········         \n" +
                    "# position: 1592, header: 68\n" +
                    "--- !!data #binary\n" +
                    "00000630                                      c4 c3 c3 c3              ····\n" +
                    "00000640 c3 c3 c3 c3                                      ····             \n" +
                    "# position: 1604, header: 69\n" +
                    "--- !!data #binary\n" +
                    "00000640                          c5 c4 c4 c4 c4 c4 c4 c4          ········\n" +
                    "# position: 1616, header: 70\n" +
                    "--- !!data #binary\n" +
                    "00000650             c6 c5 c5 c5  c5 c5 c5 c5                 ···· ····    \n" +
                    "# position: 1628, header: 71\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\": \n" +
                    "# position: 1640, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000660                                      c8 c7 c7 c7              ····\n" +
                    "00000670 c7 c7 c7 c7                                      ····             \n" +
                    "# position: 1652, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000670                          c9 c8 c8 c8 c8 c8 c8 c8          ········\n" +
                    "# position: 1664, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000680             ca c9 c9 c9  c9 c9 c9 c9                 ···· ····    \n" +
                    "# position: 1676, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000690 cb ca ca ca ca ca ca ca                          ········         \n" +
                    "# position: 1688, header: 76\n" +
                    "--- !!data #binary\n" +
                    "00000690                                      cc cb cb cb              ····\n" +
                    "000006a0 cb cb cb cb                                      ····             \n" +
                    "# position: 1700, header: 77\n" +
                    "--- !!data #binary\n" +
                    "000006a0                          cd cc cc cc cc cc cc cc          ········\n" +
                    "# position: 1712, header: 78\n" +
                    "--- !!data #binary\n" +
                    "000006b0             ce cd cd cd  cd cd cd cd                 ···· ····    \n" +
                    "# position: 1724, header: 79\n" +
                    "--- !!data #binary\n" +
                    "000006c0 cf ce ce ce ce ce ce ce                          ········         \n" +
                    "# position: 1736, header: 80\n" +
                    "--- !!data #binary\n" +
                    "000006c0                                      d0 cf cf cf              ····\n" +
                    "000006d0 cf cf cf cf                                      ····             \n" +
                    "# position: 1748, header: 81\n" +
                    "--- !!data #binary\n" +
                    "000006d0                          d1 d0 d0 d0 d0 d0 d0 d0          ········\n" +
                    "# position: 1760, header: 82\n" +
                    "--- !!data #binary\n" +
                    "000006e0             d2 d1 d1 d1  d1 d1 d1 d1                 ···· ····    \n" +
                    "# position: 1772, header: 83\n" +
                    "--- !!data #binary\n" +
                    "000006f0 d3 d2 d2 d2 d2 d2 d2 d2                          ········         \n" +
                    "# position: 1784, header: 84\n" +
                    "--- !!data #binary\n" +
                    "000006f0                                      d4 d3 d3 d3              ····\n" +
                    "00000700 d3 d3 d3 d3                                      ····             \n" +
                    "# position: 1796, header: 85\n" +
                    "--- !!data #binary\n" +
                    "00000700                          d5 d4 d4 d4 d4 d4 d4 d4          ········\n" +
                    "# position: 1808, header: 86\n" +
                    "--- !!data #binary\n" +
                    "00000710             d6 d5 d5 d5  d5 d5 d5 d5                 ···· ····    \n" +
                    "# position: 1820, header: 87\n" +
                    "--- !!data #binary\n" +
                    "00000720 d7 d6 d6 d6 d6 d6 d6 d6                          ········         \n" +
                    "# position: 1832, header: 88\n" +
                    "--- !!data #binary\n" +
                    "00000720                                      d8 d7 d7 d7              ····\n" +
                    "00000730 d7 d7 d7 d7                                      ····             \n" +
                    "# position: 1844, header: 89\n" +
                    "--- !!data #binary\n" +
                    "00000730                          d9 d8 d8 d8 d8 d8 d8 d8          ········\n" +
                    "# position: 1856, header: 90\n" +
                    "--- !!data #binary\n" +
                    "00000740             da d9 d9 d9  d9 d9 d9 d9                 ···· ····    \n" +
                    "# position: 1868, header: 91\n" +
                    "--- !!data #binary\n" +
                    "00000750 db da da da da da da da                          ········         \n" +
                    "# position: 1880, header: 92\n" +
                    "--- !!data #binary\n" +
                    "00000750                                      dc db db db              ····\n" +
                    "00000760 db db db db                                      ····             \n" +
                    "# position: 1892, header: 93\n" +
                    "--- !!data #binary\n" +
                    "00000760                          dd dc dc dc dc dc dc dc          ········\n" +
                    "# position: 1904, header: 94\n" +
                    "--- !!data #binary\n" +
                    "00000770             de dd dd dd  dd dd dd dd                 ···· ····    \n" +
                    "# position: 1916, header: 95\n" +
                    "--- !!data #binary\n" +
                    "00000780 df de de de de de de de                          ········         \n" +
                    "# position: 1928, header: 96\n" +
                    "--- !!data #binary\n" +
                    "00000780                                      e0 df df df              ····\n" +
                    "00000790 df df df df                                      ····             \n" +
                    "# position: 1940, header: 97\n" +
                    "--- !!data #binary\n" +
                    "00000790                          e1 e0 e0 e0 e0 e0 e0 e0          ········\n" +
                    "# position: 1952, header: 98\n" +
                    "--- !!data #binary\n" +
                    "000007a0             e2 e1 e1 e1  e1 e1 e1 e1                 ···· ····    \n" +
                    "# position: 1964, header: 99\n" +
                    "--- !!data #binary\n" +
                    "000007b0 e3 e2 e2 e2 e2 e2 e2 e2                          ········         \n" +
                    "# position: 1976, header: 100\n" +
                    "--- !!data #binary\n" +
                    "000007b0                                      e4 e3 e3 e3              ····\n" +
                    "000007c0 e3 e3 e3 e3                                      ····             \n" +
                    "# position: 1988, header: 101\n" +
                    "--- !!data #binary\n" +
                    "000007c0                          e5 e4 e4 e4 e4 e4 e4 e4          ········\n" +
                    "# position: 2000, header: 102\n" +
                    "--- !!data #binary\n" +
                    "000007d0             e6 e5 e5 e5  e5 e5 e5 e5                 ···· ····    \n" +
                    "# position: 2012, header: 103\n" +
                    "--- !!data #binary\n" +
                    "000007e0 e7 e6 e6 e6 e6 e6 e6 e6                          ········         \n" +
                    "# position: 2024, header: 104\n" +
                    "--- !!data #binary\n" +
                    "000007e0                                      e8 e7 e7 e7              ····\n" +
                    "000007f0 e7 e7 e7 e7                                      ····             \n" +
                    "# position: 2036, header: 105\n" +
                    "--- !!data #binary\n" +
                    "000007f0                          e9 e8 e8 e8 e8 e8 e8 e8          ········\n" +
                    "# position: 2048, header: 106\n" +
                    "--- !!data #binary\n" +
                    "00000800             ea e9 e9 e9  e9 e9 e9 e9                 ···· ····    \n" +
                    "# position: 2060, header: 107\n" +
                    "--- !!data #binary\n" +
                    "00000810 eb ea ea ea ea ea ea ea                          ········         \n" +
                    "# position: 2072, header: 108\n" +
                    "--- !!data #binary\n" +
                    "00000810                                      ec eb eb eb              ····\n" +
                    "00000820 eb eb eb eb                                      ····             \n" +
                    "# position: 2084, header: 109\n" +
                    "--- !!data #binary\n" +
                    "00000820                          ed ec ec ec ec ec ec ec          ········\n" +
                    "# position: 2096, header: 110\n" +
                    "--- !!data #binary\n" +
                    "00000830             ee ed ed ed  ed ed ed ed                 ···· ····    \n" +
                    "# position: 2108, header: 111\n" +
                    "--- !!data #binary\n" +
                    "00000840 ef ee ee ee ee ee ee ee                          ········         \n" +
                    "# position: 2120, header: 112\n" +
                    "--- !!data #binary\n" +
                    "00000840                                      f0 ef ef ef              ····\n" +
                    "00000850 ef ef ef ef                                      ····             \n" +
                    "# position: 2132, header: 113\n" +
                    "--- !!data #binary\n" +
                    "00000850                          f1 f0 f0 f0 f0 f0 f0 f0          ········\n" +
                    "# position: 2144, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000860             f2 f1 f1 f1  f1 f1 f1 f1                 ···· ····    \n" +
                    "# position: 2156, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000870 f3 f2 f2 f2 f2 f2 f2 f2                          ········         \n" +
                    "# position: 2168, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000870                                      f4 f3 f3 f3              ····\n" +
                    "00000880 f3 f3 f3 f3                                      ····             \n" +
                    "# position: 2180, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000880                          f5 f4 f4 f4 f4 f4 f4 f4          ········\n" +
                    "# position: 2192, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000890             f6 f5 f5 f5  f5 f5 f5 f5                 ···· ····    \n" +
                    "# position: 2204, header: 119\n" +
                    "--- !!data #binary\n" +
                    "000008a0 f7 f6 f6 f6 f6 f6 f6 f6                          ········         \n" +
                    "# position: 2216, header: 120\n" +
                    "--- !!data #binary\n" +
                    "000008a0                                      f8 f7 f7 f7              ····\n" +
                    "000008b0 f7 f7 f7 f7                                      ····             \n" +
                    "# position: 2228, header: 121\n" +
                    "--- !!data #binary\n" +
                    "000008b0                          f9 f8 f8 f8 f8 f8 f8 f8          ········\n" +
                    "# position: 2240, header: 122\n" +
                    "--- !!data #binary\n" +
                    "000008c0             fa f9 f9 f9  f9 f9 f9 f9                 ···· ····    \n" +
                    "# position: 2252, header: 123\n" +
                    "--- !!data #binary\n" +
                    "000008d0 fb fa fa fa fa fa fa fa                          ········         \n" +
                    "# position: 2264, header: 124\n" +
                    "--- !!data #binary\n" +
                    "000008d0                                      fc fb fb fb              ····\n" +
                    "000008e0 fb fb fb fb                                      ····             \n" +
                    "# position: 2276, header: 125\n" +
                    "--- !!data #binary\n" +
                    "000008e0                          fd fc fc fc fc fc fc fc          ········\n" +
                    "# position: 2288, header: 126\n" +
                    "--- !!data #binary\n" +
                    "000008f0             fe fd fd fd  fd fd fd fd                 ···· ····    \n" +
                    "# position: 2300, header: 127\n" +
                    "--- !!data #binary\n" +
                    "00000900 ff fe fe fe fe fe fe fe                          ········         \n" +
                    "# position: 2312, header: 128\n" +
                    "--- !!data #binary\n" +
                    "00000900                                      00 00 00 00              ····\n" +
                    "00000910 00 00 00 00                                      ····             \n" +
                    "# position: 2324, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  2312,\n" +
                    "  2644,\n" +
                    "  2692,\n" +
                    "  2740,\n" +
                    "  2788,\n" +
                    "  2836,\n" +
                    "  2884,\n" +
                    "  2932,\n" +
                    "  2980,\n" +
                    "  3028,\n" +
                    "  3076,\n" +
                    "  3124,\n" +
                    "  3172,\n" +
                    "  3220,\n" +
                    "  3268,\n" +
                    "  3316,\n" +
                    "  3364,\n" +
                    "  3412,\n" +
                    "  3460,\n" +
                    "  3508,\n" +
                    "  3556,\n" +
                    "  3604,\n" +
                    "  3652,\n" +
                    "  3700,\n" +
                    "  3748,\n" +
                    "  3796,\n" +
                    "  3844,\n" +
                    "  3892,\n" +
                    "  3940,\n" +
                    "  3988,\n" +
                    "  4036,\n" +
                    "  4084\n" +
                    "]\n" +
                    "# position: 2608, header: 129\n" +
                    "--- !!data #binary\n" +
                    "00000a30             01 01 01 01  01 01 01 01                 ···· ····    \n" +
                    "# position: 2620, header: 130\n" +
                    "--- !!data #binary\n" +
                    "00000a40 02 02 02 02 02 02 02 02                          ········         \n" +
                    "# position: 2632, header: 131\n" +
                    "--- !!data #binary\n" +
                    "00000a40                                      03 03 03 03              ····\n" +
                    "00000a50 03 03 03 03                                      ····             \n" +
                    "# position: 2644, header: 132\n" +
                    "--- !!data #binary\n" +
                    "00000a50                          04 04 04 04 04 04 04 04          ········\n" +
                    "# position: 2656, header: 133\n" +
                    "--- !!data #binary\n" +
                    "00000a60             05 05 05 05  05 05 05 05                 ···· ····    \n" +
                    "# position: 2668, header: 134\n" +
                    "--- !!data #binary\n" +
                    "00000a70 06 06 06 06 06 06 06 06                          ········         \n" +
                    "# position: 2680, header: 135\n" +
                    "--- !!data #binary\n" +
                    "00000a70                                      07 07 07 07              ····\n" +
                    "00000a80 07 07 07 07                                      ····             \n" +
                    "# position: 2692, header: 136\n" +
                    "--- !!data #binary\n" +
                    "00000a80                          08 08 08 08 08 08 08 08          ········\n" +
                    "# position: 2704, header: 137\n" +
                    "--- !!data #binary\n" +
                    "00000a90             09 09 09 09  09 09 09 09                 ···· ····    \n" +
                    "# position: 2716, header: 138\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2728, header: 139\n" +
                    "--- !!data #binary\n" +
                    "00000aa0                                      0b 0b 0b 0b              ····\n" +
                    "00000ab0 0b 0b 0b 0b                                      ····             \n" +
                    "# position: 2740, header: 140\n" +
                    "--- !!data #binary\n" +
                    "00000ab0                          0c 0c 0c 0c 0c 0c 0c 0c          ········\n" +
                    "# position: 2752, header: 141\n" +
                    "--- !!data #binary\n" +
                    "00000ac0             0d 0d 0d 0d  0d 0d 0d 0d                 ···· ····    \n" +
                    "# position: 2764, header: 142\n" +
                    "--- !!data #binary\n" +
                    "00000ad0 0e 0e 0e 0e 0e 0e 0e 0e                          ········         \n" +
                    "# position: 2776, header: 143\n" +
                    "--- !!data #binary\n" +
                    "00000ad0                                      0f 0f 0f 0f              ····\n" +
                    "00000ae0 0f 0f 0f 0f                                      ····             \n" +
                    "# position: 2788, header: 144\n" +
                    "--- !!data #binary\n" +
                    "00000ae0                          10 10 10 10 10 10 10 10          ········\n" +
                    "# position: 2800, header: 145\n" +
                    "--- !!data #binary\n" +
                    "00000af0             11 11 11 11  11 11 11 11                 ···· ····    \n" +
                    "# position: 2812, header: 146\n" +
                    "--- !!data #binary\n" +
                    "00000b00 12 12 12 12 12 12 12 12                          ········         \n" +
                    "# position: 2824, header: 147\n" +
                    "--- !!data #binary\n" +
                    "00000b00                                      13 13 13 13              ····\n" +
                    "00000b10 13 13 13 13                                      ····             \n" +
                    "# position: 2836, header: 148\n" +
                    "--- !!data #binary\n" +
                    "00000b10                          14 14 14 14 14 14 14 14          ········\n" +
                    "# position: 2848, header: 149\n" +
                    "--- !!data #binary\n" +
                    "00000b20             15 15 15 15  15 15 15 15                 ···· ····    \n" +
                    "# position: 2860, header: 150\n" +
                    "--- !!data #binary\n" +
                    "00000b30 16 16 16 16 16 16 16 16                          ········         \n" +
                    "# position: 2872, header: 151\n" +
                    "--- !!data #binary\n" +
                    "00000b30                                      17 17 17 17              ····\n" +
                    "00000b40 17 17 17 17                                      ····             \n" +
                    "# position: 2884, header: 152\n" +
                    "--- !!data #binary\n" +
                    "00000b40                          18 18 18 18 18 18 18 18          ········\n" +
                    "# position: 2896, header: 153\n" +
                    "--- !!data #binary\n" +
                    "00000b50             19 19 19 19  19 19 19 19                 ···· ····    \n" +
                    "# position: 2908, header: 154\n" +
                    "--- !!data #binary\n" +
                    "00000b60 1a 1a 1a 1a 1a 1a 1a 1a                          ········         \n" +
                    "# position: 2920, header: 155\n" +
                    "--- !!data #binary\n" +
                    "00000b60                                      1b 1b 1b 1b              ····\n" +
                    "00000b70 1b 1b 1b 1b                                      ····             \n" +
                    "# position: 2932, header: 156\n" +
                    "--- !!data #binary\n" +
                    "00000b70                          1c 1c 1c 1c 1c 1c 1c 1c          ········\n" +
                    "# position: 2944, header: 157\n" +
                    "--- !!data #binary\n" +
                    "00000b80             1d 1d 1d 1d  1d 1d 1d 1d                 ···· ····    \n" +
                    "# position: 2956, header: 158\n" +
                    "--- !!data #binary\n" +
                    "00000b90 1e 1e 1e 1e 1e 1e 1e 1e                          ········         \n" +
                    "# position: 2968, header: 159\n" +
                    "--- !!data #binary\n" +
                    "00000b90                                      1f 1f 1f 1f              ····\n" +
                    "00000ba0 1f 1f 1f 1f                                      ····             \n" +
                    "# position: 2980, header: 160\n" +
                    "--- !!data\n" +
                    "        \n" +
                    "# position: 2992, header: 161\n" +
                    "--- !!data\n" +
                    "!!!!!!!!\n" +
                    "# position: 3004, header: 162\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\"\"\"\"\n" +
                    "# position: 3016, header: 163\n" +
                    "--- !!data\n" +
                    "########\n" +
                    "# position: 3028, header: 164\n" +
                    "--- !!data\n" +
                    "$$$$$$$$\n" +
                    "# position: 3040, header: 165\n" +
                    "--- !!data\n" +
                    "%%%%%%%%\n" +
                    "# position: 3052, header: 166\n" +
                    "--- !!data\n" +
                    "&&&&&&&&\n" +
                    "# position: 3064, header: 167\n" +
                    "--- !!data\n" +
                    "''''''''\n" +
                    "# position: 3076, header: 168\n" +
                    "--- !!data\n" +
                    "((((((((\n" +
                    "# position: 3088, header: 169\n" +
                    "--- !!data\n" +
                    "))))))))\n" +
                    "# position: 3100, header: 170\n" +
                    "--- !!data\n" +
                    "********\n" +
                    "# position: 3112, header: 171\n" +
                    "--- !!data\n" +
                    "++++++++\n" +
                    "# position: 3124, header: 172\n" +
                    "--- !!data\n" +
                    ",,,,,,,,\n" +
                    "# position: 3136, header: 173\n" +
                    "--- !!data\n" +
                    "--------\n" +
                    "# position: 3148, header: 174\n" +
                    "--- !!data\n" +
                    "........\n" +
                    "# position: 3160, header: 175\n" +
                    "--- !!data\n" +
                    "////////\n" +
                    "# position: 3172, header: 176\n" +
                    "--- !!data\n" +
                    "00000000\n" +
                    "# position: 3184, header: 177\n" +
                    "--- !!data\n" +
                    "11111111\n" +
                    "# position: 3196, header: 178\n" +
                    "--- !!data\n" +
                    "22222222\n" +
                    "# position: 3208, header: 179\n" +
                    "--- !!data\n" +
                    "33333333\n" +
                    "# position: 3220, header: 180\n" +
                    "--- !!data\n" +
                    "44444444\n" +
                    "# position: 3232, header: 181\n" +
                    "--- !!data\n" +
                    "55555555\n" +
                    "# position: 3244, header: 182\n" +
                    "--- !!data\n" +
                    "66666666\n" +
                    "# position: 3256, header: 183\n" +
                    "--- !!data\n" +
                    "77777777\n" +
                    "# position: 3268, header: 184\n" +
                    "--- !!data\n" +
                    "88888888\n" +
                    "# position: 3280, header: 185\n" +
                    "--- !!data\n" +
                    "99999999\n" +
                    "# position: 3292, header: 186\n" +
                    "--- !!data\n" +
                    "::::::::\n" +
                    "# position: 3304, header: 187\n" +
                    "--- !!data\n" +
                    ";;;;;;;;\n" +
                    "# position: 3316, header: 188\n" +
                    "--- !!data\n" +
                    "<<<<<<<<\n" +
                    "# position: 3328, header: 189\n" +
                    "--- !!data\n" +
                    "========\n" +
                    "# position: 3340, header: 190\n" +
                    "--- !!data\n" +
                    ">>>>>>>>\n" +
                    "# position: 3352, header: 191\n" +
                    "--- !!data\n" +
                    "????????\n" +
                    "# position: 3364, header: 192\n" +
                    "--- !!data\n" +
                    "@@@@@@@@\n" +
                    "# position: 3376, header: 193\n" +
                    "--- !!data\n" +
                    "AAAAAAAA\n" +
                    "# position: 3388, header: 194\n" +
                    "--- !!data\n" +
                    "BBBBBBBB\n" +
                    "# position: 3400, header: 195\n" +
                    "--- !!data\n" +
                    "CCCCCCCC\n" +
                    "# position: 3412, header: 196\n" +
                    "--- !!data\n" +
                    "DDDDDDDD\n" +
                    "# position: 3424, header: 197\n" +
                    "--- !!data\n" +
                    "EEEEEEEE\n" +
                    "# position: 3436, header: 198\n" +
                    "--- !!data\n" +
                    "FFFFFFFF\n" +
                    "# position: 3448, header: 199\n" +
                    "--- !!data\n" +
                    "GGGGGGGG\n" +
                    "# position: 3460, header: 200\n" +
                    "--- !!data\n" +
                    "HHHHHHHH\n" +
                    "# position: 3472, header: 201\n" +
                    "--- !!data\n" +
                    "IIIIIIII\n" +
                    "# position: 3484, header: 202\n" +
                    "--- !!data\n" +
                    "JJJJJJJJ\n" +
                    "# position: 3496, header: 203\n" +
                    "--- !!data\n" +
                    "KKKKKKKK\n" +
                    "# position: 3508, header: 204\n" +
                    "--- !!data\n" +
                    "LLLLLLLL\n" +
                    "# position: 3520, header: 205\n" +
                    "--- !!data\n" +
                    "MMMMMMMM\n" +
                    "# position: 3532, header: 206\n" +
                    "--- !!data\n" +
                    "NNNNNNNN\n" +
                    "# position: 3544, header: 207\n" +
                    "--- !!data\n" +
                    "OOOOOOOO\n" +
                    "# position: 3556, header: 208\n" +
                    "--- !!data\n" +
                    "PPPPPPPP\n" +
                    "# position: 3568, header: 209\n" +
                    "--- !!data\n" +
                    "QQQQQQQQ\n" +
                    "# position: 3580, header: 210\n" +
                    "--- !!data\n" +
                    "RRRRRRRR\n" +
                    "# position: 3592, header: 211\n" +
                    "--- !!data\n" +
                    "SSSSSSSS\n" +
                    "# position: 3604, header: 212\n" +
                    "--- !!data\n" +
                    "TTTTTTTT\n" +
                    "# position: 3616, header: 213\n" +
                    "--- !!data\n" +
                    "UUUUUUUU\n" +
                    "# position: 3628, header: 214\n" +
                    "--- !!data\n" +
                    "VVVVVVVV\n" +
                    "# position: 3640, header: 215\n" +
                    "--- !!data\n" +
                    "WWWWWWWW\n" +
                    "# position: 3652, header: 216\n" +
                    "--- !!data\n" +
                    "XXXXXXXX\n" +
                    "# position: 3664, header: 217\n" +
                    "--- !!data\n" +
                    "YYYYYYYY\n" +
                    "# position: 3676, header: 218\n" +
                    "--- !!data\n" +
                    "ZZZZZZZZ\n" +
                    "# position: 3688, header: 219\n" +
                    "--- !!data\n" +
                    "[[[[[[[[\n" +
                    "# position: 3700, header: 220\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\\\\\\\\\n" +
                    "# position: 3712, header: 221\n" +
                    "--- !!data\n" +
                    "]]]]]]]]\n" +
                    "# position: 3724, header: 222\n" +
                    "--- !!data\n" +
                    "^^^^^^^^\n" +
                    "# position: 3736, header: 223\n" +
                    "--- !!data\n" +
                    "________\n" +
                    "# position: 3748, header: 224\n" +
                    "--- !!data\n" +
                    "````````\n" +
                    "# position: 3760, header: 225\n" +
                    "--- !!data\n" +
                    "aaaaaaaa\n" +
                    "# position: 3772, header: 226\n" +
                    "--- !!data\n" +
                    "bbbbbbbb\n" +
                    "# position: 3784, header: 227\n" +
                    "--- !!data\n" +
                    "cccccccc\n" +
                    "# position: 3796, header: 228\n" +
                    "--- !!data\n" +
                    "dddddddd\n" +
                    "# position: 3808, header: 229\n" +
                    "--- !!data\n" +
                    "eeeeeeee\n" +
                    "# position: 3820, header: 230\n" +
                    "--- !!data\n" +
                    "ffffffff\n" +
                    "# position: 3832, header: 231\n" +
                    "--- !!data\n" +
                    "gggggggg\n" +
                    "# position: 3844, header: 232\n" +
                    "--- !!data\n" +
                    "hhhhhhhh\n" +
                    "# position: 3856, header: 233\n" +
                    "--- !!data\n" +
                    "iiiiiiii\n" +
                    "# position: 3868, header: 234\n" +
                    "--- !!data\n" +
                    "jjjjjjjj\n" +
                    "# position: 3880, header: 235\n" +
                    "--- !!data\n" +
                    "kkkkkkkk\n" +
                    "# position: 3892, header: 236\n" +
                    "--- !!data\n" +
                    "llllllll\n" +
                    "# position: 3904, header: 237\n" +
                    "--- !!data\n" +
                    "mmmmmmmm\n" +
                    "# position: 3916, header: 238\n" +
                    "--- !!data\n" +
                    "nnnnnnnn\n" +
                    "# position: 3928, header: 239\n" +
                    "--- !!data\n" +
                    "oooooooo\n" +
                    "# position: 3940, header: 240\n" +
                    "--- !!data\n" +
                    "pppppppp\n" +
                    "# position: 3952, header: 241\n" +
                    "--- !!data\n" +
                    "qqqqqqqq\n" +
                    "# position: 3964, header: 242\n" +
                    "--- !!data\n" +
                    "rrrrrrrr\n" +
                    "# position: 3976, header: 243\n" +
                    "--- !!data\n" +
                    "ssssssss\n" +
                    "# position: 3988, header: 244\n" +
                    "--- !!data\n" +
                    "tttttttt\n" +
                    "# position: 4000, header: 245\n" +
                    "--- !!data\n" +
                    "uuuuuuuu\n" +
                    "# position: 4012, header: 246\n" +
                    "--- !!data\n" +
                    "vvvvvvvv\n" +
                    "# position: 4024, header: 247\n" +
                    "--- !!data\n" +
                    "wwwwwwww\n" +
                    "# position: 4036, header: 248\n" +
                    "--- !!data\n" +
                    "xxxxxxxx\n" +
                    "# position: 4048, header: 249\n" +
                    "--- !!data\n" +
                    "yyyyyyyy\n" +
                    "# position: 4060, header: 250\n" +
                    "--- !!data\n" +
                    "zzzzzzzz\n" +
                    "# position: 4072, header: 251\n" +
                    "--- !!data\n" +
                    "{{{{{{{{\n" +
                    "# position: 4084, header: 252\n" +
                    "--- !!data\n" +
                    "||||||||\n" +
                    "# position: 4096, header: 253\n" +
                    "--- !!data\n" +
                    "}}}}}}}}\n" +
                    "# position: 4108, header: 254\n" +
                    "--- !!data\n" +
                    "~~~~~~~~\n" +
                    "# position: 4120, header: 255\n" +
                    "--- !!data\n" +
                    "\u007F\u007F\u007F\u007F\u007F\u007F\u007F\u007F\n" +
                    "...\n" +
                    "# 126936 bytes remaining\n", queue.dump());

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

    @Override
    public void assertReferencesReleased(){
        outgoingBytes.releaseLast();
        super.assertReferencesReleased();
    }
}
