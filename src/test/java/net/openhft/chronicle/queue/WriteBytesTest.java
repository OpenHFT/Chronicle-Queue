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
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
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
        final SingleChronicleQueueBuilder builder = binary(dir)
                .testBlockSize()
                .rollCycle(TEST4_DAILY)
                .timeProvider(new SetTimeProvider("2020/10/19T01:01:01"));
        try (ChronicleQueue queue = builder
                .build()) {
            final boolean useSparseFiles = builder.useSparseFiles();

            ExcerptAppender appender = queue.acquireAppender();
            for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
                byte finalI = (byte) i;
                appender.writeBytes(b ->
                        b.writeLong(finalI * 0x0101010101010101L));
            }

            assertEquals("" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T4', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 180, header: 0\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 18554\n" +
                    "# position: 216, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 18554\n" +
                    "# position: 256, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 3\n" +
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
                    "    4128,\n" +
                    "    17729624998143\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 256\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 2\n" +
                    "  496,\n" +
                    "  2332,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 496, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  784,\n" +
                    "  832,\n" +
                    "  880,\n" +
                    "  928,\n" +
                    "  976,\n" +
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
                    "  2272\n" +
                    "]\n" +
                    "# position: 784, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000310             80 7f 7f 7f  7f 7f 7f 7f                 ···· ····    \n" +
                    "# position: 796, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000320 81 80 80 80 80 80 80 80                          ········         \n" +
                    "# position: 808, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000320                                      82 81 81 81              ····\n" +
                    "00000330 81 81 81 81                                      ····             \n" +
                    "# position: 820, header: 3\n" +
                    "--- !!data #binary\n" +
                    "00000330                          83 82 82 82 82 82 82 82          ········\n" +
                    "# position: 832, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 844, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 856, header: 6\n" +
                    "--- !!data #binary\n" +
                    "# # BYTES_MARSHALLABLE\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 868, header: 7\n" +
                    "--- !!data #binary\n" +
                    "00000360                          87 86 86 86 86 86 86 86          ········\n" +
                    "# position: 880, header: 8\n" +
                    "--- !!data #binary\n" +
                    "00000370             88 87 87 87  87 87 87 87                 ···· ····    \n" +
                    "# position: 892, header: 9\n" +
                    "--- !!data #binary\n" +
                    "00000380 89 88 88 88 88 88 88 88                          ········         \n" +
                    "# position: 904, header: 10\n" +
                    "--- !!data #binary\n" +
                    "\"\\x89\\x89\\x89\\x89\\x89\\x89\\x89\"\n" +
                    "# position: 916, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "\"\\x8A\\x8A\\x8A\\x8A\\x8A\\x8A\"\n" +
                    "# position: 928, header: 12\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 940, header: 13\n" +
                    "--- !!data #binary\n" +
                    "000003b0 8d 8c 8c 8c 8c 8c 8c 8c                          ········         \n" +
                    "# position: 952, header: 14\n" +
                    "--- !!data #binary\n" +
                    "000003b0                                      8e 8d 8d 8d              ····\n" +
                    "000003c0 8d 8d 8d 8d                                      ····             \n" +
                    "# position: 964, header: 15\n" +
                    "--- !!data #binary\n" +
                    "000003c0                          8f 8e 8e 8e 8e 8e 8e 8e          ········\n" +
                    "# position: 976, header: 16\n" +
                    "--- !!data #binary\n" +
                    "-1.4156185439721035E-29\n" +
                    "# position: 988, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "-5.702071897398123E-29\n" +
                    "# # EndOfFile\n" +
                    "# position: 1000, header: 18\n" +
                    "--- !!data #binary\n" +
                    "# # EndOfFile\n" +
                    "# position: 1012, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_2\n" +
                    "# # EndOfFile\n" +
                    "# position: 1024, header: 20\n" +
                    "--- !!data #binary\n" +
                    "# # EndOfFile\n" +
                    "# position: 1036, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_4\n" +
                    "# # EndOfFile\n" +
                    "# position: 1048, header: 22\n" +
                    "--- !!data #binary\n" +
                    "# # EndOfFile\n" +
                    "# position: 1060, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_6\n" +
                    "# # EndOfFile\n" +
                    "# position: 1072, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 1084, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 1096, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 1108, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# position: 1120, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# position: 1132, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# position: 1144, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 1156, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 1168, header: 32\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 1180, header: 33\n" +
                    "--- !!data #binary\n" +
                    "160\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# position: 1192, header: 34\n" +
                    "--- !!data #binary\n" +
                    "41377\n" +
                    "161\n" +
                    "161\n" +
                    "-1\n" +
                    "# position: 1204, header: 35\n" +
                    "--- !!data #binary\n" +
                    "2728567458\n" +
                    "41634\n" +
                    "# position: 1216, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "2745410467\n" +
                    "# # EndOfFile\n" +
                    "# position: 1228, header: 37\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte -92\n" +
                    "!byte -92\n" +
                    "# # EndOfFile\n" +
                    "# position: 1240, header: 38\n" +
                    "--- !!data #binary\n" +
                    "-1515870811\n" +
                    "!short -23131\n" +
                    "# position: 1252, header: 39\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "-1499027802\n" +
                    "# # EndOfFile\n" +
                    "# position: 1264, header: 40\n" +
                    "--- !!data #binary\n" +
                    "167\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# position: 1276, header: 41\n" +
                    "--- !!data #binary\n" +
                    "43176\n" +
                    "168\n" +
                    "168\n" +
                    "-1\n" +
                    "# position: 1288, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # SET_LOW_INT16\n" +
                    "43433\n" +
                    "43433\n" +
                    "# position: 1300, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1312, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1324, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1336, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1348, header: 47\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1360, header: 48\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1372, header: 49\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1384, header: 50\n" +
                    "--- !!data #binary\n" +
                    "00000560                                      b2 b1 b1 b1              ····\n" +
                    "00000570 b1 b1 b1 b1                                      ····             \n" +
                    "# position: 1396, header: 51\n" +
                    "--- !!data #binary\n" +
                    "00000570                          b3 b2 b2 b2 b2 b2 b2 b2          ········\n" +
                    "# position: 1408, header: 52\n" +
                    "--- !!data #binary\n" +
                    "00000580             b4 b3 b3 b3  b3 b3 b3 b3                 ···· ····    \n" +
                    "# position: 1420, header: 53\n" +
                    "--- !!data #binary\n" +
                    "00000590 b5 b4 b4 b4 b4 b4 b4 b4                          ········         \n" +
                    "# position: 1432, header: 54\n" +
                    "--- !!data #binary\n" +
                    "00000590                                      b6 b5 b5 b5              ····\n" +
                    "000005a0 b5 b5 b5 b5                                      ····             \n" +
                    "# position: 1444, header: 55\n" +
                    "--- !!data #binary\n" +
                    "000005a0                          b7 b6 b6 b6 b6 b6 b6 b6          ········\n" +
                    "# position: 1456, header: 56\n" +
                    "--- !!data #binary\n" +
                    "000005b0             b8 b7 b7 b7  b7 b7 b7 b7                 ···· ····    \n" +
                    "# position: 1468, header: 57\n" +
                    "--- !!data #binary\n" +
                    "000005c0 b9 b8 b8 b8 b8 b8 b8 b8                          ········         \n" +
                    "# position: 1480, header: 58\n" +
                    "--- !!data #binary\n" +
                    "000005c0                                      ba b9 b9 b9              ····\n" +
                    "000005d0 b9 b9 b9 b9                                      ····             \n" +
                    "# position: 1492, header: 59\n" +
                    "--- !!data #binary\n" +
                    "000005d0                          bb ba ba ba ba ba ba ba          ········\n" +
                    "# position: 1504, header: 60\n" +
                    "--- !!data #binary\n" +
                    "000005e0             bc bb bb bb  bb bb bb bb                 ···· ····    \n" +
                    "# position: 1516, header: 61\n" +
                    "--- !!data #binary\n" +
                    "000005f0 bd bc bc bc bc bc bc bc                          ········         \n" +
                    "# position: 1528, header: 62\n" +
                    "--- !!data #binary\n" +
                    "000005f0                                      be bd bd bd              ····\n" +
                    "00000600 bd bd bd bd                                      ····             \n" +
                    "# position: 1540, header: 63\n" +
                    "--- !!data #binary\n" +
                    "00000600                          bf be be be be be be be          ········\n" +
                    "# position: 1552, header: 64\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1564, header: 65\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC0\": \"\": \"\": \"\": \"\": \"\": \"\": \n" +
                    "# position: 1576, header: 66\n" +
                    "--- !!data #binary\n" +
                    "00000620                                      c2 c1 c1 c1              ····\n" +
                    "00000630 c1 c1 c1 c1                                      ····             \n" +
                    "# position: 1588, header: 67\n" +
                    "--- !!data #binary\n" +
                    "00000630                          c3 c2 c2 c2 c2 c2 c2 c2          ········\n" +
                    "# position: 1600, header: 68\n" +
                    "--- !!data #binary\n" +
                    "00000640             c4 c3 c3 c3  c3 c3 c3 c3                 ···· ····    \n" +
                    "# position: 1612, header: 69\n" +
                    "--- !!data #binary\n" +
                    "00000650 c5 c4 c4 c4 c4 c4 c4 c4                          ········         \n" +
                    "# position: 1624, header: 70\n" +
                    "--- !!data #binary\n" +
                    "00000650                                      c6 c5 c5 c5              ····\n" +
                    "00000660 c5 c5 c5 c5                                      ····             \n" +
                    "# position: 1636, header: 71\n" +
                    "--- !!data #binary\n" +
                    "\"\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\\xC6\": \n" +
                    "# position: 1648, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000670             c8 c7 c7 c7  c7 c7 c7 c7                 ···· ····    \n" +
                    "# position: 1660, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000680 c9 c8 c8 c8 c8 c8 c8 c8                          ········         \n" +
                    "# position: 1672, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000680                                      ca c9 c9 c9              ····\n" +
                    "00000690 c9 c9 c9 c9                                      ····             \n" +
                    "# position: 1684, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000690                          cb ca ca ca ca ca ca ca          ········\n" +
                    "# position: 1696, header: 76\n" +
                    "--- !!data #binary\n" +
                    "000006a0             cc cb cb cb  cb cb cb cb                 ···· ····    \n" +
                    "# position: 1708, header: 77\n" +
                    "--- !!data #binary\n" +
                    "000006b0 cd cc cc cc cc cc cc cc                          ········         \n" +
                    "# position: 1720, header: 78\n" +
                    "--- !!data #binary\n" +
                    "000006b0                                      ce cd cd cd              ····\n" +
                    "000006c0 cd cd cd cd                                      ····             \n" +
                    "# position: 1732, header: 79\n" +
                    "--- !!data #binary\n" +
                    "000006c0                          cf ce ce ce ce ce ce ce          ········\n" +
                    "# position: 1744, header: 80\n" +
                    "--- !!data #binary\n" +
                    "000006d0             d0 cf cf cf  cf cf cf cf                 ···· ····    \n" +
                    "# position: 1756, header: 81\n" +
                    "--- !!data #binary\n" +
                    "000006e0 d1 d0 d0 d0 d0 d0 d0 d0                          ········         \n" +
                    "# position: 1768, header: 82\n" +
                    "--- !!data #binary\n" +
                    "000006e0                                      d2 d1 d1 d1              ····\n" +
                    "000006f0 d1 d1 d1 d1                                      ····             \n" +
                    "# position: 1780, header: 83\n" +
                    "--- !!data #binary\n" +
                    "000006f0                          d3 d2 d2 d2 d2 d2 d2 d2          ········\n" +
                    "# position: 1792, header: 84\n" +
                    "--- !!data #binary\n" +
                    "00000700             d4 d3 d3 d3  d3 d3 d3 d3                 ···· ····    \n" +
                    "# position: 1804, header: 85\n" +
                    "--- !!data #binary\n" +
                    "00000710 d5 d4 d4 d4 d4 d4 d4 d4                          ········         \n" +
                    "# position: 1816, header: 86\n" +
                    "--- !!data #binary\n" +
                    "00000710                                      d6 d5 d5 d5              ····\n" +
                    "00000720 d5 d5 d5 d5                                      ····             \n" +
                    "# position: 1828, header: 87\n" +
                    "--- !!data #binary\n" +
                    "00000720                          d7 d6 d6 d6 d6 d6 d6 d6          ········\n" +
                    "# position: 1840, header: 88\n" +
                    "--- !!data #binary\n" +
                    "00000730             d8 d7 d7 d7  d7 d7 d7 d7                 ···· ····    \n" +
                    "# position: 1852, header: 89\n" +
                    "--- !!data #binary\n" +
                    "00000740 d9 d8 d8 d8 d8 d8 d8 d8                          ········         \n" +
                    "# position: 1864, header: 90\n" +
                    "--- !!data #binary\n" +
                    "00000740                                      da d9 d9 d9              ····\n" +
                    "00000750 d9 d9 d9 d9                                      ····             \n" +
                    "# position: 1876, header: 91\n" +
                    "--- !!data #binary\n" +
                    "00000750                          db da da da da da da da          ········\n" +
                    "# position: 1888, header: 92\n" +
                    "--- !!data #binary\n" +
                    "00000760             dc db db db  db db db db                 ···· ····    \n" +
                    "# position: 1900, header: 93\n" +
                    "--- !!data #binary\n" +
                    "00000770 dd dc dc dc dc dc dc dc                          ········         \n" +
                    "# position: 1912, header: 94\n" +
                    "--- !!data #binary\n" +
                    "00000770                                      de dd dd dd              ····\n" +
                    "00000780 dd dd dd dd                                      ····             \n" +
                    "# position: 1924, header: 95\n" +
                    "--- !!data #binary\n" +
                    "00000780                          df de de de de de de de          ········\n" +
                    "# position: 1936, header: 96\n" +
                    "--- !!data #binary\n" +
                    "00000790             e0 df df df  df df df df                 ···· ····    \n" +
                    "# position: 1948, header: 97\n" +
                    "--- !!data #binary\n" +
                    "000007a0 e1 e0 e0 e0 e0 e0 e0 e0                          ········         \n" +
                    "# position: 1960, header: 98\n" +
                    "--- !!data #binary\n" +
                    "000007a0                                      e2 e1 e1 e1              ····\n" +
                    "000007b0 e1 e1 e1 e1                                      ····             \n" +
                    "# position: 1972, header: 99\n" +
                    "--- !!data #binary\n" +
                    "000007b0                          e3 e2 e2 e2 e2 e2 e2 e2          ········\n" +
                    "# position: 1984, header: 100\n" +
                    "--- !!data #binary\n" +
                    "000007c0             e4 e3 e3 e3  e3 e3 e3 e3                 ···· ····    \n" +
                    "# position: 1996, header: 101\n" +
                    "--- !!data #binary\n" +
                    "000007d0 e5 e4 e4 e4 e4 e4 e4 e4                          ········         \n" +
                    "# position: 2008, header: 102\n" +
                    "--- !!data #binary\n" +
                    "000007d0                                      e6 e5 e5 e5              ····\n" +
                    "000007e0 e5 e5 e5 e5                                      ····             \n" +
                    "# position: 2020, header: 103\n" +
                    "--- !!data #binary\n" +
                    "000007e0                          e7 e6 e6 e6 e6 e6 e6 e6          ········\n" +
                    "# position: 2032, header: 104\n" +
                    "--- !!data #binary\n" +
                    "000007f0             e8 e7 e7 e7  e7 e7 e7 e7                 ···· ····    \n" +
                    "# position: 2044, header: 105\n" +
                    "--- !!data #binary\n" +
                    "00000800 e9 e8 e8 e8 e8 e8 e8 e8                          ········         \n" +
                    "# position: 2056, header: 106\n" +
                    "--- !!data #binary\n" +
                    "00000800                                      ea e9 e9 e9              ····\n" +
                    "00000810 e9 e9 e9 e9                                      ····             \n" +
                    "# position: 2068, header: 107\n" +
                    "--- !!data #binary\n" +
                    "00000810                          eb ea ea ea ea ea ea ea          ········\n" +
                    "# position: 2080, header: 108\n" +
                    "--- !!data #binary\n" +
                    "00000820             ec eb eb eb  eb eb eb eb                 ···· ····    \n" +
                    "# position: 2092, header: 109\n" +
                    "--- !!data #binary\n" +
                    "00000830 ed ec ec ec ec ec ec ec                          ········         \n" +
                    "# position: 2104, header: 110\n" +
                    "--- !!data #binary\n" +
                    "00000830                                      ee ed ed ed              ····\n" +
                    "00000840 ed ed ed ed                                      ····             \n" +
                    "# position: 2116, header: 111\n" +
                    "--- !!data #binary\n" +
                    "00000840                          ef ee ee ee ee ee ee ee          ········\n" +
                    "# position: 2128, header: 112\n" +
                    "--- !!data #binary\n" +
                    "00000850             f0 ef ef ef  ef ef ef ef                 ···· ····    \n" +
                    "# position: 2140, header: 113\n" +
                    "--- !!data #binary\n" +
                    "00000860 f1 f0 f0 f0 f0 f0 f0 f0                          ········         \n" +
                    "# position: 2152, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000860                                      f2 f1 f1 f1              ····\n" +
                    "00000870 f1 f1 f1 f1                                      ····             \n" +
                    "# position: 2164, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000870                          f3 f2 f2 f2 f2 f2 f2 f2          ········\n" +
                    "# position: 2176, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000880             f4 f3 f3 f3  f3 f3 f3 f3                 ···· ····    \n" +
                    "# position: 2188, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000890 f5 f4 f4 f4 f4 f4 f4 f4                          ········         \n" +
                    "# position: 2200, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000890                                      f6 f5 f5 f5              ····\n" +
                    "000008a0 f5 f5 f5 f5                                      ····             \n" +
                    "# position: 2212, header: 119\n" +
                    "--- !!data #binary\n" +
                    "000008a0                          f7 f6 f6 f6 f6 f6 f6 f6          ········\n" +
                    "# position: 2224, header: 120\n" +
                    "--- !!data #binary\n" +
                    "000008b0             f8 f7 f7 f7  f7 f7 f7 f7                 ···· ····    \n" +
                    "# position: 2236, header: 121\n" +
                    "--- !!data #binary\n" +
                    "000008c0 f9 f8 f8 f8 f8 f8 f8 f8                          ········         \n" +
                    "# position: 2248, header: 122\n" +
                    "--- !!data #binary\n" +
                    "000008c0                                      fa f9 f9 f9              ····\n" +
                    "000008d0 f9 f9 f9 f9                                      ····             \n" +
                    "# position: 2260, header: 123\n" +
                    "--- !!data #binary\n" +
                    "000008d0                          fb fa fa fa fa fa fa fa          ········\n" +
                    "# position: 2272, header: 124\n" +
                    "--- !!data #binary\n" +
                    "000008e0             fc fb fb fb  fb fb fb fb                 ···· ····    \n" +
                    "# position: 2284, header: 125\n" +
                    "--- !!data #binary\n" +
                    "000008f0 fd fc fc fc fc fc fc fc                          ········         \n" +
                    "# position: 2296, header: 126\n" +
                    "--- !!data #binary\n" +
                    "000008f0                                      fe fd fd fd              ····\n" +
                    "00000900 fd fd fd fd                                      ····             \n" +
                    "# position: 2308, header: 127\n" +
                    "--- !!data #binary\n" +
                    "00000900                          ff fe fe fe fe fe fe fe          ········\n" +
                    "# position: 2320, header: 128\n" +
                    "--- !!data #binary\n" +
                    "00000910             00 00 00 00  00 00 00 00                 ···· ····    \n" +
                    "# position: 2332, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  2320,\n" +
                    "  2652,\n" +
                    "  2700,\n" +
                    "  2748,\n" +
                    "  2796,\n" +
                    "  2844,\n" +
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
                    "  4092\n" +
                    "]\n" +
                    "# position: 2616, header: 129\n" +
                    "--- !!data #binary\n" +
                    "00000a30                                      01 01 01 01              ····\n" +
                    "00000a40 01 01 01 01                                      ····             \n" +
                    "# position: 2628, header: 130\n" +
                    "--- !!data #binary\n" +
                    "00000a40                          02 02 02 02 02 02 02 02          ········\n" +
                    "# position: 2640, header: 131\n" +
                    "--- !!data #binary\n" +
                    "00000a50             03 03 03 03  03 03 03 03                 ···· ····    \n" +
                    "# position: 2652, header: 132\n" +
                    "--- !!data #binary\n" +
                    "00000a60 04 04 04 04 04 04 04 04                          ········         \n" +
                    "# position: 2664, header: 133\n" +
                    "--- !!data #binary\n" +
                    "00000a60                                      05 05 05 05              ····\n" +
                    "00000a70 05 05 05 05                                      ····             \n" +
                    "# position: 2676, header: 134\n" +
                    "--- !!data #binary\n" +
                    "00000a70                          06 06 06 06 06 06 06 06          ········\n" +
                    "# position: 2688, header: 135\n" +
                    "--- !!data #binary\n" +
                    "00000a80             07 07 07 07  07 07 07 07                 ···· ····    \n" +
                    "# position: 2700, header: 136\n" +
                    "--- !!data #binary\n" +
                    "00000a90 08 08 08 08 08 08 08 08                          ········         \n" +
                    "# position: 2712, header: 137\n" +
                    "--- !!data #binary\n" +
                    "00000a90                                      09 09 09 09              ····\n" +
                    "00000aa0 09 09 09 09                                      ····             \n" +
                    "# position: 2724, header: 138\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2736, header: 139\n" +
                    "--- !!data #binary\n" +
                    "00000ab0             0b 0b 0b 0b  0b 0b 0b 0b                 ···· ····    \n" +
                    "# position: 2748, header: 140\n" +
                    "--- !!data #binary\n" +
                    "00000ac0 0c 0c 0c 0c 0c 0c 0c 0c                          ········         \n" +
                    "# position: 2760, header: 141\n" +
                    "--- !!data #binary\n" +
                    "00000ac0                                      0d 0d 0d 0d              ····\n" +
                    "00000ad0 0d 0d 0d 0d                                      ····             \n" +
                    "# position: 2772, header: 142\n" +
                    "--- !!data #binary\n" +
                    "00000ad0                          0e 0e 0e 0e 0e 0e 0e 0e          ········\n" +
                    "# position: 2784, header: 143\n" +
                    "--- !!data #binary\n" +
                    "00000ae0             0f 0f 0f 0f  0f 0f 0f 0f                 ···· ····    \n" +
                    "# position: 2796, header: 144\n" +
                    "--- !!data #binary\n" +
                    "00000af0 10 10 10 10 10 10 10 10                          ········         \n" +
                    "# position: 2808, header: 145\n" +
                    "--- !!data #binary\n" +
                    "00000af0                                      11 11 11 11              ····\n" +
                    "00000b00 11 11 11 11                                      ····             \n" +
                    "# position: 2820, header: 146\n" +
                    "--- !!data #binary\n" +
                    "00000b00                          12 12 12 12 12 12 12 12          ········\n" +
                    "# position: 2832, header: 147\n" +
                    "--- !!data #binary\n" +
                    "00000b10             13 13 13 13  13 13 13 13                 ···· ····    \n" +
                    "# position: 2844, header: 148\n" +
                    "--- !!data #binary\n" +
                    "00000b20 14 14 14 14 14 14 14 14                          ········         \n" +
                    "# position: 2856, header: 149\n" +
                    "--- !!data #binary\n" +
                    "00000b20                                      15 15 15 15              ····\n" +
                    "00000b30 15 15 15 15                                      ····             \n" +
                    "# position: 2868, header: 150\n" +
                    "--- !!data #binary\n" +
                    "00000b30                          16 16 16 16 16 16 16 16          ········\n" +
                    "# position: 2880, header: 151\n" +
                    "--- !!data #binary\n" +
                    "00000b40             17 17 17 17  17 17 17 17                 ···· ····    \n" +
                    "# position: 2892, header: 152\n" +
                    "--- !!data #binary\n" +
                    "00000b50 18 18 18 18 18 18 18 18                          ········         \n" +
                    "# position: 2904, header: 153\n" +
                    "--- !!data #binary\n" +
                    "00000b50                                      19 19 19 19              ····\n" +
                    "00000b60 19 19 19 19                                      ····             \n" +
                    "# position: 2916, header: 154\n" +
                    "--- !!data #binary\n" +
                    "00000b60                          1a 1a 1a 1a 1a 1a 1a 1a          ········\n" +
                    "# position: 2928, header: 155\n" +
                    "--- !!data #binary\n" +
                    "00000b70             1b 1b 1b 1b  1b 1b 1b 1b                 ···· ····    \n" +
                    "# position: 2940, header: 156\n" +
                    "--- !!data #binary\n" +
                    "00000b80 1c 1c 1c 1c 1c 1c 1c 1c                          ········         \n" +
                    "# position: 2952, header: 157\n" +
                    "--- !!data #binary\n" +
                    "00000b80                                      1d 1d 1d 1d              ····\n" +
                    "00000b90 1d 1d 1d 1d                                      ····             \n" +
                    "# position: 2964, header: 158\n" +
                    "--- !!data #binary\n" +
                    "00000b90                          1e 1e 1e 1e 1e 1e 1e 1e          ········\n" +
                    "# position: 2976, header: 159\n" +
                    "--- !!data #binary\n" +
                    "00000ba0             1f 1f 1f 1f  1f 1f 1f 1f                 ···· ····    \n" +
                    "# position: 2988, header: 160\n" +
                    "--- !!data\n" +
                    "        \n" +
                    "# position: 3000, header: 161\n" +
                    "--- !!data\n" +
                    "!!!!!!!!\n" +
                    "# position: 3012, header: 162\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\"\"\"\"\n" +
                    "# position: 3024, header: 163\n" +
                    "--- !!data\n" +
                    "########\n" +
                    "# position: 3036, header: 164\n" +
                    "--- !!data\n" +
                    "$$$$$$$$\n" +
                    "# position: 3048, header: 165\n" +
                    "--- !!data\n" +
                    "%%%%%%%%\n" +
                    "# position: 3060, header: 166\n" +
                    "--- !!data\n" +
                    "&&&&&&&&\n" +
                    "# position: 3072, header: 167\n" +
                    "--- !!data\n" +
                    "''''''''\n" +
                    "# position: 3084, header: 168\n" +
                    "--- !!data\n" +
                    "((((((((\n" +
                    "# position: 3096, header: 169\n" +
                    "--- !!data\n" +
                    "))))))))\n" +
                    "# position: 3108, header: 170\n" +
                    "--- !!data\n" +
                    "********\n" +
                    "# position: 3120, header: 171\n" +
                    "--- !!data\n" +
                    "++++++++\n" +
                    "# position: 3132, header: 172\n" +
                    "--- !!data\n" +
                    ",,,,,,,,\n" +
                    "# position: 3144, header: 173\n" +
                    "--- !!data\n" +
                    "--------\n" +
                    "# position: 3156, header: 174\n" +
                    "--- !!data\n" +
                    "........\n" +
                    "# position: 3168, header: 175\n" +
                    "--- !!data\n" +
                    "////////\n" +
                    "# position: 3180, header: 176\n" +
                    "--- !!data\n" +
                    "00000000\n" +
                    "# position: 3192, header: 177\n" +
                    "--- !!data\n" +
                    "11111111\n" +
                    "# position: 3204, header: 178\n" +
                    "--- !!data\n" +
                    "22222222\n" +
                    "# position: 3216, header: 179\n" +
                    "--- !!data\n" +
                    "33333333\n" +
                    "# position: 3228, header: 180\n" +
                    "--- !!data\n" +
                    "44444444\n" +
                    "# position: 3240, header: 181\n" +
                    "--- !!data\n" +
                    "55555555\n" +
                    "# position: 3252, header: 182\n" +
                    "--- !!data\n" +
                    "66666666\n" +
                    "# position: 3264, header: 183\n" +
                    "--- !!data\n" +
                    "77777777\n" +
                    "# position: 3276, header: 184\n" +
                    "--- !!data\n" +
                    "88888888\n" +
                    "# position: 3288, header: 185\n" +
                    "--- !!data\n" +
                    "99999999\n" +
                    "# position: 3300, header: 186\n" +
                    "--- !!data\n" +
                    "::::::::\n" +
                    "# position: 3312, header: 187\n" +
                    "--- !!data\n" +
                    ";;;;;;;;\n" +
                    "# position: 3324, header: 188\n" +
                    "--- !!data\n" +
                    "<<<<<<<<\n" +
                    "# position: 3336, header: 189\n" +
                    "--- !!data\n" +
                    "========\n" +
                    "# position: 3348, header: 190\n" +
                    "--- !!data\n" +
                    ">>>>>>>>\n" +
                    "# position: 3360, header: 191\n" +
                    "--- !!data\n" +
                    "????????\n" +
                    "# position: 3372, header: 192\n" +
                    "--- !!data\n" +
                    "@@@@@@@@\n" +
                    "# position: 3384, header: 193\n" +
                    "--- !!data\n" +
                    "AAAAAAAA\n" +
                    "# position: 3396, header: 194\n" +
                    "--- !!data\n" +
                    "BBBBBBBB\n" +
                    "# position: 3408, header: 195\n" +
                    "--- !!data\n" +
                    "CCCCCCCC\n" +
                    "# position: 3420, header: 196\n" +
                    "--- !!data\n" +
                    "DDDDDDDD\n" +
                    "# position: 3432, header: 197\n" +
                    "--- !!data\n" +
                    "EEEEEEEE\n" +
                    "# position: 3444, header: 198\n" +
                    "--- !!data\n" +
                    "FFFFFFFF\n" +
                    "# position: 3456, header: 199\n" +
                    "--- !!data\n" +
                    "GGGGGGGG\n" +
                    "# position: 3468, header: 200\n" +
                    "--- !!data\n" +
                    "HHHHHHHH\n" +
                    "# position: 3480, header: 201\n" +
                    "--- !!data\n" +
                    "IIIIIIII\n" +
                    "# position: 3492, header: 202\n" +
                    "--- !!data\n" +
                    "JJJJJJJJ\n" +
                    "# position: 3504, header: 203\n" +
                    "--- !!data\n" +
                    "KKKKKKKK\n" +
                    "# position: 3516, header: 204\n" +
                    "--- !!data\n" +
                    "LLLLLLLL\n" +
                    "# position: 3528, header: 205\n" +
                    "--- !!data\n" +
                    "MMMMMMMM\n" +
                    "# position: 3540, header: 206\n" +
                    "--- !!data\n" +
                    "NNNNNNNN\n" +
                    "# position: 3552, header: 207\n" +
                    "--- !!data\n" +
                    "OOOOOOOO\n" +
                    "# position: 3564, header: 208\n" +
                    "--- !!data\n" +
                    "PPPPPPPP\n" +
                    "# position: 3576, header: 209\n" +
                    "--- !!data\n" +
                    "QQQQQQQQ\n" +
                    "# position: 3588, header: 210\n" +
                    "--- !!data\n" +
                    "RRRRRRRR\n" +
                    "# position: 3600, header: 211\n" +
                    "--- !!data\n" +
                    "SSSSSSSS\n" +
                    "# position: 3612, header: 212\n" +
                    "--- !!data\n" +
                    "TTTTTTTT\n" +
                    "# position: 3624, header: 213\n" +
                    "--- !!data\n" +
                    "UUUUUUUU\n" +
                    "# position: 3636, header: 214\n" +
                    "--- !!data\n" +
                    "VVVVVVVV\n" +
                    "# position: 3648, header: 215\n" +
                    "--- !!data\n" +
                    "WWWWWWWW\n" +
                    "# position: 3660, header: 216\n" +
                    "--- !!data\n" +
                    "XXXXXXXX\n" +
                    "# position: 3672, header: 217\n" +
                    "--- !!data\n" +
                    "YYYYYYYY\n" +
                    "# position: 3684, header: 218\n" +
                    "--- !!data\n" +
                    "ZZZZZZZZ\n" +
                    "# position: 3696, header: 219\n" +
                    "--- !!data\n" +
                    "[[[[[[[[\n" +
                    "# position: 3708, header: 220\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\\\\\\\\\n" +
                    "# position: 3720, header: 221\n" +
                    "--- !!data\n" +
                    "]]]]]]]]\n" +
                    "# position: 3732, header: 222\n" +
                    "--- !!data\n" +
                    "^^^^^^^^\n" +
                    "# position: 3744, header: 223\n" +
                    "--- !!data\n" +
                    "________\n" +
                    "# position: 3756, header: 224\n" +
                    "--- !!data\n" +
                    "````````\n" +
                    "# position: 3768, header: 225\n" +
                    "--- !!data\n" +
                    "aaaaaaaa\n" +
                    "# position: 3780, header: 226\n" +
                    "--- !!data\n" +
                    "bbbbbbbb\n" +
                    "# position: 3792, header: 227\n" +
                    "--- !!data\n" +
                    "cccccccc\n" +
                    "# position: 3804, header: 228\n" +
                    "--- !!data\n" +
                    "dddddddd\n" +
                    "# position: 3816, header: 229\n" +
                    "--- !!data\n" +
                    "eeeeeeee\n" +
                    "# position: 3828, header: 230\n" +
                    "--- !!data\n" +
                    "ffffffff\n" +
                    "# position: 3840, header: 231\n" +
                    "--- !!data\n" +
                    "gggggggg\n" +
                    "# position: 3852, header: 232\n" +
                    "--- !!data\n" +
                    "hhhhhhhh\n" +
                    "# position: 3864, header: 233\n" +
                    "--- !!data\n" +
                    "iiiiiiii\n" +
                    "# position: 3876, header: 234\n" +
                    "--- !!data\n" +
                    "jjjjjjjj\n" +
                    "# position: 3888, header: 235\n" +
                    "--- !!data\n" +
                    "kkkkkkkk\n" +
                    "# position: 3900, header: 236\n" +
                    "--- !!data\n" +
                    "llllllll\n" +
                    "# position: 3912, header: 237\n" +
                    "--- !!data\n" +
                    "mmmmmmmm\n" +
                    "# position: 3924, header: 238\n" +
                    "--- !!data\n" +
                    "nnnnnnnn\n" +
                    "# position: 3936, header: 239\n" +
                    "--- !!data\n" +
                    "oooooooo\n" +
                    "# position: 3948, header: 240\n" +
                    "--- !!data\n" +
                    "pppppppp\n" +
                    "# position: 3960, header: 241\n" +
                    "--- !!data\n" +
                    "qqqqqqqq\n" +
                    "# position: 3972, header: 242\n" +
                    "--- !!data\n" +
                    "rrrrrrrr\n" +
                    "# position: 3984, header: 243\n" +
                    "--- !!data\n" +
                    "ssssssss\n" +
                    "# position: 3996, header: 244\n" +
                    "--- !!data\n" +
                    "tttttttt\n" +
                    "# position: 4008, header: 245\n" +
                    "--- !!data\n" +
                    "uuuuuuuu\n" +
                    "# position: 4020, header: 246\n" +
                    "--- !!data\n" +
                    "vvvvvvvv\n" +
                    "# position: 4032, header: 247\n" +
                    "--- !!data\n" +
                    "wwwwwwww\n" +
                    "# position: 4044, header: 248\n" +
                    "--- !!data\n" +
                    "xxxxxxxx\n" +
                    "# position: 4056, header: 249\n" +
                    "--- !!data\n" +
                    "yyyyyyyy\n" +
                    "# position: 4068, header: 250\n" +
                    "--- !!data\n" +
                    "zzzzzzzz\n" +
                    "# position: 4080, header: 251\n" +
                    "--- !!data\n" +
                    "{{{{{{{{\n" +
                    "# position: 4092, header: 252\n" +
                    "--- !!data\n" +
                    "||||||||\n" +
                    "# position: 4104, header: 253\n" +
                    "--- !!data\n" +
                    "}}}}}}}}\n" +
                    "# position: 4116, header: 254\n" +
                    "--- !!data\n" +
                    "~~~~~~~~\n" +
                    "# position: 4128, header: 255\n" +
                    "--- !!data\n" +
                    "\u007F\u007F\u007F\u007F\u007F\u007F\u007F\u007F\n" +
                    "...\n" +
                    (useSparseFiles
                            ? "# 4294963160 bytes remaining\n"
                            : "# 126928 bytes remaining\n"), queue.dump());

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
