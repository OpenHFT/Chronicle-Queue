package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static net.openhft.chronicle.queue.RollCycles.TEST4_DAILY;
import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 16/05/2016.
 */
public class WriteBytesTest {
    Bytes outgoingBytes = Bytes.elasticByteBuffer();
    private byte[] incomingMsgBytes = new byte[100];
    private byte[] outgoingMsgBytes = new byte[100];

    @Test
    public void testWriteBytes() {
        String dir = OS.TARGET + "/WriteBytesTest-" + System.nanoTime();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).build()) {

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
        String dir = OS.TARGET + "/WriteBytesTestAndDump";
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
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
                    "  writePosition: 4280,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 352,\n" +
                    "    lastIndex: 256\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 352, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 2\n" +
                    "  648,\n" +
                    "  2484,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 648, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  936,\n" +
                    "  984,\n" +
                    "  1032,\n" +
                    "  1080,\n" +
                    "  1128,\n" +
                    "  1176,\n" +
                    "  1224,\n" +
                    "  1272,\n" +
                    "  1320,\n" +
                    "  1368,\n" +
                    "  1416,\n" +
                    "  1464,\n" +
                    "  1512,\n" +
                    "  1560,\n" +
                    "  1608,\n" +
                    "  1656,\n" +
                    "  1704,\n" +
                    "  1752,\n" +
                    "  1800,\n" +
                    "  1848,\n" +
                    "  1896,\n" +
                    "  1944,\n" +
                    "  1992,\n" +
                    "  2040,\n" +
                    "  2088,\n" +
                    "  2136,\n" +
                    "  2184,\n" +
                    "  2232,\n" +
                    "  2280,\n" +
                    "  2328,\n" +
                    "  2376,\n" +
                    "  2424\n" +
                    "]\n" +
                    "# position: 936, header: 0\n" +
                    "--- !!data #binary\n" +
                    "000003a0                                      80 7F 7F 7F              ····\n" +
                    "000003b0 7F 7F 7F 7F                                      ····             \n" +
                    "# position: 948, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000003b0                          81 80 80 80 80 80 80 80          ········\n" +
                    "# position: 960, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000003c0             82 81 81 81  81 81 81 81                 ···· ····    \n" +
                    "# position: 972, header: 3\n" +
                    "--- !!data #binary\n" +
                    "000003d0 83 82 82 82 82 82 82 82                          ········         \n" +
                    "# position: 984, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 996, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 1008, header: 6\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x86\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 1020, header: 7\n" +
                    "--- !!data #binary\n" +
                    "00000400 87 86 86 86 86 86 86 86                          ········         \n" +
                    "# position: 1032, header: 8\n" +
                    "--- !!data #binary\n" +
                    "00000400                                      88 87 87 87              ····\n" +
                    "00000410 87 87 87 87                                      ····             \n" +
                    "# position: 1044, header: 9\n" +
                    "--- !!data #binary\n" +
                    "00000410                          89 88 88 88 88 88 88 88          ········\n" +
                    "# position: 1056, header: 10\n" +
                    "--- !!data #binary\n" +
                    "!!binary XAEAQLkGaA==\n" +
                    "\n" +
                    "# position: 1068, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "!!binary XAEAQLkG\n" +
                    "\n" +
                    "# position: 1080, header: 12\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 1092, header: 13\n" +
                    "--- !!data #binary\n" +
                    "00000440                          8D 8C 8C 8C 8C 8C 8C 8C          ········\n" +
                    "# position: 1104, header: 14\n" +
                    "--- !!data #binary\n" +
                    "00000450             8E 8D 8D 8D  8D 8D 8D 8D                 ···· ····    \n" +
                    "# position: 1116, header: 15\n" +
                    "--- !!data #binary\n" +
                    "00000460 8F 8E 8E 8E 8E 8E 8E 8E                          ········         \n" +
                    "# position: 1128, header: 16\n" +
                    "--- !!data #binary\n" +
                    "-0.000000000000000000000000000014156185439721036\n" +
                    "# position: 1140, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "-0.00000000000000000000000000005702071897398123\n" +
                    "# # EndOfFile\n" +
                    "# position: 1152, header: 18\n" +
                    "--- !!data #binary\n" +
                    "-753555055760.82\n" +
                    "# position: 1164, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_2\n" +
                    "-48698841.79\n" +
                    "# position: 1176, header: 20\n" +
                    "--- !!data #binary\n" +
                    "-8422085917.3268\n" +
                    "# position: 1188, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_4\n" +
                    "-541098.2421\n" +
                    "# position: 1200, header: 22\n" +
                    "--- !!data #binary\n" +
                    "-93086212.770454\n" +
                    "# position: 1212, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_6\n" +
                    "-5952.080663\n" +
                    "# position: 1224, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 1236, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 1248, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 1260, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# position: 1272, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# position: 1284, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# position: 1296, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 1308, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 1320, header: 32\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 1332, header: 33\n" +
                    "--- !!data #binary\n" +
                    "!int 160\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# position: 1344, header: 34\n" +
                    "--- !!data #binary\n" +
                    "!int 41377\n" +
                    "!int 161\n" +
                    "!int 161\n" +
                    "!int -1\n" +
                    "# position: 1356, header: 35\n" +
                    "--- !!data #binary\n" +
                    "2728567458\n" +
                    "!int 41634\n" +
                    "# position: 1368, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "2745410467\n" +
                    "# # EndOfFile\n" +
                    "# position: 1380, header: 37\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte -92\n" +
                    "!byte -92\n" +
                    "!byte 0\n" +
                    "# position: 1392, header: 38\n" +
                    "--- !!data #binary\n" +
                    "!int -1515870811\n" +
                    "!short -23131\n" +
                    "# position: 1404, header: 39\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "!int -1499027802\n" +
                    "# # EndOfFile\n" +
                    "# position: 1416, header: 40\n" +
                    "--- !!data #binary\n" +
                    "!int 167\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# position: 1428, header: 41\n" +
                    "--- !!data #binary\n" +
                    "!int 43176\n" +
                    "!int 168\n" +
                    "!int 168\n" +
                    "!int -1\n" +
                    "# position: 1440, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # SET_LOW_INT16\n" +
                    "!int 43433\n" +
                    "!int 43433\n" +
                    "# position: 1452, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1464, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1476, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1488, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1500, header: 47\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1512, header: 48\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1524, header: 49\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1536, header: 50\n" +
                    "--- !!data #binary\n" +
                    "00000600             B2 B1 B1 B1  B1 B1 B1 B1                 ···· ····    \n" +
                    "# position: 1548, header: 51\n" +
                    "--- !!data #binary\n" +
                    "00000610 B3 B2 B2 B2 B2 B2 B2 B2                          ········         \n" +
                    "# position: 1560, header: 52\n" +
                    "--- !!data #binary\n" +
                    "00000610                                      B4 B3 B3 B3              ····\n" +
                    "00000620 B3 B3 B3 B3                                      ····             \n" +
                    "# position: 1572, header: 53\n" +
                    "--- !!data #binary\n" +
                    "00000620                          B5 B4 B4 B4 B4 B4 B4 B4          ········\n" +
                    "# position: 1584, header: 54\n" +
                    "--- !!data #binary\n" +
                    "00000630             B6 B5 B5 B5  B5 B5 B5 B5                 ···· ····    \n" +
                    "# position: 1596, header: 55\n" +
                    "--- !!data #binary\n" +
                    "00000640 B7 B6 B6 B6 B6 B6 B6 B6                          ········         \n" +
                    "# position: 1608, header: 56\n" +
                    "--- !!data #binary\n" +
                    "00000640                                      B8 B7 B7 B7              ····\n" +
                    "00000650 B7 B7 B7 B7                                      ····             \n" +
                    "# position: 1620, header: 57\n" +
                    "--- !!data #binary\n" +
                    "00000650                          B9 B8 B8 B8 B8 B8 B8 B8          ········\n" +
                    "# position: 1632, header: 58\n" +
                    "--- !!data #binary\n" +
                    "\"-252662577519802\": \n" +
                    "# position: 1644, header: 59\n" +
                    "--- !!data #binary\n" +
                    "!!null \"\"\n" +
                    "\"-2008556674363\": \n" +
                    "# position: 1656, header: 60\n" +
                    "--- !!data #binary\n" +
                    "00000670                                      BC BB BB BB              ····\n" +
                    "00000680 BB BB BB BB                                      ····             \n" +
                    "# position: 1668, header: 61\n" +
                    "--- !!data #binary\n" +
                    "00000680                          BD BC BC BC BC BC BC BC          ········\n" +
                    "# position: 1680, header: 62\n" +
                    "--- !!data #binary\n" +
                    "00000690             BE BD BD BD  BD BD BD BD                 ···· ····    \n" +
                    "# position: 1692, header: 63\n" +
                    "--- !!data #binary\n" +
                    "000006a0 BF BE BE BE BE BE BE BE                          ········         \n" +
                    "# position: 1704, header: 64\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1716, header: 65\n" +
                    "--- !!data #binary\n" +
                    "Ã\u0080: \"\": \"\": \"\": \"\": \"\": \"\": \n" +
                    "# position: 1728, header: 66\n" +
                    "--- !!data #binary\n" +
                    "000006c0             C2 C1 C1 C1  C1 C1 C1 C1                 ···· ····    \n" +
                    "# position: 1740, header: 67\n" +
                    "--- !!data #binary\n" +
                    "000006d0 C3 C2 C2 C2 C2 C2 C2 C2                          ········         \n" +
                    "# position: 1752, header: 68\n" +
                    "--- !!data #binary\n" +
                    "000006d0                                      C4 C3 C3 C3              ····\n" +
                    "000006e0 C3 C3 C3 C3                                      ····             \n" +
                    "# position: 1764, header: 69\n" +
                    "--- !!data #binary\n" +
                    "000006e0                          C5 C4 C4 C4 C4 C4 C4 C4          ········\n" +
                    "# position: 1776, header: 70\n" +
                    "--- !!data #binary\n" +
                    "000006f0             C6 C5 C5 C5  C5 C5 C5 C5                 ···· ····    \n" +
                    "# position: 1788, header: 71\n" +
                    "--- !!data #binary\n" +
                    "Ã\u0086Ã\u0086Ã\u0086Ã\u0086Ã\u0086Ã\u0086Ã\u0086: \n" +
                    "# position: 1800, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000700                                      C8 C7 C7 C7              ····\n" +
                    "00000710 C7 C7 C7 C7                                      ····             \n" +
                    "# position: 1812, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000710                          C9 C8 C8 C8 C8 C8 C8 C8          ········\n" +
                    "# position: 1824, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000720             CA C9 C9 C9  C9 C9 C9 C9                 ···· ····    \n" +
                    "# position: 1836, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000730 CB CA CA CA CA CA CA CA                          ········         \n" +
                    "# position: 1848, header: 76\n" +
                    "--- !!data #binary\n" +
                    "00000730                                      CC CB CB CB              ····\n" +
                    "00000740 CB CB CB CB                                      ····             \n" +
                    "# position: 1860, header: 77\n" +
                    "--- !!data #binary\n" +
                    "00000740                          CD CC CC CC CC CC CC CC          ········\n" +
                    "# position: 1872, header: 78\n" +
                    "--- !!data #binary\n" +
                    "00000750             CE CD CD CD  CD CD CD CD                 ···· ····    \n" +
                    "# position: 1884, header: 79\n" +
                    "--- !!data #binary\n" +
                    "00000760 CF CE CE CE CE CE CE CE                          ········         \n" +
                    "# position: 1896, header: 80\n" +
                    "--- !!data #binary\n" +
                    "00000760                                      D0 CF CF CF              ····\n" +
                    "00000770 CF CF CF CF                                      ····             \n" +
                    "# position: 1908, header: 81\n" +
                    "--- !!data #binary\n" +
                    "00000770                          D1 D0 D0 D0 D0 D0 D0 D0          ········\n" +
                    "# position: 1920, header: 82\n" +
                    "--- !!data #binary\n" +
                    "00000780             D2 D1 D1 D1  D1 D1 D1 D1                 ···· ····    \n" +
                    "# position: 1932, header: 83\n" +
                    "--- !!data #binary\n" +
                    "00000790 D3 D2 D2 D2 D2 D2 D2 D2                          ········         \n" +
                    "# position: 1944, header: 84\n" +
                    "--- !!data #binary\n" +
                    "00000790                                      D4 D3 D3 D3              ····\n" +
                    "000007a0 D3 D3 D3 D3                                      ····             \n" +
                    "# position: 1956, header: 85\n" +
                    "--- !!data #binary\n" +
                    "000007a0                          D5 D4 D4 D4 D4 D4 D4 D4          ········\n" +
                    "# position: 1968, header: 86\n" +
                    "--- !!data #binary\n" +
                    "000007b0             D6 D5 D5 D5  D5 D5 D5 D5                 ···· ····    \n" +
                    "# position: 1980, header: 87\n" +
                    "--- !!data #binary\n" +
                    "000007c0 D7 D6 D6 D6 D6 D6 D6 D6                          ········         \n" +
                    "# position: 1992, header: 88\n" +
                    "--- !!data #binary\n" +
                    "000007c0                                      D8 D7 D7 D7              ····\n" +
                    "000007d0 D7 D7 D7 D7                                      ····             \n" +
                    "# position: 2004, header: 89\n" +
                    "--- !!data #binary\n" +
                    "000007d0                          D9 D8 D8 D8 D8 D8 D8 D8          ········\n" +
                    "# position: 2016, header: 90\n" +
                    "--- !!data #binary\n" +
                    "000007e0             DA D9 D9 D9  D9 D9 D9 D9                 ···· ····    \n" +
                    "# position: 2028, header: 91\n" +
                    "--- !!data #binary\n" +
                    "000007f0 DB DA DA DA DA DA DA DA                          ········         \n" +
                    "# position: 2040, header: 92\n" +
                    "--- !!data #binary\n" +
                    "000007f0                                      DC DB DB DB              ····\n" +
                    "00000800 DB DB DB DB                                      ····             \n" +
                    "# position: 2052, header: 93\n" +
                    "--- !!data #binary\n" +
                    "00000800                          DD DC DC DC DC DC DC DC          ········\n" +
                    "# position: 2064, header: 94\n" +
                    "--- !!data #binary\n" +
                    "00000810             DE DD DD DD  DD DD DD DD                 ···· ····    \n" +
                    "# position: 2076, header: 95\n" +
                    "--- !!data #binary\n" +
                    "00000820 DF DE DE DE DE DE DE DE                          ········         \n" +
                    "# position: 2088, header: 96\n" +
                    "--- !!data #binary\n" +
                    "00000820                                      E0 DF DF DF              ····\n" +
                    "00000830 DF DF DF DF                                      ····             \n" +
                    "# position: 2100, header: 97\n" +
                    "--- !!data #binary\n" +
                    "00000830                          E1 E0 E0 E0 E0 E0 E0 E0          ········\n" +
                    "# position: 2112, header: 98\n" +
                    "--- !!data #binary\n" +
                    "00000840             E2 E1 E1 E1  E1 E1 E1 E1                 ···· ····    \n" +
                    "# position: 2124, header: 99\n" +
                    "--- !!data #binary\n" +
                    "00000850 E3 E2 E2 E2 E2 E2 E2 E2                          ········         \n" +
                    "# position: 2136, header: 100\n" +
                    "--- !!data #binary\n" +
                    "00000850                                      E4 E3 E3 E3              ····\n" +
                    "00000860 E3 E3 E3 E3                                      ····             \n" +
                    "# position: 2148, header: 101\n" +
                    "--- !!data #binary\n" +
                    "00000860                          E5 E4 E4 E4 E4 E4 E4 E4          ········\n" +
                    "# position: 2160, header: 102\n" +
                    "--- !!data #binary\n" +
                    "00000870             E6 E5 E5 E5  E5 E5 E5 E5                 ···· ····    \n" +
                    "# position: 2172, header: 103\n" +
                    "--- !!data #binary\n" +
                    "00000880 E7 E6 E6 E6 E6 E6 E6 E6                          ········         \n" +
                    "# position: 2184, header: 104\n" +
                    "--- !!data #binary\n" +
                    "00000880                                      E8 E7 E7 E7              ····\n" +
                    "00000890 E7 E7 E7 E7                                      ····             \n" +
                    "# position: 2196, header: 105\n" +
                    "--- !!data #binary\n" +
                    "00000890                          E9 E8 E8 E8 E8 E8 E8 E8          ········\n" +
                    "# position: 2208, header: 106\n" +
                    "--- !!data #binary\n" +
                    "000008a0             EA E9 E9 E9  E9 E9 E9 E9                 ···· ····    \n" +
                    "# position: 2220, header: 107\n" +
                    "--- !!data #binary\n" +
                    "000008b0 EB EA EA EA EA EA EA EA                          ········         \n" +
                    "# position: 2232, header: 108\n" +
                    "--- !!data #binary\n" +
                    "000008b0                                      EC EB EB EB              ····\n" +
                    "000008c0 EB EB EB EB                                      ····             \n" +
                    "# position: 2244, header: 109\n" +
                    "--- !!data #binary\n" +
                    "000008c0                          ED EC EC EC EC EC EC EC          ········\n" +
                    "# position: 2256, header: 110\n" +
                    "--- !!data #binary\n" +
                    "000008d0             EE ED ED ED  ED ED ED ED                 ···· ····    \n" +
                    "# position: 2268, header: 111\n" +
                    "--- !!data #binary\n" +
                    "000008e0 EF EE EE EE EE EE EE EE                          ········         \n" +
                    "# position: 2280, header: 112\n" +
                    "--- !!data #binary\n" +
                    "000008e0                                      F0 EF EF EF              ····\n" +
                    "000008f0 EF EF EF EF                                      ····             \n" +
                    "# position: 2292, header: 113\n" +
                    "--- !!data #binary\n" +
                    "000008f0                          F1 F0 F0 F0 F0 F0 F0 F0          ········\n" +
                    "# position: 2304, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000900             F2 F1 F1 F1  F1 F1 F1 F1                 ···· ····    \n" +
                    "# position: 2316, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000910 F3 F2 F2 F2 F2 F2 F2 F2                          ········         \n" +
                    "# position: 2328, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000910                                      F4 F3 F3 F3              ····\n" +
                    "00000920 F3 F3 F3 F3                                      ····             \n" +
                    "# position: 2340, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000920                          F5 F4 F4 F4 F4 F4 F4 F4          ········\n" +
                    "# position: 2352, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000930             F6 F5 F5 F5  F5 F5 F5 F5                 ···· ····    \n" +
                    "# position: 2364, header: 119\n" +
                    "--- !!data #binary\n" +
                    "00000940 F7 F6 F6 F6 F6 F6 F6 F6                          ········         \n" +
                    "# position: 2376, header: 120\n" +
                    "--- !!data #binary\n" +
                    "00000940                                      F8 F7 F7 F7              ····\n" +
                    "00000950 F7 F7 F7 F7                                      ····             \n" +
                    "# position: 2388, header: 121\n" +
                    "--- !!data #binary\n" +
                    "00000950                          F9 F8 F8 F8 F8 F8 F8 F8          ········\n" +
                    "# position: 2400, header: 122\n" +
                    "--- !!data #binary\n" +
                    "00000960             FA F9 F9 F9  F9 F9 F9 F9                 ···· ····    \n" +
                    "# position: 2412, header: 123\n" +
                    "--- !!data #binary\n" +
                    "00000970 FB FA FA FA FA FA FA FA                          ········         \n" +
                    "# position: 2424, header: 124\n" +
                    "--- !!data #binary\n" +
                    "00000970                                      FC FB FB FB              ····\n" +
                    "00000980 FB FB FB FB                                      ····             \n" +
                    "# position: 2436, header: 125\n" +
                    "--- !!data #binary\n" +
                    "00000980                          FD FC FC FC FC FC FC FC          ········\n" +
                    "# position: 2448, header: 126\n" +
                    "--- !!data #binary\n" +
                    "00000990             FE FD FD FD  FD FD FD FD                 ···· ····    \n" +
                    "# position: 2460, header: 127\n" +
                    "--- !!data #binary\n" +
                    "000009a0 FF FE FE FE FE FE FE FE                          ········         \n" +
                    "# position: 2472, header: 128\n" +
                    "--- !!data #binary\n" +
                    "000009a0                                      00 00 00 00              ····\n" +
                    "000009b0 00 00 00 00                                      ····             \n" +
                    "# position: 2484, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  2472,\n" +
                    "  2804,\n" +
                    "  2852,\n" +
                    "  2900,\n" +
                    "  2948,\n" +
                    "  2996,\n" +
                    "  3044,\n" +
                    "  3092,\n" +
                    "  3140,\n" +
                    "  3188,\n" +
                    "  3236,\n" +
                    "  3284,\n" +
                    "  3332,\n" +
                    "  3380,\n" +
                    "  3428,\n" +
                    "  3476,\n" +
                    "  3524,\n" +
                    "  3572,\n" +
                    "  3620,\n" +
                    "  3668,\n" +
                    "  3716,\n" +
                    "  3764,\n" +
                    "  3812,\n" +
                    "  3860,\n" +
                    "  3908,\n" +
                    "  3956,\n" +
                    "  4004,\n" +
                    "  4052,\n" +
                    "  4100,\n" +
                    "  4148,\n" +
                    "  4196,\n" +
                    "  4244\n" +
                    "]\n" +
                    "# position: 2768, header: 129\n" +
                    "--- !!data #binary\n" +
                    "00000ad0             01 01 01 01  01 01 01 01                 ···· ····    \n" +
                    "# position: 2780, header: 130\n" +
                    "--- !!data #binary\n" +
                    "00000ae0 02 02 02 02 02 02 02 02                          ········         \n" +
                    "# position: 2792, header: 131\n" +
                    "--- !!data #binary\n" +
                    "00000ae0                                      03 03 03 03              ····\n" +
                    "00000af0 03 03 03 03                                      ····             \n" +
                    "# position: 2804, header: 132\n" +
                    "--- !!data #binary\n" +
                    "00000af0                          04 04 04 04 04 04 04 04          ········\n" +
                    "# position: 2816, header: 133\n" +
                    "--- !!data #binary\n" +
                    "00000b00             05 05 05 05  05 05 05 05                 ···· ····    \n" +
                    "# position: 2828, header: 134\n" +
                    "--- !!data #binary\n" +
                    "00000b10 06 06 06 06 06 06 06 06                          ········         \n" +
                    "# position: 2840, header: 135\n" +
                    "--- !!data #binary\n" +
                    "00000b10                                      07 07 07 07              ····\n" +
                    "00000b20 07 07 07 07                                      ····             \n" +
                    "# position: 2852, header: 136\n" +
                    "--- !!data #binary\n" +
                    "00000b20                          08 08 08 08 08 08 08 08          ········\n" +
                    "# position: 2864, header: 137\n" +
                    "--- !!data #binary\n" +
                    "00000b30             09 09 09 09  09 09 09 09                 ···· ····    \n" +
                    "# position: 2876, header: 138\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2888, header: 139\n" +
                    "--- !!data #binary\n" +
                    "00000b40                                      0B 0B 0B 0B              ····\n" +
                    "00000b50 0B 0B 0B 0B                                      ····             \n" +
                    "# position: 2900, header: 140\n" +
                    "--- !!data #binary\n" +
                    "00000b50                          0C 0C 0C 0C 0C 0C 0C 0C          ········\n" +
                    "# position: 2912, header: 141\n" +
                    "--- !!data #binary\n" +
                    "00000b60             0D 0D 0D 0D  0D 0D 0D 0D                 ···· ····    \n" +
                    "# position: 2924, header: 142\n" +
                    "--- !!data #binary\n" +
                    "00000b70 0E 0E 0E 0E 0E 0E 0E 0E                          ········         \n" +
                    "# position: 2936, header: 143\n" +
                    "--- !!data #binary\n" +
                    "00000b70                                      0F 0F 0F 0F              ····\n" +
                    "00000b80 0F 0F 0F 0F                                      ····             \n" +
                    "# position: 2948, header: 144\n" +
                    "--- !!data #binary\n" +
                    "00000b80                          10 10 10 10 10 10 10 10          ········\n" +
                    "# position: 2960, header: 145\n" +
                    "--- !!data #binary\n" +
                    "00000b90             11 11 11 11  11 11 11 11                 ···· ····    \n" +
                    "# position: 2972, header: 146\n" +
                    "--- !!data #binary\n" +
                    "00000ba0 12 12 12 12 12 12 12 12                          ········         \n" +
                    "# position: 2984, header: 147\n" +
                    "--- !!data #binary\n" +
                    "00000ba0                                      13 13 13 13              ····\n" +
                    "00000bb0 13 13 13 13                                      ····             \n" +
                    "# position: 2996, header: 148\n" +
                    "--- !!data #binary\n" +
                    "00000bb0                          14 14 14 14 14 14 14 14          ········\n" +
                    "# position: 3008, header: 149\n" +
                    "--- !!data #binary\n" +
                    "00000bc0             15 15 15 15  15 15 15 15                 ···· ····    \n" +
                    "# position: 3020, header: 150\n" +
                    "--- !!data #binary\n" +
                    "00000bd0 16 16 16 16 16 16 16 16                          ········         \n" +
                    "# position: 3032, header: 151\n" +
                    "--- !!data #binary\n" +
                    "00000bd0                                      17 17 17 17              ····\n" +
                    "00000be0 17 17 17 17                                      ····             \n" +
                    "# position: 3044, header: 152\n" +
                    "--- !!data #binary\n" +
                    "00000be0                          18 18 18 18 18 18 18 18          ········\n" +
                    "# position: 3056, header: 153\n" +
                    "--- !!data #binary\n" +
                    "00000bf0             19 19 19 19  19 19 19 19                 ···· ····    \n" +
                    "# position: 3068, header: 154\n" +
                    "--- !!data #binary\n" +
                    "00000c00 1A 1A 1A 1A 1A 1A 1A 1A                          ········         \n" +
                    "# position: 3080, header: 155\n" +
                    "--- !!data #binary\n" +
                    "00000c00                                      1B 1B 1B 1B              ····\n" +
                    "00000c10 1B 1B 1B 1B                                      ····             \n" +
                    "# position: 3092, header: 156\n" +
                    "--- !!data #binary\n" +
                    "00000c10                          1C 1C 1C 1C 1C 1C 1C 1C          ········\n" +
                    "# position: 3104, header: 157\n" +
                    "--- !!data #binary\n" +
                    "00000c20             1D 1D 1D 1D  1D 1D 1D 1D                 ···· ····    \n" +
                    "# position: 3116, header: 158\n" +
                    "--- !!data #binary\n" +
                    "00000c30 1E 1E 1E 1E 1E 1E 1E 1E                          ········         \n" +
                    "# position: 3128, header: 159\n" +
                    "--- !!data #binary\n" +
                    "00000c30                                      1F 1F 1F 1F              ····\n" +
                    "00000c40 1F 1F 1F 1F                                      ····             \n" +
                    "# position: 3140, header: 160\n" +
                    "--- !!data\n" +
                    "        \n" +
                    "# position: 3152, header: 161\n" +
                    "--- !!data\n" +
                    "!!!!!!!!\n" +
                    "# position: 3164, header: 162\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\"\"\"\"\n" +
                    "# position: 3176, header: 163\n" +
                    "--- !!data\n" +
                    "########\n" +
                    "# position: 3188, header: 164\n" +
                    "--- !!data\n" +
                    "$$$$$$$$\n" +
                    "# position: 3200, header: 165\n" +
                    "--- !!data\n" +
                    "%%%%%%%%\n" +
                    "# position: 3212, header: 166\n" +
                    "--- !!data\n" +
                    "&&&&&&&&\n" +
                    "# position: 3224, header: 167\n" +
                    "--- !!data\n" +
                    "''''''''\n" +
                    "# position: 3236, header: 168\n" +
                    "--- !!data\n" +
                    "((((((((\n" +
                    "# position: 3248, header: 169\n" +
                    "--- !!data\n" +
                    "))))))))\n" +
                    "# position: 3260, header: 170\n" +
                    "--- !!data\n" +
                    "********\n" +
                    "# position: 3272, header: 171\n" +
                    "--- !!data\n" +
                    "++++++++\n" +
                    "# position: 3284, header: 172\n" +
                    "--- !!data\n" +
                    ",,,,,,,,\n" +
                    "# position: 3296, header: 173\n" +
                    "--- !!data\n" +
                    "--------\n" +
                    "# position: 3308, header: 174\n" +
                    "--- !!data\n" +
                    "........\n" +
                    "# position: 3320, header: 175\n" +
                    "--- !!data\n" +
                    "////////\n" +
                    "# position: 3332, header: 176\n" +
                    "--- !!data\n" +
                    "00000000\n" +
                    "# position: 3344, header: 177\n" +
                    "--- !!data\n" +
                    "11111111\n" +
                    "# position: 3356, header: 178\n" +
                    "--- !!data\n" +
                    "22222222\n" +
                    "# position: 3368, header: 179\n" +
                    "--- !!data\n" +
                    "33333333\n" +
                    "# position: 3380, header: 180\n" +
                    "--- !!data\n" +
                    "44444444\n" +
                    "# position: 3392, header: 181\n" +
                    "--- !!data\n" +
                    "55555555\n" +
                    "# position: 3404, header: 182\n" +
                    "--- !!data\n" +
                    "66666666\n" +
                    "# position: 3416, header: 183\n" +
                    "--- !!data\n" +
                    "77777777\n" +
                    "# position: 3428, header: 184\n" +
                    "--- !!data\n" +
                    "88888888\n" +
                    "# position: 3440, header: 185\n" +
                    "--- !!data\n" +
                    "99999999\n" +
                    "# position: 3452, header: 186\n" +
                    "--- !!data\n" +
                    "::::::::\n" +
                    "# position: 3464, header: 187\n" +
                    "--- !!data\n" +
                    ";;;;;;;;\n" +
                    "# position: 3476, header: 188\n" +
                    "--- !!data\n" +
                    "<<<<<<<<\n" +
                    "# position: 3488, header: 189\n" +
                    "--- !!data\n" +
                    "========\n" +
                    "# position: 3500, header: 190\n" +
                    "--- !!data\n" +
                    ">>>>>>>>\n" +
                    "# position: 3512, header: 191\n" +
                    "--- !!data\n" +
                    "????????\n" +
                    "# position: 3524, header: 192\n" +
                    "--- !!data\n" +
                    "@@@@@@@@\n" +
                    "# position: 3536, header: 193\n" +
                    "--- !!data\n" +
                    "AAAAAAAA\n" +
                    "# position: 3548, header: 194\n" +
                    "--- !!data\n" +
                    "BBBBBBBB\n" +
                    "# position: 3560, header: 195\n" +
                    "--- !!data\n" +
                    "CCCCCCCC\n" +
                    "# position: 3572, header: 196\n" +
                    "--- !!data\n" +
                    "DDDDDDDD\n" +
                    "# position: 3584, header: 197\n" +
                    "--- !!data\n" +
                    "EEEEEEEE\n" +
                    "# position: 3596, header: 198\n" +
                    "--- !!data\n" +
                    "FFFFFFFF\n" +
                    "# position: 3608, header: 199\n" +
                    "--- !!data\n" +
                    "GGGGGGGG\n" +
                    "# position: 3620, header: 200\n" +
                    "--- !!data\n" +
                    "HHHHHHHH\n" +
                    "# position: 3632, header: 201\n" +
                    "--- !!data\n" +
                    "IIIIIIII\n" +
                    "# position: 3644, header: 202\n" +
                    "--- !!data\n" +
                    "JJJJJJJJ\n" +
                    "# position: 3656, header: 203\n" +
                    "--- !!data\n" +
                    "KKKKKKKK\n" +
                    "# position: 3668, header: 204\n" +
                    "--- !!data\n" +
                    "LLLLLLLL\n" +
                    "# position: 3680, header: 205\n" +
                    "--- !!data\n" +
                    "MMMMMMMM\n" +
                    "# position: 3692, header: 206\n" +
                    "--- !!data\n" +
                    "NNNNNNNN\n" +
                    "# position: 3704, header: 207\n" +
                    "--- !!data\n" +
                    "OOOOOOOO\n" +
                    "# position: 3716, header: 208\n" +
                    "--- !!data\n" +
                    "PPPPPPPP\n" +
                    "# position: 3728, header: 209\n" +
                    "--- !!data\n" +
                    "QQQQQQQQ\n" +
                    "# position: 3740, header: 210\n" +
                    "--- !!data\n" +
                    "RRRRRRRR\n" +
                    "# position: 3752, header: 211\n" +
                    "--- !!data\n" +
                    "SSSSSSSS\n" +
                    "# position: 3764, header: 212\n" +
                    "--- !!data\n" +
                    "TTTTTTTT\n" +
                    "# position: 3776, header: 213\n" +
                    "--- !!data\n" +
                    "UUUUUUUU\n" +
                    "# position: 3788, header: 214\n" +
                    "--- !!data\n" +
                    "VVVVVVVV\n" +
                    "# position: 3800, header: 215\n" +
                    "--- !!data\n" +
                    "WWWWWWWW\n" +
                    "# position: 3812, header: 216\n" +
                    "--- !!data\n" +
                    "XXXXXXXX\n" +
                    "# position: 3824, header: 217\n" +
                    "--- !!data\n" +
                    "YYYYYYYY\n" +
                    "# position: 3836, header: 218\n" +
                    "--- !!data\n" +
                    "ZZZZZZZZ\n" +
                    "# position: 3848, header: 219\n" +
                    "--- !!data\n" +
                    "[[[[[[[[\n" +
                    "# position: 3860, header: 220\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\\\\\\\\\n" +
                    "# position: 3872, header: 221\n" +
                    "--- !!data\n" +
                    "]]]]]]]]\n" +
                    "# position: 3884, header: 222\n" +
                    "--- !!data\n" +
                    "^^^^^^^^\n" +
                    "# position: 3896, header: 223\n" +
                    "--- !!data\n" +
                    "________\n" +
                    "# position: 3908, header: 224\n" +
                    "--- !!data\n" +
                    "````````\n" +
                    "# position: 3920, header: 225\n" +
                    "--- !!data\n" +
                    "aaaaaaaa\n" +
                    "# position: 3932, header: 226\n" +
                    "--- !!data\n" +
                    "bbbbbbbb\n" +
                    "# position: 3944, header: 227\n" +
                    "--- !!data\n" +
                    "cccccccc\n" +
                    "# position: 3956, header: 228\n" +
                    "--- !!data\n" +
                    "dddddddd\n" +
                    "# position: 3968, header: 229\n" +
                    "--- !!data\n" +
                    "eeeeeeee\n" +
                    "# position: 3980, header: 230\n" +
                    "--- !!data\n" +
                    "ffffffff\n" +
                    "# position: 3992, header: 231\n" +
                    "--- !!data\n" +
                    "gggggggg\n" +
                    "# position: 4004, header: 232\n" +
                    "--- !!data\n" +
                    "hhhhhhhh\n" +
                    "# position: 4016, header: 233\n" +
                    "--- !!data\n" +
                    "iiiiiiii\n" +
                    "# position: 4028, header: 234\n" +
                    "--- !!data\n" +
                    "jjjjjjjj\n" +
                    "# position: 4040, header: 235\n" +
                    "--- !!data\n" +
                    "kkkkkkkk\n" +
                    "# position: 4052, header: 236\n" +
                    "--- !!data\n" +
                    "llllllll\n" +
                    "# position: 4064, header: 237\n" +
                    "--- !!data\n" +
                    "mmmmmmmm\n" +
                    "# position: 4076, header: 238\n" +
                    "--- !!data\n" +
                    "nnnnnnnn\n" +
                    "# position: 4088, header: 239\n" +
                    "--- !!data\n" +
                    "oooooooo\n" +
                    "# position: 4100, header: 240\n" +
                    "--- !!data\n" +
                    "pppppppp\n" +
                    "# position: 4112, header: 241\n" +
                    "--- !!data\n" +
                    "qqqqqqqq\n" +
                    "# position: 4124, header: 242\n" +
                    "--- !!data\n" +
                    "rrrrrrrr\n" +
                    "# position: 4136, header: 243\n" +
                    "--- !!data\n" +
                    "ssssssss\n" +
                    "# position: 4148, header: 244\n" +
                    "--- !!data\n" +
                    "tttttttt\n" +
                    "# position: 4160, header: 245\n" +
                    "--- !!data\n" +
                    "uuuuuuuu\n" +
                    "# position: 4172, header: 246\n" +
                    "--- !!data\n" +
                    "vvvvvvvv\n" +
                    "# position: 4184, header: 247\n" +
                    "--- !!data\n" +
                    "wwwwwwww\n" +
                    "# position: 4196, header: 248\n" +
                    "--- !!data\n" +
                    "xxxxxxxx\n" +
                    "# position: 4208, header: 249\n" +
                    "--- !!data\n" +
                    "yyyyyyyy\n" +
                    "# position: 4220, header: 250\n" +
                    "--- !!data\n" +
                    "zzzzzzzz\n" +
                    "# position: 4232, header: 251\n" +
                    "--- !!data\n" +
                    "{{{{{{{{\n" +
                    "# position: 4244, header: 252\n" +
                    "--- !!data\n" +
                    "||||||||\n" +
                    "# position: 4256, header: 253\n" +
                    "--- !!data\n" +
                    "}}}}}}}}\n" +
                    "# position: 4268, header: 254\n" +
                    "--- !!data\n" +
                    "~~~~~~~~\n" +
                    "# position: 4280, header: 255\n" +
                    "--- !!data\n" +
                    "\u007F\u007F\u007F\u007F\u007F\u007F\u007F\u007F\n" +
                    "...\n" +
                    "# 83881784 bytes remaining\n", queue.dump());

        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }

    public boolean postOneMessage(ExcerptAppender appender) {
        appender.writeBytes(outgoingBytes);
        return true;
    }

    public int fetchOneMessage(ExcerptTailer tailer, byte[] using) {
        try (DocumentContext dc = tailer.readingDocument()) {
            return !dc.isPresent() ? -1 : dc.wire().bytes().read(using);
        }
    }
} 