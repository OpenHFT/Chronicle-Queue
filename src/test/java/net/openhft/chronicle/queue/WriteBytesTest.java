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
                    "  writePosition: 4320,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 392,\n" +
                    "    lastIndex: 256\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: -1\n" +
                    "}\n" +
                    "# position: 392, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 2\n" +
                    "  688,\n" +
                    "  2524,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 688, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
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
                    "  2272,\n" +
                    "  2320,\n" +
                    "  2368,\n" +
                    "  2416,\n" +
                    "  2464\n" +
                    "]\n" +
                    "# position: 976, header: 0\n" +
                    "--- !!data #binary\n" +
                    "000003d0             80 7F 7F 7F  7F 7F 7F 7F                 ···· ····    \n" +
                    "# position: 988, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000003e0 81 80 80 80 80 80 80 80                          ········         \n" +
                    "# position: 1000, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000003e0                                      82 81 81 81              ····\n" +
                    "000003f0 81 81 81 81                                      ····             \n" +
                    "# position: 1012, header: 3\n" +
                    "--- !!data #binary\n" +
                    "000003f0                          83 82 82 82 82 82 82 82          ········\n" +
                    "# position: 1024, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 1036, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 1048, header: 6\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x86\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 1060, header: 7\n" +
                    "--- !!data #binary\n" +
                    "00000420                          87 86 86 86 86 86 86 86          ········\n" +
                    "# position: 1072, header: 8\n" +
                    "--- !!data #binary\n" +
                    "00000430             88 87 87 87  87 87 87 87                 ···· ····    \n" +
                    "# position: 1084, header: 9\n" +
                    "--- !!data #binary\n" +
                    "00000440 89 88 88 88 88 88 88 88                          ········         \n" +
                    "# position: 1096, header: 10\n" +
                    "--- !!data #binary\n" +
                    "!!binary hAEAQLkGaA==\n" +
                    "\n" +
                    "# position: 1108, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "!!binary hAEAQLkG\n" +
                    "\n" +
                    "# position: 1120, header: 12\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 1132, header: 13\n" +
                    "--- !!data #binary\n" +
                    "00000470 8D 8C 8C 8C 8C 8C 8C 8C                          ········         \n" +
                    "# position: 1144, header: 14\n" +
                    "--- !!data #binary\n" +
                    "00000470                                      8E 8D 8D 8D              ····\n" +
                    "00000480 8D 8D 8D 8D                                      ····             \n" +
                    "# position: 1156, header: 15\n" +
                    "--- !!data #binary\n" +
                    "00000480                          8F 8E 8E 8E 8E 8E 8E 8E          ········\n" +
                    "# position: 1168, header: 16\n" +
                    "--- !!data #binary\n" +
                    "-0.000000000000000000000000000014156185439721036\n" +
                    "# position: 1180, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "-0.00000000000000000000000000005702071897398123\n" +
                    "# # EndOfFile\n" +
                    "# position: 1192, header: 18\n" +
                    "--- !!data #binary\n" +
                    "-753555055760.82\n" +
                    "# position: 1204, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_2\n" +
                    "-48698841.79\n" +
                    "# position: 1216, header: 20\n" +
                    "--- !!data #binary\n" +
                    "-8422085917.3268\n" +
                    "# position: 1228, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_4\n" +
                    "-541098.2421\n" +
                    "# position: 1240, header: 22\n" +
                    "--- !!data #binary\n" +
                    "-93086212.770454\n" +
                    "# position: 1252, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_STOP_6\n" +
                    "-5952.080663\n" +
                    "# position: 1264, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 1276, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 1288, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 1300, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# # FLOAT_SET_LOW_0\n" +
                    "# position: 1312, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# # FLOAT_SET_LOW_2\n" +
                    "# position: 1324, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# # FLOAT_SET_LOW_4\n" +
                    "# position: 1336, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 1348, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 1360, header: 32\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 1372, header: 33\n" +
                    "--- !!data #binary\n" +
                    "!int 160\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# # UUID\n" +
                    "# position: 1384, header: 34\n" +
                    "--- !!data #binary\n" +
                    "!int 41377\n" +
                    "!int 161\n" +
                    "!int 161\n" +
                    "!int -1\n" +
                    "# position: 1396, header: 35\n" +
                    "--- !!data #binary\n" +
                    "2728567458\n" +
                    "!int 41634\n" +
                    "# position: 1408, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "2745410467\n" +
                    "# # EndOfFile\n" +
                    "# position: 1420, header: 37\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte -92\n" +
                    "!byte -92\n" +
                    "!byte 0\n" +
                    "# position: 1432, header: 38\n" +
                    "--- !!data #binary\n" +
                    "!int -1515870811\n" +
                    "!short -23131\n" +
                    "# position: 1444, header: 39\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "!int -1499027802\n" +
                    "# # EndOfFile\n" +
                    "# position: 1456, header: 40\n" +
                    "--- !!data #binary\n" +
                    "!int 167\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# # INT64\n" +
                    "# position: 1468, header: 41\n" +
                    "--- !!data #binary\n" +
                    "!int 43176\n" +
                    "!int 168\n" +
                    "!int 168\n" +
                    "!int -1\n" +
                    "# position: 1480, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # SET_LOW_INT16\n" +
                    "!int 43433\n" +
                    "!int 43433\n" +
                    "# position: 1492, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1504, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1516, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1528, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1540, header: 47\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1552, header: 48\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1564, header: 49\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1576, header: 50\n" +
                    "--- !!data #binary\n" +
                    "00000620                                      B2 B1 B1 B1              ····\n" +
                    "00000630 B1 B1 B1 B1                                      ····             \n" +
                    "# position: 1588, header: 51\n" +
                    "--- !!data #binary\n" +
                    "00000630                          B3 B2 B2 B2 B2 B2 B2 B2          ········\n" +
                    "# position: 1600, header: 52\n" +
                    "--- !!data #binary\n" +
                    "00000640             B4 B3 B3 B3  B3 B3 B3 B3                 ···· ····    \n" +
                    "# position: 1612, header: 53\n" +
                    "--- !!data #binary\n" +
                    "00000650 B5 B4 B4 B4 B4 B4 B4 B4                          ········         \n" +
                    "# position: 1624, header: 54\n" +
                    "--- !!data #binary\n" +
                    "00000650                                      B6 B5 B5 B5              ····\n" +
                    "00000660 B5 B5 B5 B5                                      ····             \n" +
                    "# position: 1636, header: 55\n" +
                    "--- !!data #binary\n" +
                    "00000660                          B7 B6 B6 B6 B6 B6 B6 B6          ········\n" +
                    "# position: 1648, header: 56\n" +
                    "--- !!data #binary\n" +
                    "00000670             B8 B7 B7 B7  B7 B7 B7 B7                 ···· ····    \n" +
                    "# position: 1660, header: 57\n" +
                    "--- !!data #binary\n" +
                    "00000680 B9 B8 B8 B8 B8 B8 B8 B8                          ········         \n" +
                    "# position: 1672, header: 58\n" +
                    "--- !!data #binary\n" +
                    "\"-252662577519802\": \n" +
                    "# position: 1684, header: 59\n" +
                    "--- !!data #binary\n" +
                    "!!null \"\"\n" +
                    "\"-2008556674363\": \n" +
                    "# position: 1696, header: 60\n" +
                    "--- !!data #binary\n" +
                    "000006a0             BC BB BB BB  BB BB BB BB                 ···· ····    \n" +
                    "# position: 1708, header: 61\n" +
                    "--- !!data #binary\n" +
                    "000006b0 BD BC BC BC BC BC BC BC                          ········         \n" +
                    "# position: 1720, header: 62\n" +
                    "--- !!data #binary\n" +
                    "000006b0                                      BE BD BD BD              ····\n" +
                    "000006c0 BD BD BD BD                                      ····             \n" +
                    "# position: 1732, header: 63\n" +
                    "--- !!data #binary\n" +
                    "000006c0                          BF BE BE BE BE BE BE BE          ········\n" +
                    "# position: 1744, header: 64\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1756, header: 65\n" +
                    "--- !!data #binary\n" +
                    "Ã\u0080: \"\": \"\": \"\": \"\": \"\": \"\": \n" +
                    "# position: 1768, header: 66\n" +
                    "--- !!data #binary\n" +
                    "000006e0                                      C2 C1 C1 C1              ····\n" +
                    "000006f0 C1 C1 C1 C1                                      ····             \n" +
                    "# position: 1780, header: 67\n" +
                    "--- !!data #binary\n" +
                    "000006f0                          C3 C2 C2 C2 C2 C2 C2 C2          ········\n" +
                    "# position: 1792, header: 68\n" +
                    "--- !!data #binary\n" +
                    "00000700             C4 C3 C3 C3  C3 C3 C3 C3                 ···· ····    \n" +
                    "# position: 1804, header: 69\n" +
                    "--- !!data #binary\n" +
                    "00000710 C5 C4 C4 C4 C4 C4 C4 C4                          ········         \n" +
                    "# position: 1816, header: 70\n" +
                    "--- !!data #binary\n" +
                    "00000710                                      C6 C5 C5 C5              ····\n" +
                    "00000720 C5 C5 C5 C5                                      ····             \n" +
                    "# position: 1828, header: 71\n" +
                    "--- !!data #binary\n" +
                    "Ã\u0086Ã\u0086Ã\u0086Ã\u0086Ã\u0086Ã\u0086Ã\u0086: \n" +
                    "# position: 1840, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000730             C8 C7 C7 C7  C7 C7 C7 C7                 ···· ····    \n" +
                    "# position: 1852, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000740 C9 C8 C8 C8 C8 C8 C8 C8                          ········         \n" +
                    "# position: 1864, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000740                                      CA C9 C9 C9              ····\n" +
                    "00000750 C9 C9 C9 C9                                      ····             \n" +
                    "# position: 1876, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000750                          CB CA CA CA CA CA CA CA          ········\n" +
                    "# position: 1888, header: 76\n" +
                    "--- !!data #binary\n" +
                    "00000760             CC CB CB CB  CB CB CB CB                 ···· ····    \n" +
                    "# position: 1900, header: 77\n" +
                    "--- !!data #binary\n" +
                    "00000770 CD CC CC CC CC CC CC CC                          ········         \n" +
                    "# position: 1912, header: 78\n" +
                    "--- !!data #binary\n" +
                    "00000770                                      CE CD CD CD              ····\n" +
                    "00000780 CD CD CD CD                                      ····             \n" +
                    "# position: 1924, header: 79\n" +
                    "--- !!data #binary\n" +
                    "00000780                          CF CE CE CE CE CE CE CE          ········\n" +
                    "# position: 1936, header: 80\n" +
                    "--- !!data #binary\n" +
                    "00000790             D0 CF CF CF  CF CF CF CF                 ···· ····    \n" +
                    "# position: 1948, header: 81\n" +
                    "--- !!data #binary\n" +
                    "000007a0 D1 D0 D0 D0 D0 D0 D0 D0                          ········         \n" +
                    "# position: 1960, header: 82\n" +
                    "--- !!data #binary\n" +
                    "000007a0                                      D2 D1 D1 D1              ····\n" +
                    "000007b0 D1 D1 D1 D1                                      ····             \n" +
                    "# position: 1972, header: 83\n" +
                    "--- !!data #binary\n" +
                    "000007b0                          D3 D2 D2 D2 D2 D2 D2 D2          ········\n" +
                    "# position: 1984, header: 84\n" +
                    "--- !!data #binary\n" +
                    "000007c0             D4 D3 D3 D3  D3 D3 D3 D3                 ···· ····    \n" +
                    "# position: 1996, header: 85\n" +
                    "--- !!data #binary\n" +
                    "000007d0 D5 D4 D4 D4 D4 D4 D4 D4                          ········         \n" +
                    "# position: 2008, header: 86\n" +
                    "--- !!data #binary\n" +
                    "000007d0                                      D6 D5 D5 D5              ····\n" +
                    "000007e0 D5 D5 D5 D5                                      ····             \n" +
                    "# position: 2020, header: 87\n" +
                    "--- !!data #binary\n" +
                    "000007e0                          D7 D6 D6 D6 D6 D6 D6 D6          ········\n" +
                    "# position: 2032, header: 88\n" +
                    "--- !!data #binary\n" +
                    "000007f0             D8 D7 D7 D7  D7 D7 D7 D7                 ···· ····    \n" +
                    "# position: 2044, header: 89\n" +
                    "--- !!data #binary\n" +
                    "00000800 D9 D8 D8 D8 D8 D8 D8 D8                          ········         \n" +
                    "# position: 2056, header: 90\n" +
                    "--- !!data #binary\n" +
                    "00000800                                      DA D9 D9 D9              ····\n" +
                    "00000810 D9 D9 D9 D9                                      ····             \n" +
                    "# position: 2068, header: 91\n" +
                    "--- !!data #binary\n" +
                    "00000810                          DB DA DA DA DA DA DA DA          ········\n" +
                    "# position: 2080, header: 92\n" +
                    "--- !!data #binary\n" +
                    "00000820             DC DB DB DB  DB DB DB DB                 ···· ····    \n" +
                    "# position: 2092, header: 93\n" +
                    "--- !!data #binary\n" +
                    "00000830 DD DC DC DC DC DC DC DC                          ········         \n" +
                    "# position: 2104, header: 94\n" +
                    "--- !!data #binary\n" +
                    "00000830                                      DE DD DD DD              ····\n" +
                    "00000840 DD DD DD DD                                      ····             \n" +
                    "# position: 2116, header: 95\n" +
                    "--- !!data #binary\n" +
                    "00000840                          DF DE DE DE DE DE DE DE          ········\n" +
                    "# position: 2128, header: 96\n" +
                    "--- !!data #binary\n" +
                    "00000850             E0 DF DF DF  DF DF DF DF                 ···· ····    \n" +
                    "# position: 2140, header: 97\n" +
                    "--- !!data #binary\n" +
                    "00000860 E1 E0 E0 E0 E0 E0 E0 E0                          ········         \n" +
                    "# position: 2152, header: 98\n" +
                    "--- !!data #binary\n" +
                    "00000860                                      E2 E1 E1 E1              ····\n" +
                    "00000870 E1 E1 E1 E1                                      ····             \n" +
                    "# position: 2164, header: 99\n" +
                    "--- !!data #binary\n" +
                    "00000870                          E3 E2 E2 E2 E2 E2 E2 E2          ········\n" +
                    "# position: 2176, header: 100\n" +
                    "--- !!data #binary\n" +
                    "00000880             E4 E3 E3 E3  E3 E3 E3 E3                 ···· ····    \n" +
                    "# position: 2188, header: 101\n" +
                    "--- !!data #binary\n" +
                    "00000890 E5 E4 E4 E4 E4 E4 E4 E4                          ········         \n" +
                    "# position: 2200, header: 102\n" +
                    "--- !!data #binary\n" +
                    "00000890                                      E6 E5 E5 E5              ····\n" +
                    "000008a0 E5 E5 E5 E5                                      ····             \n" +
                    "# position: 2212, header: 103\n" +
                    "--- !!data #binary\n" +
                    "000008a0                          E7 E6 E6 E6 E6 E6 E6 E6          ········\n" +
                    "# position: 2224, header: 104\n" +
                    "--- !!data #binary\n" +
                    "000008b0             E8 E7 E7 E7  E7 E7 E7 E7                 ···· ····    \n" +
                    "# position: 2236, header: 105\n" +
                    "--- !!data #binary\n" +
                    "000008c0 E9 E8 E8 E8 E8 E8 E8 E8                          ········         \n" +
                    "# position: 2248, header: 106\n" +
                    "--- !!data #binary\n" +
                    "000008c0                                      EA E9 E9 E9              ····\n" +
                    "000008d0 E9 E9 E9 E9                                      ····             \n" +
                    "# position: 2260, header: 107\n" +
                    "--- !!data #binary\n" +
                    "000008d0                          EB EA EA EA EA EA EA EA          ········\n" +
                    "# position: 2272, header: 108\n" +
                    "--- !!data #binary\n" +
                    "000008e0             EC EB EB EB  EB EB EB EB                 ···· ····    \n" +
                    "# position: 2284, header: 109\n" +
                    "--- !!data #binary\n" +
                    "000008f0 ED EC EC EC EC EC EC EC                          ········         \n" +
                    "# position: 2296, header: 110\n" +
                    "--- !!data #binary\n" +
                    "000008f0                                      EE ED ED ED              ····\n" +
                    "00000900 ED ED ED ED                                      ····             \n" +
                    "# position: 2308, header: 111\n" +
                    "--- !!data #binary\n" +
                    "00000900                          EF EE EE EE EE EE EE EE          ········\n" +
                    "# position: 2320, header: 112\n" +
                    "--- !!data #binary\n" +
                    "00000910             F0 EF EF EF  EF EF EF EF                 ···· ····    \n" +
                    "# position: 2332, header: 113\n" +
                    "--- !!data #binary\n" +
                    "00000920 F1 F0 F0 F0 F0 F0 F0 F0                          ········         \n" +
                    "# position: 2344, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000920                                      F2 F1 F1 F1              ····\n" +
                    "00000930 F1 F1 F1 F1                                      ····             \n" +
                    "# position: 2356, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000930                          F3 F2 F2 F2 F2 F2 F2 F2          ········\n" +
                    "# position: 2368, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000940             F4 F3 F3 F3  F3 F3 F3 F3                 ···· ····    \n" +
                    "# position: 2380, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000950 F5 F4 F4 F4 F4 F4 F4 F4                          ········         \n" +
                    "# position: 2392, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000950                                      F6 F5 F5 F5              ····\n" +
                    "00000960 F5 F5 F5 F5                                      ····             \n" +
                    "# position: 2404, header: 119\n" +
                    "--- !!data #binary\n" +
                    "00000960                          F7 F6 F6 F6 F6 F6 F6 F6          ········\n" +
                    "# position: 2416, header: 120\n" +
                    "--- !!data #binary\n" +
                    "00000970             F8 F7 F7 F7  F7 F7 F7 F7                 ···· ····    \n" +
                    "# position: 2428, header: 121\n" +
                    "--- !!data #binary\n" +
                    "00000980 F9 F8 F8 F8 F8 F8 F8 F8                          ········         \n" +
                    "# position: 2440, header: 122\n" +
                    "--- !!data #binary\n" +
                    "00000980                                      FA F9 F9 F9              ····\n" +
                    "00000990 F9 F9 F9 F9                                      ····             \n" +
                    "# position: 2452, header: 123\n" +
                    "--- !!data #binary\n" +
                    "00000990                          FB FA FA FA FA FA FA FA          ········\n" +
                    "# position: 2464, header: 124\n" +
                    "--- !!data #binary\n" +
                    "000009a0             FC FB FB FB  FB FB FB FB                 ···· ····    \n" +
                    "# position: 2476, header: 125\n" +
                    "--- !!data #binary\n" +
                    "000009b0 FD FC FC FC FC FC FC FC                          ········         \n" +
                    "# position: 2488, header: 126\n" +
                    "--- !!data #binary\n" +
                    "000009b0                                      FE FD FD FD              ····\n" +
                    "000009c0 FD FD FD FD                                      ····             \n" +
                    "# position: 2500, header: 127\n" +
                    "--- !!data #binary\n" +
                    "000009c0                          FF FE FE FE FE FE FE FE          ········\n" +
                    "# position: 2512, header: 128\n" +
                    "--- !!data #binary\n" +
                    "000009d0             00 00 00 00  00 00 00 00                 ···· ····    \n" +
                    "# position: 2524, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 32\n" +
                    "  2512,\n" +
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
                    "  4092,\n" +
                    "  4140,\n" +
                    "  4188,\n" +
                    "  4236,\n" +
                    "  4284\n" +
                    "]\n" +
                    "# position: 2808, header: 129\n" +
                    "--- !!data #binary\n" +
                    "00000af0                                      01 01 01 01              ····\n" +
                    "00000b00 01 01 01 01                                      ····             \n" +
                    "# position: 2820, header: 130\n" +
                    "--- !!data #binary\n" +
                    "00000b00                          02 02 02 02 02 02 02 02          ········\n" +
                    "# position: 2832, header: 131\n" +
                    "--- !!data #binary\n" +
                    "00000b10             03 03 03 03  03 03 03 03                 ···· ····    \n" +
                    "# position: 2844, header: 132\n" +
                    "--- !!data #binary\n" +
                    "00000b20 04 04 04 04 04 04 04 04                          ········         \n" +
                    "# position: 2856, header: 133\n" +
                    "--- !!data #binary\n" +
                    "00000b20                                      05 05 05 05              ····\n" +
                    "00000b30 05 05 05 05                                      ····             \n" +
                    "# position: 2868, header: 134\n" +
                    "--- !!data #binary\n" +
                    "00000b30                          06 06 06 06 06 06 06 06          ········\n" +
                    "# position: 2880, header: 135\n" +
                    "--- !!data #binary\n" +
                    "00000b40             07 07 07 07  07 07 07 07                 ···· ····    \n" +
                    "# position: 2892, header: 136\n" +
                    "--- !!data #binary\n" +
                    "00000b50 08 08 08 08 08 08 08 08                          ········         \n" +
                    "# position: 2904, header: 137\n" +
                    "--- !!data #binary\n" +
                    "00000b50                                      09 09 09 09              ····\n" +
                    "00000b60 09 09 09 09                                      ····             \n" +
                    "# position: 2916, header: 138\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2928, header: 139\n" +
                    "--- !!data #binary\n" +
                    "00000b70             0B 0B 0B 0B  0B 0B 0B 0B                 ···· ····    \n" +
                    "# position: 2940, header: 140\n" +
                    "--- !!data #binary\n" +
                    "00000b80 0C 0C 0C 0C 0C 0C 0C 0C                          ········         \n" +
                    "# position: 2952, header: 141\n" +
                    "--- !!data #binary\n" +
                    "00000b80                                      0D 0D 0D 0D              ····\n" +
                    "00000b90 0D 0D 0D 0D                                      ····             \n" +
                    "# position: 2964, header: 142\n" +
                    "--- !!data #binary\n" +
                    "00000b90                          0E 0E 0E 0E 0E 0E 0E 0E          ········\n" +
                    "# position: 2976, header: 143\n" +
                    "--- !!data #binary\n" +
                    "00000ba0             0F 0F 0F 0F  0F 0F 0F 0F                 ···· ····    \n" +
                    "# position: 2988, header: 144\n" +
                    "--- !!data #binary\n" +
                    "00000bb0 10 10 10 10 10 10 10 10                          ········         \n" +
                    "# position: 3000, header: 145\n" +
                    "--- !!data #binary\n" +
                    "00000bb0                                      11 11 11 11              ····\n" +
                    "00000bc0 11 11 11 11                                      ····             \n" +
                    "# position: 3012, header: 146\n" +
                    "--- !!data #binary\n" +
                    "00000bc0                          12 12 12 12 12 12 12 12          ········\n" +
                    "# position: 3024, header: 147\n" +
                    "--- !!data #binary\n" +
                    "00000bd0             13 13 13 13  13 13 13 13                 ···· ····    \n" +
                    "# position: 3036, header: 148\n" +
                    "--- !!data #binary\n" +
                    "00000be0 14 14 14 14 14 14 14 14                          ········         \n" +
                    "# position: 3048, header: 149\n" +
                    "--- !!data #binary\n" +
                    "00000be0                                      15 15 15 15              ····\n" +
                    "00000bf0 15 15 15 15                                      ····             \n" +
                    "# position: 3060, header: 150\n" +
                    "--- !!data #binary\n" +
                    "00000bf0                          16 16 16 16 16 16 16 16          ········\n" +
                    "# position: 3072, header: 151\n" +
                    "--- !!data #binary\n" +
                    "00000c00             17 17 17 17  17 17 17 17                 ···· ····    \n" +
                    "# position: 3084, header: 152\n" +
                    "--- !!data #binary\n" +
                    "00000c10 18 18 18 18 18 18 18 18                          ········         \n" +
                    "# position: 3096, header: 153\n" +
                    "--- !!data #binary\n" +
                    "00000c10                                      19 19 19 19              ····\n" +
                    "00000c20 19 19 19 19                                      ····             \n" +
                    "# position: 3108, header: 154\n" +
                    "--- !!data #binary\n" +
                    "00000c20                          1A 1A 1A 1A 1A 1A 1A 1A          ········\n" +
                    "# position: 3120, header: 155\n" +
                    "--- !!data #binary\n" +
                    "00000c30             1B 1B 1B 1B  1B 1B 1B 1B                 ···· ····    \n" +
                    "# position: 3132, header: 156\n" +
                    "--- !!data #binary\n" +
                    "00000c40 1C 1C 1C 1C 1C 1C 1C 1C                          ········         \n" +
                    "# position: 3144, header: 157\n" +
                    "--- !!data #binary\n" +
                    "00000c40                                      1D 1D 1D 1D              ····\n" +
                    "00000c50 1D 1D 1D 1D                                      ····             \n" +
                    "# position: 3156, header: 158\n" +
                    "--- !!data #binary\n" +
                    "00000c50                          1E 1E 1E 1E 1E 1E 1E 1E          ········\n" +
                    "# position: 3168, header: 159\n" +
                    "--- !!data #binary\n" +
                    "00000c60             1F 1F 1F 1F  1F 1F 1F 1F                 ···· ····    \n" +
                    "# position: 3180, header: 160\n" +
                    "--- !!data\n" +
                    "        \n" +
                    "# position: 3192, header: 161\n" +
                    "--- !!data\n" +
                    "!!!!!!!!\n" +
                    "# position: 3204, header: 162\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\"\"\"\"\n" +
                    "# position: 3216, header: 163\n" +
                    "--- !!data\n" +
                    "########\n" +
                    "# position: 3228, header: 164\n" +
                    "--- !!data\n" +
                    "$$$$$$$$\n" +
                    "# position: 3240, header: 165\n" +
                    "--- !!data\n" +
                    "%%%%%%%%\n" +
                    "# position: 3252, header: 166\n" +
                    "--- !!data\n" +
                    "&&&&&&&&\n" +
                    "# position: 3264, header: 167\n" +
                    "--- !!data\n" +
                    "''''''''\n" +
                    "# position: 3276, header: 168\n" +
                    "--- !!data\n" +
                    "((((((((\n" +
                    "# position: 3288, header: 169\n" +
                    "--- !!data\n" +
                    "))))))))\n" +
                    "# position: 3300, header: 170\n" +
                    "--- !!data\n" +
                    "********\n" +
                    "# position: 3312, header: 171\n" +
                    "--- !!data\n" +
                    "++++++++\n" +
                    "# position: 3324, header: 172\n" +
                    "--- !!data\n" +
                    ",,,,,,,,\n" +
                    "# position: 3336, header: 173\n" +
                    "--- !!data\n" +
                    "--------\n" +
                    "# position: 3348, header: 174\n" +
                    "--- !!data\n" +
                    "........\n" +
                    "# position: 3360, header: 175\n" +
                    "--- !!data\n" +
                    "////////\n" +
                    "# position: 3372, header: 176\n" +
                    "--- !!data\n" +
                    "00000000\n" +
                    "# position: 3384, header: 177\n" +
                    "--- !!data\n" +
                    "11111111\n" +
                    "# position: 3396, header: 178\n" +
                    "--- !!data\n" +
                    "22222222\n" +
                    "# position: 3408, header: 179\n" +
                    "--- !!data\n" +
                    "33333333\n" +
                    "# position: 3420, header: 180\n" +
                    "--- !!data\n" +
                    "44444444\n" +
                    "# position: 3432, header: 181\n" +
                    "--- !!data\n" +
                    "55555555\n" +
                    "# position: 3444, header: 182\n" +
                    "--- !!data\n" +
                    "66666666\n" +
                    "# position: 3456, header: 183\n" +
                    "--- !!data\n" +
                    "77777777\n" +
                    "# position: 3468, header: 184\n" +
                    "--- !!data\n" +
                    "88888888\n" +
                    "# position: 3480, header: 185\n" +
                    "--- !!data\n" +
                    "99999999\n" +
                    "# position: 3492, header: 186\n" +
                    "--- !!data\n" +
                    "::::::::\n" +
                    "# position: 3504, header: 187\n" +
                    "--- !!data\n" +
                    ";;;;;;;;\n" +
                    "# position: 3516, header: 188\n" +
                    "--- !!data\n" +
                    "<<<<<<<<\n" +
                    "# position: 3528, header: 189\n" +
                    "--- !!data\n" +
                    "========\n" +
                    "# position: 3540, header: 190\n" +
                    "--- !!data\n" +
                    ">>>>>>>>\n" +
                    "# position: 3552, header: 191\n" +
                    "--- !!data\n" +
                    "????????\n" +
                    "# position: 3564, header: 192\n" +
                    "--- !!data\n" +
                    "@@@@@@@@\n" +
                    "# position: 3576, header: 193\n" +
                    "--- !!data\n" +
                    "AAAAAAAA\n" +
                    "# position: 3588, header: 194\n" +
                    "--- !!data\n" +
                    "BBBBBBBB\n" +
                    "# position: 3600, header: 195\n" +
                    "--- !!data\n" +
                    "CCCCCCCC\n" +
                    "# position: 3612, header: 196\n" +
                    "--- !!data\n" +
                    "DDDDDDDD\n" +
                    "# position: 3624, header: 197\n" +
                    "--- !!data\n" +
                    "EEEEEEEE\n" +
                    "# position: 3636, header: 198\n" +
                    "--- !!data\n" +
                    "FFFFFFFF\n" +
                    "# position: 3648, header: 199\n" +
                    "--- !!data\n" +
                    "GGGGGGGG\n" +
                    "# position: 3660, header: 200\n" +
                    "--- !!data\n" +
                    "HHHHHHHH\n" +
                    "# position: 3672, header: 201\n" +
                    "--- !!data\n" +
                    "IIIIIIII\n" +
                    "# position: 3684, header: 202\n" +
                    "--- !!data\n" +
                    "JJJJJJJJ\n" +
                    "# position: 3696, header: 203\n" +
                    "--- !!data\n" +
                    "KKKKKKKK\n" +
                    "# position: 3708, header: 204\n" +
                    "--- !!data\n" +
                    "LLLLLLLL\n" +
                    "# position: 3720, header: 205\n" +
                    "--- !!data\n" +
                    "MMMMMMMM\n" +
                    "# position: 3732, header: 206\n" +
                    "--- !!data\n" +
                    "NNNNNNNN\n" +
                    "# position: 3744, header: 207\n" +
                    "--- !!data\n" +
                    "OOOOOOOO\n" +
                    "# position: 3756, header: 208\n" +
                    "--- !!data\n" +
                    "PPPPPPPP\n" +
                    "# position: 3768, header: 209\n" +
                    "--- !!data\n" +
                    "QQQQQQQQ\n" +
                    "# position: 3780, header: 210\n" +
                    "--- !!data\n" +
                    "RRRRRRRR\n" +
                    "# position: 3792, header: 211\n" +
                    "--- !!data\n" +
                    "SSSSSSSS\n" +
                    "# position: 3804, header: 212\n" +
                    "--- !!data\n" +
                    "TTTTTTTT\n" +
                    "# position: 3816, header: 213\n" +
                    "--- !!data\n" +
                    "UUUUUUUU\n" +
                    "# position: 3828, header: 214\n" +
                    "--- !!data\n" +
                    "VVVVVVVV\n" +
                    "# position: 3840, header: 215\n" +
                    "--- !!data\n" +
                    "WWWWWWWW\n" +
                    "# position: 3852, header: 216\n" +
                    "--- !!data\n" +
                    "XXXXXXXX\n" +
                    "# position: 3864, header: 217\n" +
                    "--- !!data\n" +
                    "YYYYYYYY\n" +
                    "# position: 3876, header: 218\n" +
                    "--- !!data\n" +
                    "ZZZZZZZZ\n" +
                    "# position: 3888, header: 219\n" +
                    "--- !!data\n" +
                    "[[[[[[[[\n" +
                    "# position: 3900, header: 220\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\\\\\\\\\n" +
                    "# position: 3912, header: 221\n" +
                    "--- !!data\n" +
                    "]]]]]]]]\n" +
                    "# position: 3924, header: 222\n" +
                    "--- !!data\n" +
                    "^^^^^^^^\n" +
                    "# position: 3936, header: 223\n" +
                    "--- !!data\n" +
                    "________\n" +
                    "# position: 3948, header: 224\n" +
                    "--- !!data\n" +
                    "````````\n" +
                    "# position: 3960, header: 225\n" +
                    "--- !!data\n" +
                    "aaaaaaaa\n" +
                    "# position: 3972, header: 226\n" +
                    "--- !!data\n" +
                    "bbbbbbbb\n" +
                    "# position: 3984, header: 227\n" +
                    "--- !!data\n" +
                    "cccccccc\n" +
                    "# position: 3996, header: 228\n" +
                    "--- !!data\n" +
                    "dddddddd\n" +
                    "# position: 4008, header: 229\n" +
                    "--- !!data\n" +
                    "eeeeeeee\n" +
                    "# position: 4020, header: 230\n" +
                    "--- !!data\n" +
                    "ffffffff\n" +
                    "# position: 4032, header: 231\n" +
                    "--- !!data\n" +
                    "gggggggg\n" +
                    "# position: 4044, header: 232\n" +
                    "--- !!data\n" +
                    "hhhhhhhh\n" +
                    "# position: 4056, header: 233\n" +
                    "--- !!data\n" +
                    "iiiiiiii\n" +
                    "# position: 4068, header: 234\n" +
                    "--- !!data\n" +
                    "jjjjjjjj\n" +
                    "# position: 4080, header: 235\n" +
                    "--- !!data\n" +
                    "kkkkkkkk\n" +
                    "# position: 4092, header: 236\n" +
                    "--- !!data\n" +
                    "llllllll\n" +
                    "# position: 4104, header: 237\n" +
                    "--- !!data\n" +
                    "mmmmmmmm\n" +
                    "# position: 4116, header: 238\n" +
                    "--- !!data\n" +
                    "nnnnnnnn\n" +
                    "# position: 4128, header: 239\n" +
                    "--- !!data\n" +
                    "oooooooo\n" +
                    "# position: 4140, header: 240\n" +
                    "--- !!data\n" +
                    "pppppppp\n" +
                    "# position: 4152, header: 241\n" +
                    "--- !!data\n" +
                    "qqqqqqqq\n" +
                    "# position: 4164, header: 242\n" +
                    "--- !!data\n" +
                    "rrrrrrrr\n" +
                    "# position: 4176, header: 243\n" +
                    "--- !!data\n" +
                    "ssssssss\n" +
                    "# position: 4188, header: 244\n" +
                    "--- !!data\n" +
                    "tttttttt\n" +
                    "# position: 4200, header: 245\n" +
                    "--- !!data\n" +
                    "uuuuuuuu\n" +
                    "# position: 4212, header: 246\n" +
                    "--- !!data\n" +
                    "vvvvvvvv\n" +
                    "# position: 4224, header: 247\n" +
                    "--- !!data\n" +
                    "wwwwwwww\n" +
                    "# position: 4236, header: 248\n" +
                    "--- !!data\n" +
                    "xxxxxxxx\n" +
                    "# position: 4248, header: 249\n" +
                    "--- !!data\n" +
                    "yyyyyyyy\n" +
                    "# position: 4260, header: 250\n" +
                    "--- !!data\n" +
                    "zzzzzzzz\n" +
                    "# position: 4272, header: 251\n" +
                    "--- !!data\n" +
                    "{{{{{{{{\n" +
                    "# position: 4284, header: 252\n" +
                    "--- !!data\n" +
                    "||||||||\n" +
                    "# position: 4296, header: 253\n" +
                    "--- !!data\n" +
                    "}}}}}}}}\n" +
                    "# position: 4308, header: 254\n" +
                    "--- !!data\n" +
                    "~~~~~~~~\n" +
                    "# position: 4320, header: 255\n" +
                    "--- !!data\n" +
                    "\u007F\u007F\u007F\u007F\u007F\u007F\u007F\u007F\n" +
                    "...\n" +
                    "# 83881744 bytes remaining\n", queue.dump());

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