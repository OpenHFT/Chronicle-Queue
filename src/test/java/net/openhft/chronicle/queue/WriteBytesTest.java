package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static net.openhft.chronicle.queue.RollCycles.TEST2_DAILY;
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
        String dir = OS.TARGET + "/WriteBytesTest";
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
                .rollCycle(TEST2_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();
            for (int i = -127; i < Byte.MAX_VALUE; i++) {
                byte finalI = (byte) i;
                appender.writeBytes(b ->
                        b.writeInt(finalI * 0x01010101));
            }

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 3824,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 352,\n" +
                    "    lastIndex: 254\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: 0,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 352, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 8\n" +
                    "  520,\n" +
                    "  944,\n" +
                    "  1360,\n" +
                    "  1776,\n" +
                    "  2192,\n" +
                    "  2608,\n" +
                    "  3024,\n" +
                    "  3440,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 520, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 16\n" +
                    "  680,\n" +
                    "  696,\n" +
                    "  712,\n" +
                    "  728,\n" +
                    "  744,\n" +
                    "  760,\n" +
                    "  776,\n" +
                    "  792,\n" +
                    "  808,\n" +
                    "  824,\n" +
                    "  840,\n" +
                    "  856,\n" +
                    "  872,\n" +
                    "  888,\n" +
                    "  904,\n" +
                    "  920\n" +
                    "]\n" +
                    "# position: 680, header: 0\n" +
                    "--- !!data #binary\n" +
                    "000002a0                                      81 80 80 80 ········ ········\n" +
                    "# position: 688, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000002b0             82 81 81 81                          ········         \n" +
                    "# position: 696, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000002b0                                      83 82 82 82 ········ ········\n" +
                    "# position: 704, header: 3\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# # Unknown_0x83\n" +
                    "# position: 712, header: 4\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# # Unknown_0x84\n" +
                    "# position: 720, header: 5\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x86\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# # Unknown_0x85\n" +
                    "# position: 728, header: 6\n" +
                    "--- !!data #binary\n" +
                    "000002d0                                      87 86 86 86 ········ ········\n" +
                    "# position: 736, header: 7\n" +
                    "--- !!data #binary\n" +
                    "000002e0             88 87 87 87                          ········         \n" +
                    "# position: 744, header: 8\n" +
                    "--- !!data #binary\n" +
                    "000002e0                                      89 88 88 88 ········ ········\n" +
                    "# position: 752, header: 9\n" +
                    "--- !!data #binary\n" +
                    "!!binary XAEA\n" +
                    "\n" +
                    "# position: 760, header: 10\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8B\n" +
                    "!!binary XAE=\n" +
                    "\n" +
                    "# position: 768, header: 11\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x8C\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# # Unknown_0x8B\n" +
                    "# position: 776, header: 12\n" +
                    "--- !!data #binary\n" +
                    "00000300                                      8D 8C 8C 8C ········ ········\n" +
                    "# position: 784, header: 13\n" +
                    "--- !!data #binary\n" +
                    "00000310             8E 8D 8D 8D                          ········         \n" +
                    "# position: 792, header: 14\n" +
                    "--- !!data #binary\n" +
                    "00000310                                      8F 8E 8E 8E ········ ········\n" +
                    "# position: 800, header: 15\n" +
                    "--- !!data #binary\n" +
                    "# # PADDING\n" +
                    "# position: 808, header: 16\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT32\n" +
                    "# # FLOAT32\n" +
                    "# position: 816, header: 17\n" +
                    "--- !!data #binary\n" +
                    "# # FLOAT64\n" +
                    "# # FLOAT64\n" +
                    "# position: 824, header: 18\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x92\n" +
                    "# # Unknown_0x92\n" +
                    "# position: 832, header: 19\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x93\n" +
                    "# # Unknown_0x93\n" +
                    "# position: 840, header: 20\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x94\n" +
                    "# # Unknown_0x94\n" +
                    "# position: 848, header: 21\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x95\n" +
                    "# # Unknown_0x95\n" +
                    "# position: 856, header: 22\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x96\n" +
                    "# # Unknown_0x96\n" +
                    "# position: 864, header: 23\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x97\n" +
                    "# # Unknown_0x97\n" +
                    "# position: 872, header: 24\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x98\n" +
                    "# # Unknown_0x98\n" +
                    "# position: 880, header: 25\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x99\n" +
                    "# # Unknown_0x99\n" +
                    "# position: 888, header: 26\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9A\n" +
                    "# # Unknown_0x9A\n" +
                    "# position: 896, header: 27\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9B\n" +
                    "# # Unknown_0x9B\n" +
                    "# position: 904, header: 28\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9C\n" +
                    "# # Unknown_0x9C\n" +
                    "# position: 912, header: 29\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9D\n" +
                    "# # Unknown_0x9D\n" +
                    "# position: 920, header: 30\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9E\n" +
                    "# # Unknown_0x9E\n" +
                    "# position: 928, header: 31\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0x9F\n" +
                    "# # Unknown_0x9F\n" +
                    "# position: 936, header: 32\n" +
                    "--- !!data #binary\n" +
                    "!int 160\n" +
                    "# # UUID\n" +
                    "# position: 944, header: 32\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 16\n" +
                    "  936,\n" +
                    "  1112,\n" +
                    "  1128,\n" +
                    "  1144,\n" +
                    "  1160,\n" +
                    "  1176,\n" +
                    "  1192,\n" +
                    "  1208,\n" +
                    "  1224,\n" +
                    "  1240,\n" +
                    "  1256,\n" +
                    "  1272,\n" +
                    "  1288,\n" +
                    "  1304,\n" +
                    "  1320,\n" +
                    "  1336\n" +
                    "]\n" +
                    "# position: 1104, header: 33\n" +
                    "--- !!data #binary\n" +
                    "!int 41377\n" +
                    "!int -1\n" +
                    "# position: 1112, header: 34\n" +
                    "--- !!data #binary\n" +
                    "# # UINT16\n" +
                    "# # UINT16\n" +
                    "# position: 1120, header: 35\n" +
                    "--- !!data #binary\n" +
                    "!byte -93\n" +
                    "# # UINT32\n" +
                    "# position: 1128, header: 36\n" +
                    "--- !!data #binary\n" +
                    "!short -23388\n" +
                    "!byte 0\n" +
                    "# position: 1136, header: 37\n" +
                    "--- !!data #binary\n" +
                    "# # INT16\n" +
                    "# # INT16\n" +
                    "# position: 1144, header: 38\n" +
                    "--- !!data #binary\n" +
                    "# # INT32\n" +
                    "# # INT32\n" +
                    "# position: 1152, header: 39\n" +
                    "--- !!data #binary\n" +
                    "!byte -89\n" +
                    "# # INT64\n" +
                    "# position: 1160, header: 40\n" +
                    "--- !!data #binary\n" +
                    "!short -22360\n" +
                    "!byte 0\n" +
                    "# position: 1168, header: 41\n" +
                    "--- !!data #binary\n" +
                    "# # PLUS_INT16\n" +
                    "# # PLUS_INT16\n" +
                    "# position: 1176, header: 42\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAA\n" +
                    "# # Unknown_0xAA\n" +
                    "# position: 1184, header: 43\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAB\n" +
                    "# # Unknown_0xAB\n" +
                    "# position: 1192, header: 44\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAC\n" +
                    "# # Unknown_0xAC\n" +
                    "# position: 1200, header: 45\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAD\n" +
                    "# # Unknown_0xAD\n" +
                    "# position: 1208, header: 46\n" +
                    "--- !!data #binary\n" +
                    "# # Unknown_0xAE\n" +
                    "# # Unknown_0xAE\n" +
                    "# position: 1216, header: 47\n" +
                    "--- !!data #binary\n" +
                    "false\n" +
                    "# # INT64_0x\n" +
                    "# # EndOfFile\n" +
                    "# position: 1224, header: 48\n" +
                    "--- !!data #binary\n" +
                    "true\n" +
                    "false\n" +
                    "false\n" +
                    "false\n" +
                    "# position: 1232, header: 49\n" +
                    "--- !!data #binary\n" +
                    "000004d0             B2 B1 B1 B1                          ········         \n" +
                    "# position: 1240, header: 50\n" +
                    "--- !!data #binary\n" +
                    "000004d0                                      B3 B2 B2 B2 ········ ········\n" +
                    "# position: 1248, header: 51\n" +
                    "--- !!data #binary\n" +
                    "000004e0             B4 B3 B3 B3                          ········         \n" +
                    "# position: 1256, header: 52\n" +
                    "--- !!data #binary\n" +
                    "000004e0                                      B5 B4 B4 B4 ········ ········\n" +
                    "# position: 1264, header: 53\n" +
                    "--- !!data #binary\n" +
                    "000004f0             B6 B5 B5 B5                          ········         \n" +
                    "# position: 1272, header: 54\n" +
                    "--- !!data #binary\n" +
                    "000004f0                                      B7 B6 B6 B6 ········ ········\n" +
                    "# position: 1280, header: 55\n" +
                    "--- !!data #binary\n" +
                    "00000500             B8 B7 B7 B7                          ········         \n" +
                    "# position: 1288, header: 56\n" +
                    "--- !!data #binary\n" +
                    "00000500                                      B9 B8 B8 B8 ········ ········\n" +
                    "# position: 1296, header: 57\n" +
                    "--- !!data #binary\n" +
                    "\"-941242\": \n" +
                    "# position: 1304, header: 58\n" +
                    "--- !!data #binary\n" +
                    "!!null \"\"\n" +
                    "\"-7483\": \n" +
                    "# position: 1312, header: 59\n" +
                    "--- !!data #binary\n" +
                    "00000520             BC BB BB BB                          ········         \n" +
                    "# position: 1320, header: 60\n" +
                    "--- !!data #binary\n" +
                    "00000520                                      BD BC BC BC ········ ········\n" +
                    "# position: 1328, header: 61\n" +
                    "--- !!data #binary\n" +
                    "00000530             BE BD BD BD                          ········         \n" +
                    "# position: 1336, header: 62\n" +
                    "--- !!data #binary\n" +
                    "00000530                                      BF BE BE BE ········ ········\n" +
                    "# position: 1344, header: 63\n" +
                    "--- !!data #binary\n" +
                    "\"\": # # HINT\n" +
                    "# # HINT\n" +
                    "# # HINT\n" +
                    "# position: 1352, header: 64\n" +
                    "--- !!data #binary\n" +
                    "Ã\u0080: \"\": \"\": \n" +
                    "# position: 1360, header: 64\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 16\n" +
                    "  1352,\n" +
                    "  1528,\n" +
                    "  1544,\n" +
                    "  1560,\n" +
                    "  1576,\n" +
                    "  1592,\n" +
                    "  1608,\n" +
                    "  1624,\n" +
                    "  1640,\n" +
                    "  1656,\n" +
                    "  1672,\n" +
                    "  1688,\n" +
                    "  1704,\n" +
                    "  1720,\n" +
                    "  1736,\n" +
                    "  1752\n" +
                    "]\n" +
                    "# position: 1520, header: 65\n" +
                    "--- !!data #binary\n" +
                    "000005f0             C2 C1 C1 C1                          ········         \n" +
                    "# position: 1528, header: 66\n" +
                    "--- !!data #binary\n" +
                    "Ã\u0082Ã\u0082Ã\u0082: \n" +
                    "# position: 1536, header: 67\n" +
                    "--- !!data #binary\n" +
                    "00000600             C4 C3 C3 C3                          ········         \n" +
                    "# position: 1544, header: 68\n" +
                    "--- !!data #binary\n" +
                    "00000600                                      C5 C4 C4 C4 ········ ········\n" +
                    "# position: 1552, header: 69\n" +
                    "--- !!data #binary\n" +
                    "00000610             C6 C5 C5 C5                          ········         \n" +
                    "# position: 1560, header: 70\n" +
                    "--- !!data #binary\n" +
                    "00000610                                      C7 C6 C6 C6 ········ ········\n" +
                    "# position: 1568, header: 71\n" +
                    "--- !!data #binary\n" +
                    "00000620             C8 C7 C7 C7                          ········         \n" +
                    "# position: 1576, header: 72\n" +
                    "--- !!data #binary\n" +
                    "00000620                                      C9 C8 C8 C8 ········ ········\n" +
                    "# position: 1584, header: 73\n" +
                    "--- !!data #binary\n" +
                    "00000630             CA C9 C9 C9                          ········         \n" +
                    "# position: 1592, header: 74\n" +
                    "--- !!data #binary\n" +
                    "00000630                                      CB CA CA CA ········ ········\n" +
                    "# position: 1600, header: 75\n" +
                    "--- !!data #binary\n" +
                    "00000640             CC CB CB CB                          ········         \n" +
                    "# position: 1608, header: 76\n" +
                    "--- !!data #binary\n" +
                    "00000640                                      CD CC CC CC ········ ········\n" +
                    "# position: 1616, header: 77\n" +
                    "--- !!data #binary\n" +
                    "00000650             CE CD CD CD                          ········         \n" +
                    "# position: 1624, header: 78\n" +
                    "--- !!data #binary\n" +
                    "00000650                                      CF CE CE CE ········ ········\n" +
                    "# position: 1632, header: 79\n" +
                    "--- !!data #binary\n" +
                    "00000660             D0 CF CF CF                          ········         \n" +
                    "# position: 1640, header: 80\n" +
                    "--- !!data #binary\n" +
                    "00000660                                      D1 D0 D0 D0 ········ ········\n" +
                    "# position: 1648, header: 81\n" +
                    "--- !!data #binary\n" +
                    "00000670             D2 D1 D1 D1                          ········         \n" +
                    "# position: 1656, header: 82\n" +
                    "--- !!data #binary\n" +
                    "00000670                                      D3 D2 D2 D2 ········ ········\n" +
                    "# position: 1664, header: 83\n" +
                    "--- !!data #binary\n" +
                    "00000680             D4 D3 D3 D3                          ········         \n" +
                    "# position: 1672, header: 84\n" +
                    "--- !!data #binary\n" +
                    "00000680                                      D5 D4 D4 D4 ········ ········\n" +
                    "# position: 1680, header: 85\n" +
                    "--- !!data #binary\n" +
                    "00000690             D6 D5 D5 D5                          ········         \n" +
                    "# position: 1688, header: 86\n" +
                    "--- !!data #binary\n" +
                    "00000690                                      D7 D6 D6 D6 ········ ········\n" +
                    "# position: 1696, header: 87\n" +
                    "--- !!data #binary\n" +
                    "000006a0             D8 D7 D7 D7                          ········         \n" +
                    "# position: 1704, header: 88\n" +
                    "--- !!data #binary\n" +
                    "000006a0                                      D9 D8 D8 D8 ········ ········\n" +
                    "# position: 1712, header: 89\n" +
                    "--- !!data #binary\n" +
                    "000006b0             DA D9 D9 D9                          ········         \n" +
                    "# position: 1720, header: 90\n" +
                    "--- !!data #binary\n" +
                    "000006b0                                      DB DA DA DA ········ ········\n" +
                    "# position: 1728, header: 91\n" +
                    "--- !!data #binary\n" +
                    "000006c0             DC DB DB DB                          ········         \n" +
                    "# position: 1736, header: 92\n" +
                    "--- !!data #binary\n" +
                    "000006c0                                      DD DC DC DC ········ ········\n" +
                    "# position: 1744, header: 93\n" +
                    "--- !!data #binary\n" +
                    "000006d0             DE DD DD DD                          ········         \n" +
                    "# position: 1752, header: 94\n" +
                    "--- !!data #binary\n" +
                    "000006d0                                      DF DE DE DE ········ ········\n" +
                    "# position: 1760, header: 95\n" +
                    "--- !!data #binary\n" +
                    "000006e0             E0 DF DF DF                          ········         \n" +
                    "# position: 1768, header: 96\n" +
                    "--- !!data #binary\n" +
                    "000006e0                                      E1 E0 E0 E0 ········ ········\n" +
                    "# position: 1776, header: 96\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 16\n" +
                    "  1768,\n" +
                    "  1944,\n" +
                    "  1960,\n" +
                    "  1976,\n" +
                    "  1992,\n" +
                    "  2008,\n" +
                    "  2024,\n" +
                    "  2040,\n" +
                    "  2056,\n" +
                    "  2072,\n" +
                    "  2088,\n" +
                    "  2104,\n" +
                    "  2120,\n" +
                    "  2136,\n" +
                    "  2152,\n" +
                    "  2168\n" +
                    "]\n" +
                    "# position: 1936, header: 97\n" +
                    "--- !!data #binary\n" +
                    "00000790             E2 E1 E1 E1                          ········         \n" +
                    "# position: 1944, header: 98\n" +
                    "--- !!data #binary\n" +
                    "00000790                                      E3 E2 E2 E2 ········ ········\n" +
                    "# position: 1952, header: 99\n" +
                    "--- !!data #binary\n" +
                    "000007a0             E4 E3 E3 E3                          ········         \n" +
                    "# position: 1960, header: 100\n" +
                    "--- !!data #binary\n" +
                    "000007a0                                      E5 E4 E4 E4 ········ ········\n" +
                    "# position: 1968, header: 101\n" +
                    "--- !!data #binary\n" +
                    "000007b0             E6 E5 E5 E5                          ········         \n" +
                    "# position: 1976, header: 102\n" +
                    "--- !!data #binary\n" +
                    "000007b0                                      E7 E6 E6 E6 ········ ········\n" +
                    "# position: 1984, header: 103\n" +
                    "--- !!data #binary\n" +
                    "000007c0             E8 E7 E7 E7                          ········         \n" +
                    "# position: 1992, header: 104\n" +
                    "--- !!data #binary\n" +
                    "000007c0                                      E9 E8 E8 E8 ········ ········\n" +
                    "# position: 2000, header: 105\n" +
                    "--- !!data #binary\n" +
                    "000007d0             EA E9 E9 E9                          ········         \n" +
                    "# position: 2008, header: 106\n" +
                    "--- !!data #binary\n" +
                    "000007d0                                      EB EA EA EA ········ ········\n" +
                    "# position: 2016, header: 107\n" +
                    "--- !!data #binary\n" +
                    "000007e0             EC EB EB EB                          ········         \n" +
                    "# position: 2024, header: 108\n" +
                    "--- !!data #binary\n" +
                    "000007e0                                      ED EC EC EC ········ ········\n" +
                    "# position: 2032, header: 109\n" +
                    "--- !!data #binary\n" +
                    "000007f0             EE ED ED ED                          ········         \n" +
                    "# position: 2040, header: 110\n" +
                    "--- !!data #binary\n" +
                    "000007f0                                      EF EE EE EE ········ ········\n" +
                    "# position: 2048, header: 111\n" +
                    "--- !!data #binary\n" +
                    "00000800             F0 EF EF EF                          ········         \n" +
                    "# position: 2056, header: 112\n" +
                    "--- !!data #binary\n" +
                    "00000800                                      F1 F0 F0 F0 ········ ········\n" +
                    "# position: 2064, header: 113\n" +
                    "--- !!data #binary\n" +
                    "00000810             F2 F1 F1 F1                          ········         \n" +
                    "# position: 2072, header: 114\n" +
                    "--- !!data #binary\n" +
                    "00000810                                      F3 F2 F2 F2 ········ ········\n" +
                    "# position: 2080, header: 115\n" +
                    "--- !!data #binary\n" +
                    "00000820             F4 F3 F3 F3                          ········         \n" +
                    "# position: 2088, header: 116\n" +
                    "--- !!data #binary\n" +
                    "00000820                                      F5 F4 F4 F4 ········ ········\n" +
                    "# position: 2096, header: 117\n" +
                    "--- !!data #binary\n" +
                    "00000830             F6 F5 F5 F5                          ········         \n" +
                    "# position: 2104, header: 118\n" +
                    "--- !!data #binary\n" +
                    "00000830                                      F7 F6 F6 F6 ········ ········\n" +
                    "# position: 2112, header: 119\n" +
                    "--- !!data #binary\n" +
                    "00000840             F8 F7 F7 F7                          ········         \n" +
                    "# position: 2120, header: 120\n" +
                    "--- !!data #binary\n" +
                    "00000840                                      F9 F8 F8 F8 ········ ········\n" +
                    "# position: 2128, header: 121\n" +
                    "--- !!data #binary\n" +
                    "00000850             FA F9 F9 F9                          ········         \n" +
                    "# position: 2136, header: 122\n" +
                    "--- !!data #binary\n" +
                    "00000850                                      FB FA FA FA ········ ········\n" +
                    "# position: 2144, header: 123\n" +
                    "--- !!data #binary\n" +
                    "00000860             FC FB FB FB                          ········         \n" +
                    "# position: 2152, header: 124\n" +
                    "--- !!data #binary\n" +
                    "00000860                                      FD FC FC FC ········ ········\n" +
                    "# position: 2160, header: 125\n" +
                    "--- !!data #binary\n" +
                    "00000870             FE FD FD FD                          ········         \n" +
                    "# position: 2168, header: 126\n" +
                    "--- !!data #binary\n" +
                    "00000870                                      FF FE FE FE ········ ········\n" +
                    "# position: 2176, header: 127\n" +
                    "--- !!data #binary\n" +
                    "0\n" +
                    "0\n" +
                    "0\n" +
                    "0\n" +
                    "# position: 2184, header: 128\n" +
                    "--- !!data #binary\n" +
                    "1\n" +
                    "1\n" +
                    "1\n" +
                    "1\n" +
                    "# position: 2192, header: 128\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 16\n" +
                    "  2184,\n" +
                    "  2360,\n" +
                    "  2376,\n" +
                    "  2392,\n" +
                    "  2408,\n" +
                    "  2424,\n" +
                    "  2440,\n" +
                    "  2456,\n" +
                    "  2472,\n" +
                    "  2488,\n" +
                    "  2504,\n" +
                    "  2520,\n" +
                    "  2536,\n" +
                    "  2552,\n" +
                    "  2568,\n" +
                    "  2584\n" +
                    "]\n" +
                    "# position: 2352, header: 129\n" +
                    "--- !!data #binary\n" +
                    "2\n" +
                    "2\n" +
                    "2\n" +
                    "2\n" +
                    "# position: 2360, header: 130\n" +
                    "--- !!data #binary\n" +
                    "3\n" +
                    "3\n" +
                    "3\n" +
                    "3\n" +
                    "# position: 2368, header: 131\n" +
                    "--- !!data #binary\n" +
                    "4\n" +
                    "4\n" +
                    "4\n" +
                    "4\n" +
                    "# position: 2376, header: 132\n" +
                    "--- !!data #binary\n" +
                    "5\n" +
                    "5\n" +
                    "5\n" +
                    "5\n" +
                    "# position: 2384, header: 133\n" +
                    "--- !!data #binary\n" +
                    "6\n" +
                    "6\n" +
                    "6\n" +
                    "6\n" +
                    "# position: 2392, header: 134\n" +
                    "--- !!data #binary\n" +
                    "7\n" +
                    "7\n" +
                    "7\n" +
                    "7\n" +
                    "# position: 2400, header: 135\n" +
                    "--- !!data #binary\n" +
                    "8\n" +
                    "8\n" +
                    "8\n" +
                    "8\n" +
                    "# position: 2408, header: 136\n" +
                    "--- !!data #binary\n" +
                    "9\n" +
                    "9\n" +
                    "9\n" +
                    "9\n" +
                    "# position: 2416, header: 137\n" +
                    "--- !!data\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "# position: 2424, header: 138\n" +
                    "--- !!data #binary\n" +
                    "11\n" +
                    "11\n" +
                    "11\n" +
                    "11\n" +
                    "# position: 2432, header: 139\n" +
                    "--- !!data #binary\n" +
                    "12\n" +
                    "12\n" +
                    "12\n" +
                    "12\n" +
                    "# position: 2440, header: 140\n" +
                    "--- !!data #binary\n" +
                    "13\n" +
                    "13\n" +
                    "13\n" +
                    "13\n" +
                    "# position: 2448, header: 141\n" +
                    "--- !!data #binary\n" +
                    "14\n" +
                    "14\n" +
                    "14\n" +
                    "14\n" +
                    "# position: 2456, header: 142\n" +
                    "--- !!data #binary\n" +
                    "15\n" +
                    "15\n" +
                    "15\n" +
                    "15\n" +
                    "# position: 2464, header: 143\n" +
                    "--- !!data #binary\n" +
                    "16\n" +
                    "16\n" +
                    "16\n" +
                    "16\n" +
                    "# position: 2472, header: 144\n" +
                    "--- !!data #binary\n" +
                    "17\n" +
                    "17\n" +
                    "17\n" +
                    "17\n" +
                    "# position: 2480, header: 145\n" +
                    "--- !!data #binary\n" +
                    "18\n" +
                    "18\n" +
                    "18\n" +
                    "18\n" +
                    "# position: 2488, header: 146\n" +
                    "--- !!data #binary\n" +
                    "19\n" +
                    "19\n" +
                    "19\n" +
                    "19\n" +
                    "# position: 2496, header: 147\n" +
                    "--- !!data #binary\n" +
                    "20\n" +
                    "20\n" +
                    "20\n" +
                    "20\n" +
                    "# position: 2504, header: 148\n" +
                    "--- !!data #binary\n" +
                    "21\n" +
                    "21\n" +
                    "21\n" +
                    "21\n" +
                    "# position: 2512, header: 149\n" +
                    "--- !!data #binary\n" +
                    "22\n" +
                    "22\n" +
                    "22\n" +
                    "22\n" +
                    "# position: 2520, header: 150\n" +
                    "--- !!data #binary\n" +
                    "23\n" +
                    "23\n" +
                    "23\n" +
                    "23\n" +
                    "# position: 2528, header: 151\n" +
                    "--- !!data #binary\n" +
                    "24\n" +
                    "24\n" +
                    "24\n" +
                    "24\n" +
                    "# position: 2536, header: 152\n" +
                    "--- !!data #binary\n" +
                    "25\n" +
                    "25\n" +
                    "25\n" +
                    "25\n" +
                    "# position: 2544, header: 153\n" +
                    "--- !!data #binary\n" +
                    "26\n" +
                    "26\n" +
                    "26\n" +
                    "26\n" +
                    "# position: 2552, header: 154\n" +
                    "--- !!data #binary\n" +
                    "27\n" +
                    "27\n" +
                    "27\n" +
                    "27\n" +
                    "# position: 2560, header: 155\n" +
                    "--- !!data #binary\n" +
                    "28\n" +
                    "28\n" +
                    "28\n" +
                    "28\n" +
                    "# position: 2568, header: 156\n" +
                    "--- !!data #binary\n" +
                    "29\n" +
                    "29\n" +
                    "29\n" +
                    "29\n" +
                    "# position: 2576, header: 157\n" +
                    "--- !!data #binary\n" +
                    "30\n" +
                    "30\n" +
                    "30\n" +
                    "30\n" +
                    "# position: 2584, header: 158\n" +
                    "--- !!data #binary\n" +
                    "31\n" +
                    "31\n" +
                    "31\n" +
                    "31\n" +
                    "# position: 2592, header: 159\n" +
                    "--- !!data\n" +
                    "    \n" +
                    "# position: 2600, header: 160\n" +
                    "--- !!data\n" +
                    "!!!!\n" +
                    "# position: 2608, header: 160\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 16\n" +
                    "  2600,\n" +
                    "  2776,\n" +
                    "  2792,\n" +
                    "  2808,\n" +
                    "  2824,\n" +
                    "  2840,\n" +
                    "  2856,\n" +
                    "  2872,\n" +
                    "  2888,\n" +
                    "  2904,\n" +
                    "  2920,\n" +
                    "  2936,\n" +
                    "  2952,\n" +
                    "  2968,\n" +
                    "  2984,\n" +
                    "  3000\n" +
                    "]\n" +
                    "# position: 2768, header: 161\n" +
                    "--- !!data\n" +
                    "\"\"\"\"\n" +
                    "# position: 2776, header: 162\n" +
                    "--- !!data\n" +
                    "####\n" +
                    "# position: 2784, header: 163\n" +
                    "--- !!data\n" +
                    "$$$$\n" +
                    "# position: 2792, header: 164\n" +
                    "--- !!data\n" +
                    "%%%%\n" +
                    "# position: 2800, header: 165\n" +
                    "--- !!data\n" +
                    "&&&&\n" +
                    "# position: 2808, header: 166\n" +
                    "--- !!data\n" +
                    "''''\n" +
                    "# position: 2816, header: 167\n" +
                    "--- !!data\n" +
                    "((((\n" +
                    "# position: 2824, header: 168\n" +
                    "--- !!data\n" +
                    "))))\n" +
                    "# position: 2832, header: 169\n" +
                    "--- !!data\n" +
                    "****\n" +
                    "# position: 2840, header: 170\n" +
                    "--- !!data\n" +
                    "++++\n" +
                    "# position: 2848, header: 171\n" +
                    "--- !!data\n" +
                    ",,,,\n" +
                    "# position: 2856, header: 172\n" +
                    "--- !!data\n" +
                    "----\n" +
                    "# position: 2864, header: 173\n" +
                    "--- !!data\n" +
                    "....\n" +
                    "# position: 2872, header: 174\n" +
                    "--- !!data\n" +
                    "////\n" +
                    "# position: 2880, header: 175\n" +
                    "--- !!data\n" +
                    "0000\n" +
                    "# position: 2888, header: 176\n" +
                    "--- !!data\n" +
                    "1111\n" +
                    "# position: 2896, header: 177\n" +
                    "--- !!data\n" +
                    "2222\n" +
                    "# position: 2904, header: 178\n" +
                    "--- !!data\n" +
                    "3333\n" +
                    "# position: 2912, header: 179\n" +
                    "--- !!data\n" +
                    "4444\n" +
                    "# position: 2920, header: 180\n" +
                    "--- !!data\n" +
                    "5555\n" +
                    "# position: 2928, header: 181\n" +
                    "--- !!data\n" +
                    "6666\n" +
                    "# position: 2936, header: 182\n" +
                    "--- !!data\n" +
                    "7777\n" +
                    "# position: 2944, header: 183\n" +
                    "--- !!data\n" +
                    "8888\n" +
                    "# position: 2952, header: 184\n" +
                    "--- !!data\n" +
                    "9999\n" +
                    "# position: 2960, header: 185\n" +
                    "--- !!data\n" +
                    "::::\n" +
                    "# position: 2968, header: 186\n" +
                    "--- !!data\n" +
                    ";;;;\n" +
                    "# position: 2976, header: 187\n" +
                    "--- !!data\n" +
                    "<<<<\n" +
                    "# position: 2984, header: 188\n" +
                    "--- !!data\n" +
                    "====\n" +
                    "# position: 2992, header: 189\n" +
                    "--- !!data\n" +
                    ">>>>\n" +
                    "# position: 3000, header: 190\n" +
                    "--- !!data\n" +
                    "????\n" +
                    "# position: 3008, header: 191\n" +
                    "--- !!data\n" +
                    "@@@@\n" +
                    "# position: 3016, header: 192\n" +
                    "--- !!data\n" +
                    "AAAA\n" +
                    "# position: 3024, header: 192\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 16\n" +
                    "  3016,\n" +
                    "  3192,\n" +
                    "  3208,\n" +
                    "  3224,\n" +
                    "  3240,\n" +
                    "  3256,\n" +
                    "  3272,\n" +
                    "  3288,\n" +
                    "  3304,\n" +
                    "  3320,\n" +
                    "  3336,\n" +
                    "  3352,\n" +
                    "  3368,\n" +
                    "  3384,\n" +
                    "  3400,\n" +
                    "  3416\n" +
                    "]\n" +
                    "# position: 3184, header: 193\n" +
                    "--- !!data\n" +
                    "BBBB\n" +
                    "# position: 3192, header: 194\n" +
                    "--- !!data\n" +
                    "CCCC\n" +
                    "# position: 3200, header: 195\n" +
                    "--- !!data\n" +
                    "DDDD\n" +
                    "# position: 3208, header: 196\n" +
                    "--- !!data\n" +
                    "EEEE\n" +
                    "# position: 3216, header: 197\n" +
                    "--- !!data\n" +
                    "FFFF\n" +
                    "# position: 3224, header: 198\n" +
                    "--- !!data\n" +
                    "GGGG\n" +
                    "# position: 3232, header: 199\n" +
                    "--- !!data\n" +
                    "HHHH\n" +
                    "# position: 3240, header: 200\n" +
                    "--- !!data\n" +
                    "IIII\n" +
                    "# position: 3248, header: 201\n" +
                    "--- !!data\n" +
                    "JJJJ\n" +
                    "# position: 3256, header: 202\n" +
                    "--- !!data\n" +
                    "KKKK\n" +
                    "# position: 3264, header: 203\n" +
                    "--- !!data\n" +
                    "LLLL\n" +
                    "# position: 3272, header: 204\n" +
                    "--- !!data\n" +
                    "MMMM\n" +
                    "# position: 3280, header: 205\n" +
                    "--- !!data\n" +
                    "NNNN\n" +
                    "# position: 3288, header: 206\n" +
                    "--- !!data\n" +
                    "OOOO\n" +
                    "# position: 3296, header: 207\n" +
                    "--- !!data\n" +
                    "PPPP\n" +
                    "# position: 3304, header: 208\n" +
                    "--- !!data\n" +
                    "QQQQ\n" +
                    "# position: 3312, header: 209\n" +
                    "--- !!data\n" +
                    "RRRR\n" +
                    "# position: 3320, header: 210\n" +
                    "--- !!data\n" +
                    "SSSS\n" +
                    "# position: 3328, header: 211\n" +
                    "--- !!data\n" +
                    "TTTT\n" +
                    "# position: 3336, header: 212\n" +
                    "--- !!data\n" +
                    "UUUU\n" +
                    "# position: 3344, header: 213\n" +
                    "--- !!data\n" +
                    "VVVV\n" +
                    "# position: 3352, header: 214\n" +
                    "--- !!data\n" +
                    "WWWW\n" +
                    "# position: 3360, header: 215\n" +
                    "--- !!data\n" +
                    "XXXX\n" +
                    "# position: 3368, header: 216\n" +
                    "--- !!data\n" +
                    "YYYY\n" +
                    "# position: 3376, header: 217\n" +
                    "--- !!data\n" +
                    "ZZZZ\n" +
                    "# position: 3384, header: 218\n" +
                    "--- !!data\n" +
                    "[[[[\n" +
                    "# position: 3392, header: 219\n" +
                    "--- !!data\n" +
                    "\\\\\\\\\n" +
                    "# position: 3400, header: 220\n" +
                    "--- !!data\n" +
                    "]]]]\n" +
                    "# position: 3408, header: 221\n" +
                    "--- !!data\n" +
                    "^^^^\n" +
                    "# position: 3416, header: 222\n" +
                    "--- !!data\n" +
                    "____\n" +
                    "# position: 3424, header: 223\n" +
                    "--- !!data\n" +
                    "````\n" +
                    "# position: 3432, header: 224\n" +
                    "--- !!data\n" +
                    "aaaa\n" +
                    "# position: 3440, header: 224\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 15\n" +
                    "  3432,\n" +
                    "  3608,\n" +
                    "  3624,\n" +
                    "  3640,\n" +
                    "  3656,\n" +
                    "  3672,\n" +
                    "  3688,\n" +
                    "  3704,\n" +
                    "  3720,\n" +
                    "  3736,\n" +
                    "  3752,\n" +
                    "  3768,\n" +
                    "  3784,\n" +
                    "  3800,\n" +
                    "  3816,\n" +
                    "  0\n" +
                    "]\n" +
                    "# position: 3600, header: 225\n" +
                    "--- !!data\n" +
                    "bbbb\n" +
                    "# position: 3608, header: 226\n" +
                    "--- !!data\n" +
                    "cccc\n" +
                    "# position: 3616, header: 227\n" +
                    "--- !!data\n" +
                    "dddd\n" +
                    "# position: 3624, header: 228\n" +
                    "--- !!data\n" +
                    "eeee\n" +
                    "# position: 3632, header: 229\n" +
                    "--- !!data\n" +
                    "ffff\n" +
                    "# position: 3640, header: 230\n" +
                    "--- !!data\n" +
                    "gggg\n" +
                    "# position: 3648, header: 231\n" +
                    "--- !!data\n" +
                    "hhhh\n" +
                    "# position: 3656, header: 232\n" +
                    "--- !!data\n" +
                    "iiii\n" +
                    "# position: 3664, header: 233\n" +
                    "--- !!data\n" +
                    "jjjj\n" +
                    "# position: 3672, header: 234\n" +
                    "--- !!data\n" +
                    "kkkk\n" +
                    "# position: 3680, header: 235\n" +
                    "--- !!data\n" +
                    "llll\n" +
                    "# position: 3688, header: 236\n" +
                    "--- !!data\n" +
                    "mmmm\n" +
                    "# position: 3696, header: 237\n" +
                    "--- !!data\n" +
                    "nnnn\n" +
                    "# position: 3704, header: 238\n" +
                    "--- !!data\n" +
                    "oooo\n" +
                    "# position: 3712, header: 239\n" +
                    "--- !!data\n" +
                    "pppp\n" +
                    "# position: 3720, header: 240\n" +
                    "--- !!data\n" +
                    "qqqq\n" +
                    "# position: 3728, header: 241\n" +
                    "--- !!data\n" +
                    "rrrr\n" +
                    "# position: 3736, header: 242\n" +
                    "--- !!data\n" +
                    "ssss\n" +
                    "# position: 3744, header: 243\n" +
                    "--- !!data\n" +
                    "tttt\n" +
                    "# position: 3752, header: 244\n" +
                    "--- !!data\n" +
                    "uuuu\n" +
                    "# position: 3760, header: 245\n" +
                    "--- !!data\n" +
                    "vvvv\n" +
                    "# position: 3768, header: 246\n" +
                    "--- !!data\n" +
                    "wwww\n" +
                    "# position: 3776, header: 247\n" +
                    "--- !!data\n" +
                    "xxxx\n" +
                    "# position: 3784, header: 248\n" +
                    "--- !!data\n" +
                    "yyyy\n" +
                    "# position: 3792, header: 249\n" +
                    "--- !!data\n" +
                    "zzzz\n" +
                    "# position: 3800, header: 250\n" +
                    "--- !!data\n" +
                    "{{{{\n" +
                    "# position: 3808, header: 251\n" +
                    "--- !!data\n" +
                    "||||\n" +
                    "# position: 3816, header: 252\n" +
                    "--- !!data\n" +
                    "}}}}\n" +
                    "# position: 3824, header: 253\n" +
                    "--- !!data\n" +
                    "~~~~\n" +
                    "...\n" +
                    "# 83882244 bytes remaining\n", queue.dump());

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