package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class RollingCycleTest {

    @Test
    public void testRollCycle() throws InterruptedException {
        SetTimeProvider stp = new SetTimeProvider();
        long start = System.currentTimeMillis() - 3 * 86_400_000;
        stp.currentTimeMillis(start);

        String basePath = OS.TARGET + "/testRollCycle" + System.nanoTime();
        try (final ChronicleQueue queue = ChronicleQueueBuilder.single(basePath)
                .timeoutMS(5)
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            int numWritten = 0;
            for (int h = 0; h < 3; h++) {
                stp.currentTimeMillis(start + h * 86_400_000);
                for (int i = 0; i < 3; i++) {
                    appender.writeBytes(new TestBytesMarshallable());
                    numWritten++;
                }
            }
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 642,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 352,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: 0,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 352, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  456,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 456, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  552,\n" +
                    "  597,\n" +
                    "  642,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 552, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000220                                      10 6E 61 6D              ·nam\n" +
                    "00000230 65 5F 2D 31 31 35 35 38  36 39 33 32 35 6F 0E FB e_-11558 69325o··\n" +
                    "00000240 68 D8 9C B8 19 FC CC 2C  35 92 F9 4D 68 E5 F1 2C h······, 5··Mh··,\n" +
                    "00000250 55 F0 B8 46 09                                   U··F·            \n" +
                    "# position: 597, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000250                             10 6E 61 6D 65 5F 2D           ·name_-\n" +
                    "00000260 31 34 36 35 31 35 34 30  38 33 68 08 F3 B5 D4 D9 14651540 83h·····\n" +
                    "00000270 BE F7 12 B8 19 27 72 E5  90 01 7E 1B DA 28 BA 5B ·····'r· ··~··(·[\n" +
                    "00000280 B5 F6                                            ··               \n" +
                    "# position: 642, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000280                   0F 6E  61 6D 65 5F 2D 32 35 38       ·n ame_-258\n" +
                    "00000290 32 37 36 31 37 32 B1 5D  7B F2 E3 AE D7 8D A9 9D 276172·] {·······\n" +
                    "000002a0 E4 EF FB 0C 34 E9 81 37  AD 65 3B C2 B1 7C       ····4··7 ·e;··|  \n" +
                    "# position: 686, header: 2 or 3\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885390 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 639,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 352,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: 0,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 352, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  456,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 456, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  552,\n" +
                    "  596,\n" +
                    "  639,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 552, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000220                                      0F 6E 61 6D              ·nam\n" +
                    "00000230 65 5F 2D 33 36 39 35 32  36 36 33 32 36 7A CA 28 e_-36952 66326z·(\n" +
                    "00000240 3D F1 F6 58 9E F3 76 5E  64 52 47 4B 73 72 4D DD =··X··v^ dRGKsrM·\n" +
                    "00000250 23 E9 A8 81                                      #···             \n" +
                    "# position: 596, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000250                          0E 6E 61 6D 65 5F 34 39          ·name_49\n" +
                    "00000260 38 30 37 34 38 37 35 EA  D6 41 C5 CB EB 0C 8A 81 8074875· ·A······\n" +
                    "00000270 BA EE A8 8C AD 56 95 47  90 20 28 7C 10 D4 0A    ·····V·G · (|··· \n" +
                    "# position: 639, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000280          10 6E 61 6D 65  5F 2D 31 30 32 33 35 39    ·name _-102359\n" +
                    "00000290 39 33 38 36 08 AA BC 9F  42 D9 D1 60 A5 60 17 E1 9386···· B··`·`··\n" +
                    "000002a0 B6 7C C7 23 69 83 41 73  4F 1C E8 B1             ·|·#i·As O···    \n" +
                    "# position: 684, header: 2 or 3\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885392 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 641,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 352,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: 0,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 352, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  456,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 456, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  552,\n" +
                    "  596,\n" +
                    "  641,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 552, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000220                                      0F 6E 61 6D              ·nam\n" +
                    "00000230 65 5F 2D 38 33 36 35 34  30 33 34 32 6B 54 49 01 e_-83654 0342kTI·\n" +
                    "00000240 02 04 3E 01 B8 2F EC 85  20 4A 2D DA 49 C4 75 BE ··>··/··  J-·I·u·\n" +
                    "00000250 BF B9 FE 05                                      ····             \n" +
                    "# position: 596, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000250                          10 6E 61 6D 65 5F 2D 31          ·name_-1\n" +
                    "00000260 32 36 36 39 37 32 35 38  31 FB 5C 68 46 B8 99 5B 26697258 1·\\hF··[\n" +
                    "00000270 24 4F 9D 4C 13 F8 8B 52  7B 23 BA 4F 9C 90 F1 67 $O·L···R {#·O···g\n" +
                    "00000280 8B                                               ·                \n" +
                    "# position: 641, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000280                10 6E 61  6D 65 5F 2D 31 38 31 36      ·na me_-1816\n" +
                    "00000290 33 34 30 35 38 30 F0 36  75 34 EB 57 76 F4 61 C5 340580·6 u4·Wv·a·\n" +
                    "000002a0 94 9F 47 64 29 46 FC F6  48 2F AF 17 B1 30       ··Gd)F·· H/···0  \n" +
                    "...\n" +
                    "# 83885390 bytes remaining\n", queue.dump());

            System.out.println("Wrote: " + numWritten + " messages");

            long numRead = 0;
            final TestBytesMarshallable reusableData = new TestBytesMarshallable();
            final ExcerptTailer currentPosTailer = queue.createTailer()
                    .toStart();
            final ExcerptTailer endPosTailer = queue.createTailer().toEnd();
            while (currentPosTailer.index() < endPosTailer.index()) {
                try {
                    assertTrue(currentPosTailer.readBytes(reusableData));
                } catch (AssertionError e) {
                    System.err.println("Could not read data at index: " +
                            numRead + " " +
                            Long.toHexString(currentPosTailer.cycle()) + " " +
                            Long.toHexString(currentPosTailer.index()) + " " +
                            e.getMessage() + " " +
                            e);
                    throw e;
                }
                numRead++;
            }
            assertFalse(currentPosTailer.readBytes(reusableData));

            System.out.println("Wrote " + numWritten + " Read " + numRead);
            try {
                IOTools.deleteDirWithFiles(basePath, 2);
            } catch (IORuntimeException e) {
                e.printStackTrace();
            }
        }
    }

    private static class TestBytesMarshallable implements WriteBytesMarshallable, ReadBytesMarshallable {
        private static final Random rand = new Random(1);
        String _name;
        long _value1;
        long _value2;
        long _value3;

        public TestBytesMarshallable() {
            _name = "name_" + rand.nextInt();
            _value1 = rand.nextLong();
            _value2 = rand.nextLong();
            _value3 = rand.nextLong();
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeUtf8(_name);
            bytes.writeLong(_value1);
            bytes.writeLong(_value2);
            bytes.writeLong(_value3);
        }

        @Override
        public void readMarshallable(BytesIn bytes) throws IORuntimeException {
            _name = bytes.readUtf8();
            _value1 = bytes.readLong();
            _value2 = bytes.readLong();
            _value3 = bytes.readLong();
        }
    }
}