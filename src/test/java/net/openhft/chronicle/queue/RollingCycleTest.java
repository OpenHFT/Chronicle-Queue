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

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RollingCycleTest {

    private final boolean lazyIndexing;

    public RollingCycleTest(boolean lazyIndexing) {
        this.lazyIndexing = lazyIndexing;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},
                {true}
        });
    }

    @Test
    public void testRollCycle() throws InterruptedException {
        SetTimeProvider stp = new SetTimeProvider();
        long start = System.currentTimeMillis() - 3 * 86_400_000;
        stp.currentTimeMillis(start);

        String basePath = OS.TARGET + "/testRollCycle" + System.nanoTime();
        try (final ChronicleQueue queue = ChronicleQueueBuilder.single(basePath)
                .testBlockSize()
                .timeoutMS(5)
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender().lazyIndexing(lazyIndexing);
            int numWritten = 0;
            for (int h = 0; h < 3; h++) {
                stp.currentTimeMillis(start + TimeUnit.DAYS.toMillis(h));
                for (int i = 0; i < 3; i++) {
                    appender.writeBytes(new TestBytesMarshallable());
                    numWritten++;
                }
            }
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 666,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  576,\n" +
                    "  621,\n" +
                    "  666,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000240             10 6E 61 6D  65 5F 2D 31 31 35 35 38     ·nam e_-11558\n" +
                    "00000250 36 39 33 32 35 6F 0E FB  68 D8 9C B8 19 FC CC 2C 69325o·· h······,\n" +
                    "00000260 35 92 F9 4D 68 E5 F1 2C  55 F0 B8 46 09          5··Mh··, U··F·   \n" +
                    "# position: 621, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000270    10 6E 61 6D 65 5F 2D  31 34 36 35 31 35 34 30  ·name_- 14651540\n" +
                    "00000280 38 33 68 08 F3 B5 D4 D9  BE F7 12 B8 19 27 72 E5 83h····· ·····'r·\n" +
                    "00000290 90 01 7E 1B DA 28 BA 5B  B5 F6                   ··~··(·[ ··      \n" +
                    "# position: 666, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000290                                            0F 6E                ·n\n" +
                    "000002a0 61 6D 65 5F 2D 32 35 38  32 37 36 31 37 32 B1 5D ame_-258 276172·]\n" +
                    "000002b0 7B F2 E3 AE D7 8D A9 9D  E4 EF FB 0C 34 E9 81 37 {······· ····4··7\n" +
                    "000002c0 AD 65 3B C2 B1 7C                                ·e;··|           \n" +
                    "# position: 710, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 326966 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 663,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  576,\n" +
                    "  620,\n" +
                    "  663,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000240             0F 6E 61 6D  65 5F 2D 33 36 39 35 32     ·nam e_-36952\n" +
                    "00000250 36 36 33 32 36 7A CA 28  3D F1 F6 58 9E F3 76 5E 66326z·( =··X··v^\n" +
                    "00000260 64 52 47 4B 73 72 4D DD  23 E9 A8 81             dRGKsrM· #···    \n" +
                    "# position: 620, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000270 0E 6E 61 6D 65 5F 34 39  38 30 37 34 38 37 35 EA ·name_49 8074875·\n" +
                    "00000280 D6 41 C5 CB EB 0C 8A 81  BA EE A8 8C AD 56 95 47 ·A······ ·····V·G\n" +
                    "00000290 90 20 28 7C 10 D4 0A                             · (|···          \n" +
                    "# position: 663, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000290                                   10 6E 61 6D 65             ·name\n" +
                    "000002a0 5F 2D 31 30 32 33 35 39  39 33 38 36 08 AA BC 9F _-102359 9386····\n" +
                    "000002b0 42 D9 D1 60 A5 60 17 E1  B6 7C C7 23 69 83 41 73 B··`·`·· ·|·#i·As\n" +
                    "000002c0 4F 1C E8 B1                                      O···             \n" +
                    "# position: 708, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 326968 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 665,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  576,\n" +
                    "  620,\n" +
                    "  665,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 576, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000240             0F 6E 61 6D  65 5F 2D 38 33 36 35 34     ·nam e_-83654\n" +
                    "00000250 30 33 34 32 6B 54 49 01  02 04 3E 01 B8 2F EC 85 0342kTI· ··>··/··\n" +
                    "00000260 20 4A 2D DA 49 C4 75 BE  BF B9 FE 05              J-·I·u· ····    \n" +
                    "# position: 620, header: 1\n" +
                    "--- !!data #binary\n" +
                    "00000270 10 6E 61 6D 65 5F 2D 31  32 36 36 39 37 32 35 38 ·name_-1 26697258\n" +
                    "00000280 31 FB 5C 68 46 B8 99 5B  24 4F 9D 4C 13 F8 8B 52 1·\\hF··[ $O·L···R\n" +
                    "00000290 7B 23 BA 4F 9C 90 F1 67  8B                      {#·O···g ·       \n" +
                    "# position: 665, header: 2\n" +
                    "--- !!data #binary\n" +
                    "00000290                                         10 6E 61               ·na\n" +
                    "000002a0 6D 65 5F 2D 31 38 31 36  33 34 30 35 38 30 F0 36 me_-1816 340580·6\n" +
                    "000002b0 75 34 EB 57 76 F4 61 C5  94 9F 47 64 29 46 FC F6 u4·Wv·a· ··Gd)F··\n" +
                    "000002c0 48 2F AF 17 B1 30                                H/···0           \n" +
                    "...\n" +
                    "# 326966 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 466,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000170                                         10 6E 61               ·na\n" +
                    "00000180 6D 65 5F 2D 31 39 37 33  39 37 39 35 37 37 8F 2F me_-1973 979577·/\n" +
                    "00000190 4D F9 E8 37 67 20 65 46  D4 3E 84 5B 55 21 70 52 M··7g eF ·>·[U!pR\n" +
                    "000001a0 FF 64 0E 7F CE 34                                ·d···4           \n" +
                    "# position: 422, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001a0                                0F 6E 61 6D 65 5F            ·name_\n" +
                    "000001b0 2D 36 36 32 39 30 33 38  33 33 6A 0F 13 68 D3 C5 -6629038 33j··h··\n" +
                    "000001c0 B4 37 75 B4 28 F3 8F 00  98 6E 49 C8 51 85 02 19 ·7u·(··· ·nI·Q···\n" +
                    "000001d0 B0 3B                                            ·;               \n" +
                    "# position: 466, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001d0                   0F 6E  61 6D 65 5F 2D 34 37 32       ·n ame_-472\n" +
                    "000001e0 38 33 38 33 32 35 96 CA  CF 09 EB D8 22 63 AE 2A 838325·· ····\"c·*\n" +
                    "000001f0 A6 97 6E 4B 74 45 33 75  B9 A7 D4 D2 E1 1C       ··nKtE3u ······  \n" +
                    "# position: 510, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327166 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 463,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000170                                         0E 6E 61               ·na\n" +
                    "00000180 6D 65 5F 39 38 32 36 39  37 30 35 33 79 FC EB FD me_98269 7053y···\n" +
                    "00000190 6C C5 AD 1E D2 5F 14 96  99 B6 08 A7 F3 B5 53 F6 l····_·· ······S·\n" +
                    "000001a0 19 94 FC FB                                      ····             \n" +
                    "# position: 420, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001a0                          0E 6E 61 6D 65 5F 38 38          ·name_88\n" +
                    "000001b0 37 39 33 30 38 37 32 94  07 E9 5F 94 37 E1 43 7C 7930872· ··_·7·C|\n" +
                    "000001c0 17 9E 76 A1 C0 E6 5C BC  7A 67 55 6F B6 E0 E0    ··v···\\· zgUo··· \n" +
                    "# position: 463, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001d0          0F 6E 61 6D 65  5F 31 32 34 38 36 38 35    ·name _1248685\n" +
                    "000001e0 32 34 38 7E B8 2B 0D 29  5F 76 71 57 7D FC 2E 6F 248~·+·) _vqW}·.o\n" +
                    "000001f0 07 0F 81 A8 EB 09 A3 ED  34 BD FF                ········ 4··     \n" +
                    "# position: 507, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327169 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 465,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 0,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000170                                         10 6E 61               ·na\n" +
                    "00000180 6D 65 5F 2D 31 35 38 37  33 39 38 39 39 33 F3 7F me_-1587 398993··\n" +
                    "00000190 DA E8 52 E6 26 47 23 00  F6 81 6C 34 46 57 35 BF ··R·&G#· ··l4FW5·\n" +
                    "000001a0 CF 7D F3 C8 0A 44                                ·}···D           \n" +
                    "# position: 422, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001a0                                0E 6E 61 6D 65 5F            ·name_\n" +
                    "000001b0 35 30 31 30 39 35 31 33  38 66 52 07 23 C1 CC C4 50109513 8fR·#···\n" +
                    "000001c0 6D F9 83 CC 8B 53 2D DF  4E B1 60 9F 7E C9 70 A2 m····S-· N·`·~·p·\n" +
                    "000001d0 B7                                               ·                \n" +
                    "# position: 465, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001d0                0F 6E 61  6D 65 5F 2D 31 36 31 34      ·na me_-1614\n" +
                    "000001e0 32 38 30 35 33 F4 50 A1  35 06 3B F0 03 A1 B6 32 28053·P· 5·;····2\n" +
                    "000001f0 2C D6 11 E7 5B ED 26 88  8C 7D 0F 13 3E          ,···[·&· ·}··>   \n" +
                    "...\n" +
                    "# 327167 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, queue.dump());

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