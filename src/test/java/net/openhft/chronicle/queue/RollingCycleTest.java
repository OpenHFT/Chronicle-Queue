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
                    "# position: 710, header: 2 or 3\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885366 bytes remaining\n" +
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
                    "# position: 708, header: 2 or 3\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 83885368 bytes remaining\n" +
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
                    "# 83885366 bytes remaining\n", queue.dump());

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