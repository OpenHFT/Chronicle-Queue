/*
 * Copyright 2016-2020 chronicle.software
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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RollingCycleTest extends QueueTestCommon {

    @BeforeClass
    public static void sync() {
        System.setProperty("chronicle.queue.synchronousFileShrinking", "true");
    }

    @Test
    public void testRollCycle() {
        SetTimeProvider stp = new SetTimeProvider();
        long start = System.currentTimeMillis() - 3 * 86_400_000;
        stp.currentTimeMillis(start);

        String basePath = OS.getTarget() + "/testRollCycle" + Time.uniqueId();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.single(basePath)
                .testBlockSize()
                .timeoutMS(5)
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            int numWritten = 0;
            for (int h = 0; h < 3; h++) {
                stp.currentTimeMillis(start + TimeUnit.DAYS.toMillis(h));
                for (int i = 0; i < 3; i++) {
                    appender.writeBytes(new TestBytesMarshallable(i));
                    numWritten++;
                }
            }
            String expectedEagerFile1 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    488,\n" +
                    "    2095944040450\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 196, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  296,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 296, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  392,\n" +
                    "  440,\n" +
                    "  488,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 392, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000180                                      10 6e 61 6d              ·nam\n" +
                    "00000190 65 5f 2d 31 31 35 35 34  38 34 35 37 36 7a cb 93 e_-11554 84576z··\n" +
                    "000001a0 3d 38 51 d9 d4 f6 c9 2d  a3 bd 70 39 9b b7 70 e9 =8Q····- ··p9··p·\n" +
                    "000001b0 8c 39 f0 1d 4f                                   ·9··O            \n" +
                    "# position: 440, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001b0                                      10 6e 61 6d              ·nam\n" +
                    "000001c0 65 5f 2d 31 31 35 35 38  36 39 33 32 35 6f 0e fb e_-11558 69325o··\n" +
                    "000001d0 68 d8 9c b8 19 fc cc 2c  35 92 f9 4d 68 e5 f1 2c h······, 5··Mh··,\n" +
                    "000001e0 55 f0 b8 46 09                                   U··F·            \n" +
                    "# position: 488, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001e0                                      10 6e 61 6d              ·nam\n" +
                    "000001f0 65 5f 2d 31 31 35 34 37  31 35 30 37 39 90 45 c5 e_-11547 15079·E·\n" +
                    "00000200 e6 f7 b9 1a 4b ea c3 2f  7f 17 5f 10 01 5c 6e 62 ····K··/ ··_··\\nb\n" +
                    "00000210 fc cc 5e cc da                                   ··^··            \n" +
                    "# position: 536, header: 2 EOF\n";
            String expectedEagerFile2 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    488,\n" +
                    "    2095944040450\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 196, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  296,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 296, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  392,\n" +
                    "  440,\n" +
                    "  488,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 392, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000180                                      10 6e 61 6d              ·nam\n" +
                    "00000190 65 5f 2d 31 31 35 35 34  38 34 35 37 36 7a cb 93 e_-11554 84576z··\n" +
                    "000001a0 3d 38 51 d9 d4 f6 c9 2d  a3 bd 70 39 9b b7 70 e9 =8Q····- ··p9··p·\n" +
                    "000001b0 8c 39 f0 1d 4f                                   ·9··O            \n" +
                    "# position: 440, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001b0                                      10 6e 61 6d              ·nam\n" +
                    "000001c0 65 5f 2d 31 31 35 35 38  36 39 33 32 35 6f 0e fb e_-11558 69325o··\n" +
                    "000001d0 68 d8 9c b8 19 fc cc 2c  35 92 f9 4d 68 e5 f1 2c h······, 5··Mh··,\n" +
                    "000001e0 55 f0 b8 46 09                                   U··F·            \n" +
                    "# position: 488, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001e0                                      10 6e 61 6d              ·nam\n" +
                    "000001f0 65 5f 2d 31 31 35 34 37  31 35 30 37 39 90 45 c5 e_-11547 15079·E·\n" +
                    "00000200 e6 f7 b9 1a 4b ea c3 2f  7f 17 5f 10 01 5c 6e 62 ····K··/ ··_··\\nb\n" +
                    "00000210 fc cc 5e cc da                                   ··^··            \n" +
                    "# position: 536, header: 2 EOF\n";
            String expectedEagerFile3 = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    488,\n" +
                    "    2095944040450\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 196,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 196, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  296,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 296, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  392,\n" +
                    "  440,\n" +
                    "  488,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 392, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000180                                      10 6e 61 6d              ·nam\n" +
                    "00000190 65 5f 2d 31 31 35 35 34  38 34 35 37 36 7a cb 93 e_-11554 84576z··\n" +
                    "000001a0 3d 38 51 d9 d4 f6 c9 2d  a3 bd 70 39 9b b7 70 e9 =8Q····- ··p9··p·\n" +
                    "000001b0 8c 39 f0 1d 4f                                   ·9··O            \n" +
                    "# position: 440, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001b0                                      10 6e 61 6d              ·nam\n" +
                    "000001c0 65 5f 2d 31 31 35 35 38  36 39 33 32 35 6f 0e fb e_-11558 69325o··\n" +
                    "000001d0 68 d8 9c b8 19 fc cc 2c  35 92 f9 4d 68 e5 f1 2c h······, 5··Mh··,\n" +
                    "000001e0 55 f0 b8 46 09                                   U··F·            \n" +
                    "# position: 488, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001e0                                      10 6e 61 6d              ·nam\n" +
                    "000001f0 65 5f 2d 31 31 35 34 37  31 35 30 37 39 90 45 c5 e_-11547 15079·E·\n" +
                    "00000200 e6 f7 b9 1a 4b ea c3 2f  7f 17 5f 10 01 5c 6e 62 ····K··/ ··_··\\nb\n" +
                    "00000210 fc cc 5e cc da                                   ··^··            \n" +
                    "...\n";

           // System.out.println("Wrote: " + numWritten + " messages");

            long numRead = 0;
            final TestBytesMarshallable reusableData = new TestBytesMarshallable(0);
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

           // System.out.println("Wrote " + numWritten + " Read " + numRead);

            String dump = queue.dump();
            assertTrue(dump.contains(expectedEagerFile1));
            assertTrue(dump.contains(expectedEagerFile2));
            assertTrue(dump.contains(expectedEagerFile3));
            try {
                IOTools.deleteDirWithFiles(basePath, 2);
            } catch (IORuntimeException e) {
                e.printStackTrace();
            }
        }
    }

    private static class TestBytesMarshallable implements WriteBytesMarshallable, ReadBytesMarshallable {
        @Nullable
        String _name;
        long _value1;
        long _value2;
        long _value3;

        public TestBytesMarshallable(int i) {
            final Random rand = new Random(i);
            _name = "name_" + rand.nextInt();
            _value1 = rand.nextLong();
            _value2 = rand.nextLong();
            _value3 = rand.nextLong();
        }

        @Override
        public void writeMarshallable(@NotNull BytesOut bytes) {
            bytes.writeUtf8(_name);
            bytes.writeLong(_value1);
            bytes.writeLong(_value2);
            bytes.writeLong(_value3);
        }

        @Override
        public void readMarshallable(@NotNull BytesIn bytes) throws IORuntimeException {
            _name = bytes.readUtf8();
            _value1 = bytes.readLong();
            _value2 = bytes.readLong();
            _value3 = bytes.readLong();
        }
    }
}