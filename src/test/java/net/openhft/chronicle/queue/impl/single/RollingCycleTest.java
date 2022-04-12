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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RollingCycleTest extends QueueTestCommon {
    protected final boolean named;

    public RollingCycleTest(boolean named) {
        this.named = named;
    }

    @Parameterized.Parameters(name = "named={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{true},
                new Object[]{false}
        );
    }

    @BeforeClass
    public static void sync() {
        System.setProperty("chronicle.queue.synchronousFileShrinking", "true");
    }

    @Test
    public void testRollCycle() {
        SetTimeProvider stp = new SetTimeProvider();
        long start = 19059 * 86_400_000L;
        stp.currentTimeMillis(start);

        String basePath = OS.getTarget() + "/testRollCycle" + Time.uniqueId();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.single(basePath)
                .testBlockSize()
                .timeoutMS(5)
                .rollCycle(RollCycles.TEST_DAILY)
                .timeProvider(stp)
                .build()) {

            final ExcerptAppender appender = queue.acquireAppender();
            for (int h = 0; h < 3; h++) {
                stp.currentTimeMillis(start + TimeUnit.DAYS.toMillis(h));
                for (int i = 0; i < 3; i++) {
                    appender.writeBytes(new TestBytesMarshallable(i));
                }
            }
            String expected = "" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T1', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 180, header: 0\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 19061\n" +
                    "# position: 216, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 19059\n" +
                    "# position: 256, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 7\n" +
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
                    (named
                            ? "# position: 472, header: 7\n" +
                            "--- !!data #binary\n" +
                            "index.named: 81866371629059\n" +
                            "# position: 504, header: 8\n" +
                            "--- !!data #binary\n" +
                            "index.named2: 81866371629059\n" +
                            "...\n" +
                            "# 130532 bytes remaining\n"
                            : "...\n" +
                            "# 130596 bytes remaining\n") +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    496,\n" +
                    "    2130303778818\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 304, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  400,\n" +
                    "  448,\n" +
                    "  496,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 400, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000190             10 6e 61 6d  65 5f 2d 31 31 35 35 34     ·nam e_-11554\n" +
                    "000001a0 38 34 35 37 36 7a cb 93  3d 38 51 d9 d4 f6 c9 2d 84576z·· =8Q····-\n" +
                    "000001b0 a3 bd 70 39 9b b7 70 e9  8c 39 f0 1d 4f          ··p9··p· ·9··O   \n" +
                    "# position: 448, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001c0             10 6e 61 6d  65 5f 2d 31 31 35 35 38     ·nam e_-11558\n" +
                    "000001d0 36 39 33 32 35 6f 0e fb  68 d8 9c b8 19 fc cc 2c 69325o·· h······,\n" +
                    "000001e0 35 92 f9 4d 68 e5 f1 2c  55 f0 b8 46 09          5··Mh··, U··F·   \n" +
                    "# position: 496, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001f0             10 6e 61 6d  65 5f 2d 31 31 35 34 37     ·nam e_-11547\n" +
                    "00000200 31 35 30 37 39 90 45 c5  e6 f7 b9 1a 4b ea c3 2f 15079·E· ····K··/\n" +
                    "00000210 7f 17 5f 10 01 5c 6e 62  fc cc 5e cc da          ··_··\\nb ··^··   \n" +
                    "# position: 544, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data #binary\n" +
                    "...\n" +
                    "# 130524 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    496,\n" +
                    "    2130303778818\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 304, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  400,\n" +
                    "  448,\n" +
                    "  496,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 400, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000190             10 6e 61 6d  65 5f 2d 31 31 35 35 34     ·nam e_-11554\n" +
                    "000001a0 38 34 35 37 36 7a cb 93  3d 38 51 d9 d4 f6 c9 2d 84576z·· =8Q····-\n" +
                    "000001b0 a3 bd 70 39 9b b7 70 e9  8c 39 f0 1d 4f          ··p9··p· ·9··O   \n" +
                    "# position: 448, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001c0             10 6e 61 6d  65 5f 2d 31 31 35 35 38     ·nam e_-11558\n" +
                    "000001d0 36 39 33 32 35 6f 0e fb  68 d8 9c b8 19 fc cc 2c 69325o·· h······,\n" +
                    "000001e0 35 92 f9 4d 68 e5 f1 2c  55 f0 b8 46 09          5··Mh··, U··F·   \n" +
                    "# position: 496, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001f0             10 6e 61 6d  65 5f 2d 31 31 35 34 37     ·nam e_-11547\n" +
                    "00000200 31 35 30 37 39 90 45 c5  e6 f7 b9 1a 4b ea c3 2f 15079·E· ····K··/\n" +
                    "00000210 7f 17 5f 10 01 5c 6e 62  fc cc 5e cc da          ··_··\\nb ··^··   \n" +
                    "# position: 544, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data #binary\n" +
                    "...\n" +
                    "# 130524 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    496,\n" +
                    "    2130303778818\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 8,\n" +
                    "    indexSpacing: 1,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 3\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 8, used: 1\n" +
                    "  304,\n" +
                    "  0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 304, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 8, used: 3\n" +
                    "  400,\n" +
                    "  448,\n" +
                    "  496,\n" +
                    "  0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 400, header: 0\n" +
                    "--- !!data #binary\n" +
                    "00000190             10 6e 61 6d  65 5f 2d 31 31 35 35 34     ·nam e_-11554\n" +
                    "000001a0 38 34 35 37 36 7a cb 93  3d 38 51 d9 d4 f6 c9 2d 84576z·· =8Q····-\n" +
                    "000001b0 a3 bd 70 39 9b b7 70 e9  8c 39 f0 1d 4f          ··p9··p· ·9··O   \n" +
                    "# position: 448, header: 1\n" +
                    "--- !!data #binary\n" +
                    "000001c0             10 6e 61 6d  65 5f 2d 31 31 35 35 38     ·nam e_-11558\n" +
                    "000001d0 36 39 33 32 35 6f 0e fb  68 d8 9c b8 19 fc cc 2c 69325o·· h······,\n" +
                    "000001e0 35 92 f9 4d 68 e5 f1 2c  55 f0 b8 46 09          5··Mh··, U··F·   \n" +
                    "# position: 496, header: 2\n" +
                    "--- !!data #binary\n" +
                    "000001f0             10 6e 61 6d  65 5f 2d 31 31 35 34 37     ·nam e_-11547\n" +
                    "00000200 31 35 30 37 39 90 45 c5  e6 f7 b9 1a 4b ea c3 2f 15079·E· ····K··/\n" +
                    "00000210 7f 17 5f 10 01 5c 6e 62  fc cc 5e cc da          ··_··\\nb ··^··   \n" +
                    "...\n" +
                    "# 130524 bytes remaining\n";

            long numRead = 0;
            final TestBytesMarshallable reusableData = new TestBytesMarshallable(0);
            final ExcerptTailer currentPosTailer = queue.createTailer(named ? "named" : null)
                    .toStart();
            final ExcerptTailer endPosTailer = queue.createTailer(named ? "named2" : null).toEnd();
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
            assertEquals(expected, dump);

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
        public void writeMarshallable(@NotNull BytesOut<?> bytes) {
            bytes.writeUtf8(_name);
            bytes.writeLong(_value1);
            bytes.writeLong(_value2);
            bytes.writeLong(_value3);
        }

        @Override
        public void readMarshallable(@NotNull BytesIn<?> bytes) throws IORuntimeException {
            _name = bytes.readUtf8();
            _value1 = bytes.readLong();
            _value2 = bytes.readLong();
            _value3 = bytes.readLong();
        }
    }
}