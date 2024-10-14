/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.*;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteAfterEOFException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_HOURLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class InternalAppenderWriteBytesTest extends QueueTestCommon {

    @Before
    public void before() {
        if (OS.isMacOSX())
            ignoreException(exceptionKey -> exceptionKey.clazz == DirectoryUtils.class, "Ignore DirectoryUtils");
        ignoreException(e -> e.level == LogLevel.PERF, "ignore all PERF");
    }

    @Test
    public void canWriteAtEndOfLastExistingRollCycle() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world again");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        RollCycle rollCycle = RollCycles.DEFAULT;
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(() -> 0)
                .rollCycle(rollCycle)
                .build();
             ExcerptAppender appender = q.createAppender();
             ExcerptTailer tailer = q.createTailer()) {

            // write at cycle 0, sequence 0
            appender.writeBytes(test);
            // append at cycle 0, sequence 1
            ((InternalAppender) appender).writeBytes(rollCycle.toIndex(0, 1), test2);

            tailer.readBytes(result);
            assertEquals(test, result);
            result.clear();

            tailer.readBytes(result);
            assertEquals(test2, result);
            result.clear();
        }
    }

    @Test
    public void canWriteAtBeginningOfNextRollCycle() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world again");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        RollCycle rollCycle = RollCycles.DEFAULT;
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir())
                .timeProvider(() -> 0)
                .rollCycle(rollCycle)
                .build();
             ExcerptAppender appender = q.createAppender();
             ExcerptTailer tailer = q.createTailer()) {

            // write at cycle 0, sequence 0
            appender.writeBytes(test);
            // append at cycle 1, sequence 0
            ((InternalAppender) appender).writeBytes(rollCycle.toIndex(1, 0), test2);

            tailer.readBytes(result);
            assertEquals(test, result);
            result.clear();

            tailer.readBytes(result);
            assertEquals(test2, result);
            result.clear();
        }
    }

    @Test
    public void cannotOverwriteExistingEntries() {
        @NotNull Bytes<byte[]> originalBytes = Bytes.from("hello world");
        final Bytes<byte[]> overwriteBytes = Bytes.from("HELLO WORLD");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(originalBytes);

            // try to overwrite - will not overwrite
            ((InternalAppender) appender).writeBytes(0, overwriteBytes);

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            assertEquals(originalBytes, result);
            assertEquals(1, tailer.index());
        }
    }

    @Test
    public void cannotOverwriteExistingEntries_DifferentQueueInstance() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world2");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        long index;
        final File tmpDir = getTmpDir();
        final String expected = "" +
                "--- !!meta-data #binary\n" +
                "header: !STStore {\n" +
                "  wireType: !WireType BINARY_LIGHT,\n" +
                "  metadata: !SCQMeta {\n" +
                "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T4', epoch: 0 },\n" +
                "    sourceId: 0\n" +
                "  }\n" +
                "}\n" +
                "# position: 176, header: 0\n" +
                "--- !!data #binary\n" +
                "listing.highestCycle: 0\n" +
                "# position: 216, header: 1\n" +
                "--- !!data #binary\n" +
                "listing.lowestCycle: 0\n" +
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
                "    792,\n" +
                "    3401614098433\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 196,\n" +
                "    lastIndex: 4\n" +
                "  },\n" +
                "  dataFormat: 1\n" +
                "}\n" +
                "# position: 196, header: -1\n" +
                "--- !!meta-data #binary\n" +
                "index2index: [\n" +
                "  # length: 32, used: 1\n" +
                "  488,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "# position: 488, header: -1\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 32, used: 1\n" +
                "  776,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "# position: 776, header: 0\n" +
                "--- !!data\n" +
                "hello world\n" +
                "# position: 792, header: 1\n" +
                "--- !!data\n" +
                "hello world2\n" +
                "...\n" +
                "# 130260 bytes remaining\n";
        try (SingleChronicleQueue q = createQueue(tmpDir);
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(test);
            appender.writeBytes(test2);
            index = appender.lastIndexAppended();
        }
        assertEquals(1, index);

        // has to be the same tmpDir
        try (SingleChronicleQueue q = createQueue(tmpDir);
             InternalAppender appender = (InternalAppender) q.createAppender()) {
            appender.writeBytes(0, Bytes.from("HELLO WORLD"));
            appender.writeBytes(1, Bytes.from("HELLO WORLD"));

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            assertEquals(test, result);
            assertEquals(1, tailer.index());
        }
    }

    @NotNull
    private SingleChronicleQueue createQueue(File tmpDir) {
        return SingleChronicleQueueBuilder.binary(tmpDir).timeProvider(() -> 0).testBlockSize().rollCycle(TEST4_DAILY).build();
    }

    @Test(expected = IllegalIndexException.class)
    public void cannotAppendToExistingCycleIfNotNextIndex() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build();
             ExcerptAppender appender = q.createAppender()) {
            // append to cycle 0, sequence 0
            appender.writeBytes(test);

            // this will throw because it is not in sequence (cycle 0, sequence 2)
            ((InternalAppender) appender).writeBytes(2, test);
        }
    }

    @Test(expected = IllegalIndexException.class)
    public void cannotWriteToNonZeroIndexOfNewRollCycle() {
        final RollCycle rollCycle = RollCycles.DEFAULT;
        try (SingleChronicleQueue q = binary(tempDir("q"))
                .rollCycle(rollCycle)
                .timeProvider(() -> 0).build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeText("hello");    // cycle 0, sequence 0

            // attempt to write to cycle 1, sequence 1
            ((InternalAppender) appender).writeBytes(rollCycle.toIndex(1, 1), Bytes.from("text"));
        }
    }

    @Test
    public void cannotAppendToPreviousCycle() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test1 = Bytes.from("hello world again cycle1");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world cycle2");
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(timeProvider).rollCycle(TEST_HOURLY).build();
             ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(test);
            long nextIndexInFirstCycle = appender.lastIndexAppended() + 1;
            int firstCycle = q.rollCycle().toCycle(nextIndexInFirstCycle);

            timeProvider.advanceMillis(TimeUnit.SECONDS.toMillis(65 * 60));
            appender.writeBytes(test2);

            Assert.assertTrue(hasEOF(q, firstCycle));
            // here we try and write to previous cycle file
            assertThrows(WriteAfterEOFException.class, () -> ((InternalAppender) appender).writeBytes(nextIndexInFirstCycle, test1));
        }
    }

    private boolean hasEOF(SingleChronicleQueue q, int cycle) {
        try (SingleChronicleQueueStore store = q.storeForCycle(cycle, 0, false, null)) {
            String dump = store.dump(WireType.BINARY_LIGHT);
            System.out.println(dump);
            return dump.contains(" EOF") && dump.contains("--- !!not-ready-meta-data");
        }
    }
}
