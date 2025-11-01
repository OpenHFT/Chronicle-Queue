/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST2_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static org.junit.Assert.assertEquals;

public class RollingChronicleQueueTest extends QueueTestCommon {

    @Test
    public void testCountExcerptsWhenTheCycleIsRolled() {

        final AtomicLong time = new AtomicLong();

        File name = getTmpDir();
        try (final RollingChronicleQueue q = binary(name)
                .testBlockSize()
                .timeProvider(time::get)
                .rollCycle(TEST2_DAILY)
                .build();
             final ExcerptAppender appender = q.createAppender()) {

            time.set(0);

            appender.writeText("1. some  text");
            long start = appender.lastIndexAppended();
            appender.writeText("2. some more text");
            appender.writeText("3. some more text");
            time.set(TimeUnit.DAYS.toMillis(1));
            appender.writeText("4. some text - first cycle");
            time.set(TimeUnit.DAYS.toMillis(2));
            time.set(TimeUnit.DAYS.toMillis(3)); // large gap to miss a cycle file
            time.set(TimeUnit.DAYS.toMillis(4));
            appender.writeText("5. some text - second cycle");
            appender.writeText("some more text");
            long end = appender.lastIndexAppended();
            final String expected = "" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T2', epoch: 0 },\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 152, header: 0\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 4\n" +
                    "# position: 192, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 0\n" +
                    "# position: 232, header: 2\n" +
                    "--- !!data #binary\n" +
                    "listing.modCount: 10\n" +
                    "# position: 264, header: 3\n" +
                    "--- !!data #binary\n" +
                    "chronicle.write.lock: -9223372036854775808\n" +
                    "# position: 304, header: 4\n" +
                    "--- !!data #binary\n" +
                    "chronicle.append.lock: -9223372036854775808\n" +
                    "# position: 344, header: 5\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexReplicated: -1\n" +
                    "# position: 392, header: 6\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                    "# position: 448, header: 7\n" +
                    "--- !!data #binary\n" +
                    "chronicle.lastIndexMSynced: -1\n" +
                    "...\n" +
                    "# X bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    572,\n" +
                    "    2456721293314\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 4\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 368, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 2\n" +
                    "  528,\n" +
                    "  572,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!data #binary\n" +
                    "\"1. some  text\"\n" +
                    "# position: 548, header: 1\n" +
                    "--- !!data #binary\n" +
                    "\"2. some more text\"\n" +
                    "# position: 572, header: 2\n" +
                    "--- !!data #binary\n" +
                    "\"3. some more text\"\n" +
                    "# position: 596, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data #binary\n" +
                    "...\n" +
                    "# X bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    528,\n" +
                    "    2267742732288\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 368, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!data #binary\n" +
                    "\"4. some text - first cycle\"\n" +
                    "# position: 560, header: 0 EOF\n" +
                    "--- !!not-ready-meta-data #binary\n" +
                    "...\n" +
                    "# X bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    560,\n" +
                    "    2405181685761\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 200,\n" +
                    "    lastIndex: 2\n" +
                    "  },\n" +
                    "  dataFormat: 1\n" +
                    "}\n" +
                    "# position: 200, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  368,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 368, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  528,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 528, header: 0\n" +
                    "--- !!data #binary\n" +
                    "\"5. some text - second cycle\"\n" +
                    "# position: 560, header: 1\n" +
                    "--- !!data #binary\n" +
                    "some more text\n" +
                    "...\n" +
                    "# X bytes remaining\n";
            assertEquals(5, q.countExcerpts(start, end));

            Thread.yield();
            // on a roll, the file might be truncated making the size remaining just a few bytes
            assertEquals(expected
                            .replaceAll(" \\d+ (bytes remaining)", " X $1"),
                    q.dump()
                            .replaceAll(" \\d+ (bytes remaining)", " X $1"));
        }
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(TEST4_DAILY).testBlockSize();
    }
}
