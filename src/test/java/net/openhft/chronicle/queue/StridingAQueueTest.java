/*
 * Copyright (c) 2016-2020 chronicle.software
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;

public class StridingAQueueTest extends ChronicleQueueTestBase {
    @Before
    public void disableFileShrinking() {
        System.setProperty("chronicle.queue.disableFileShrinking", "true");
    }

    @Test
    public void testStriding() {
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.currentTimeMillis(1_567_498_753_000L);
        File tmpDir = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(RollCycles.TEST4_SECONDLY)
                .build()) {
            SAQMessage writer = queue.acquireAppender().methodWriter(SAQMessage.class);
            for (int j = 1; j <= 4; j++) {
                for (int i = 0; i < 6 + j; i++)
                    writer.hi(j, i);
                timeProvider.advanceMillis(j * 500);
           // System.out.println(timeProvider.currentTimeMillis());
            }
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(RollCycles.TEST4_SECONDLY)
                .build()) {

            assertEquals(getExpected(), queue.dump().replaceAll("(?m)^#.+$\\n", ""));
            StringWriter sw = new StringWriter();
            ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD).toEnd().striding(true);
            MethodReader reader = tailer.methodReader(Mocker.logging(SAQMessage.class, "", sw));
            while (reader.readOne()) ;
            assertEquals("hi[4, 9]\n" +
                            "hi[4, 8]\n" +
                            "hi[4, 4]\n" +
                            "hi[4, 0]\n" +
                            "hi[3, 8]\n" +
                            "hi[3, 4]\n" +
                            "hi[3, 0]\n" +
                            "hi[2, 7]\n" +
                            "hi[2, 5]\n" +
                            "hi[2, 1]\n" +
                            "hi[1, 4]\n" +
                            "hi[1, 0]\n",
                    sw.toString().replace("\r", ""));
        }
    }

    @NotNull
    private String getExpected() {
        return "" +
                "--- !!meta-data #binary\n" +
                "header: !STStore {\n" +
                "  wireType: !WireType BINARY_LIGHT,\n" +
                "  metadata: !SCQMeta {\n" +
                "    roll: !SCQSRoll { length: !short 1000, format: yyyyMMdd-HHmmss'T4', epoch: 0 },\n" +
                "    deltaCheckpointInterval: 64,\n" +
                "    sourceId: 0\n" +
                "  }\n" +
                "}\n" +
                "--- !!data #binary\n" +
                "listing.highestCycle: 1567498756\n" +
                "--- !!data #binary\n" +
                "listing.lowestCycle: 1567498753\n" +
                "--- !!data #binary\n" +
                "listing.modCount: 7\n" +
                "--- !!data #binary\n" +
                "chronicle.write.lock: -9223372036854775808\n" +
                "--- !!data #binary\n" +
                "chronicle.append.lock: -9223372036854775808\n" +
                "--- !!data #binary\n" +
                "chronicle.lastIndexReplicated: -1\n" +
                "--- !!data #binary\n" +
                "chronicle.lastAcknowledgedIndexReplicated: -1\n" +
                "...\n" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    1064,\n" +
                "    4569845202958\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 200,\n" +
                "    lastIndex: 16\n" +
                "  },\n" +
                "  dataFormat: 1\n" +
                "}\n" +
                "--- !!meta-data #binary\n" +
                "index2index: [\n" +
                "  # length: 32, used: 1\n" +
                "  496,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 32, used: 4\n" +
                "  784,\n" +
                "  864,\n" +
                "  944,\n" +
                "  1024,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  1,\n" +
                "  0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  1,\n" +
                "  1\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  1,\n" +
                "  2\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  1,\n" +
                "  3\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  1,\n" +
                "  4\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  1,\n" +
                "  5\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  1,\n" +
                "  6\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  1\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  2\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  3\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  4\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  5\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  6\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  2,\n" +
                "  7\n" +
                "]\n" +
                "--- !!not-ready-meta-data #binary\n" +
                "...\n" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    944,\n" +
                "    4054449127432\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 200,\n" +
                "    lastIndex: 12\n" +
                "  },\n" +
                "  dataFormat: 1\n" +
                "}\n" +
                "--- !!meta-data #binary\n" +
                "index2index: [\n" +
                "  # length: 32, used: 1\n" +
                "  496,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 32, used: 3\n" +
                "  784,\n" +
                "  864,\n" +
                "  944,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  1\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  2\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  3\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  4\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  5\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  6\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  7\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  3,\n" +
                "  8\n" +
                "]\n" +
                "--- !!not-ready-meta-data #binary\n" +
                "...\n" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    964,\n" +
                "    4140348473353\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 200,\n" +
                "    lastIndex: 12\n" +
                "  },\n" +
                "  dataFormat: 1\n" +
                "}\n" +
                "--- !!meta-data #binary\n" +
                "index2index: [\n" +
                "  # length: 32, used: 1\n" +
                "  496,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!meta-data #binary\n" +
                "index: [\n" +
                "  # length: 32, used: 3\n" +
                "  784,\n" +
                "  864,\n" +
                "  944,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  0\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  1\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  2\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  3\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  4\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  5\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  6\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  7\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  8\n" +
                "]\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  4,\n" +
                "  9\n" +
                "]\n" +
                "...\n";
    }

    interface SAQMessage {
        void hi(int j, int i);
    }
}
