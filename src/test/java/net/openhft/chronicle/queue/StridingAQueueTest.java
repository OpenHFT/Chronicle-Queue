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
//            System.out.println(timeProvider.currentTimeMillis());
            }
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(RollCycles.TEST4_SECONDLY)
                .build()) {

            assertEquals(getExpected(), queue.dump());
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
        return "--- !!meta-data #binary\n" +
                "header: !STStore {\n" +
                "  wireType: !WireType BINARY_LIGHT,\n" +
                "  metadata: !SCQMeta {\n" +
                "    roll: !SCQSRoll { length: !short 1000, format: yyyyMMdd-HHmmss'T4', epoch: 0 },\n" +
                "    deltaCheckpointInterval: 64,\n" +
                "    sourceId: 0\n" +
                "  }\n" +
                "}\n" +
                "# position: 180, header: 0\n" +
                "--- !!data #binary\n" +
                "listing.highestCycle: 1567498756\n" +
                "# position: 216, header: 1\n" +
                "--- !!data #binary\n" +
                "listing.lowestCycle: 1567498753\n" +
                "# position: 256, header: 2\n" +
                "--- !!data #binary\n" +
                "listing.modCount: 3\n" +
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
                "# 65060 bytes remaining\n" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    1112,\n" +
                "    4776003633166\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 196,\n" +
                "    lastIndex: 16\n" +
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
                "  # length: 32, used: 4\n" +
                "  776,\n" +
                "  872,\n" +
                "  968,\n" +
                "  1064,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "# position: 776, header: 0\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 1,\n" +
                "  !int 0\n" +
                "]\n" +
                "# position: 800, header: 1\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 1,\n" +
                "  !int 1\n" +
                "]\n" +
                "# position: 824, header: 2\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 1,\n" +
                "  !int 2\n" +
                "]\n" +
                "# position: 848, header: 3\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 1,\n" +
                "  !int 3\n" +
                "]\n" +
                "# position: 872, header: 4\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 1,\n" +
                "  !int 4\n" +
                "]\n" +
                "# position: 896, header: 5\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 1,\n" +
                "  !int 5\n" +
                "]\n" +
                "# position: 920, header: 6\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 1,\n" +
                "  !int 6\n" +
                "]\n" +
                "# position: 944, header: 7\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 0\n" +
                "]\n" +
                "# position: 968, header: 8\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 1\n" +
                "]\n" +
                "# position: 992, header: 9\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 2\n" +
                "]\n" +
                "# position: 1016, header: 10\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 3\n" +
                "]\n" +
                "# position: 1040, header: 11\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 4\n" +
                "]\n" +
                "# position: 1064, header: 12\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 5\n" +
                "]\n" +
                "# position: 1088, header: 13\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 6\n" +
                "]\n" +
                "# position: 1112, header: 14\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 2,\n" +
                "  !int 7\n" +
                "]\n" +
                "# position: 1136, header: 14 EOF\n" +
                "--- !!not-ready-meta-data! #binary\n" +
                "...\n" +
                "# 129932 bytes remaining\n" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    968,\n" +
                "    4157528342536\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 196,\n" +
                "    lastIndex: 12\n" +
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
                "  # length: 32, used: 3\n" +
                "  776,\n" +
                "  872,\n" +
                "  968,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "# position: 776, header: 0\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 0\n" +
                "]\n" +
                "# position: 800, header: 1\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 1\n" +
                "]\n" +
                "# position: 824, header: 2\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 2\n" +
                "]\n" +
                "# position: 848, header: 3\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 3\n" +
                "]\n" +
                "# position: 872, header: 4\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 4\n" +
                "]\n" +
                "# position: 896, header: 5\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 5\n" +
                "]\n" +
                "# position: 920, header: 6\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 6\n" +
                "]\n" +
                "# position: 944, header: 7\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 7\n" +
                "]\n" +
                "# position: 968, header: 8\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 3,\n" +
                "  !int 8\n" +
                "]\n" +
                "# position: 992, header: 8 EOF\n" +
                "--- !!not-ready-meta-data! #binary\n" +
                "...\n" +
                "# 130076 bytes remaining\n" +
                "--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  writePosition: [\n" +
                "    992,\n" +
                "    4260607557641\n" +
                "  ],\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: 32,\n" +
                "    indexSpacing: 4,\n" +
                "    index2Index: 196,\n" +
                "    lastIndex: 12\n" +
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
                "  # length: 32, used: 3\n" +
                "  776,\n" +
                "  872,\n" +
                "  968,\n" +
                "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                "]\n" +
                "# position: 776, header: 0\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 0\n" +
                "]\n" +
                "# position: 800, header: 1\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 1\n" +
                "]\n" +
                "# position: 824, header: 2\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 2\n" +
                "]\n" +
                "# position: 848, header: 3\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 3\n" +
                "]\n" +
                "# position: 872, header: 4\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 4\n" +
                "]\n" +
                "# position: 896, header: 5\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 5\n" +
                "]\n" +
                "# position: 920, header: 6\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 6\n" +
                "]\n" +
                "# position: 944, header: 7\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 7\n" +
                "]\n" +
                "# position: 968, header: 8\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 8\n" +
                "]\n" +
                "# position: 992, header: 9\n" +
                "--- !!data #binary\n" +
                "hi: [\n" +
                "  !int 4,\n" +
                "  !int 9\n" +
                "]\n" +
                "...\n" +
                "# 130052 bytes remaining\n";
    }

    interface SAQMessage {
        void hi(int j, int i);
    }
}
