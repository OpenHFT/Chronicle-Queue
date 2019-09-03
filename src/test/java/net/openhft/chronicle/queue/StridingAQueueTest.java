/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.QueueFileShrinkManager;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;

public class StridingAQueueTest extends ChronicleQueueTestBase {
    interface SAQMessage {
        void hi(int j, int i);
    }

    @Before
    public void disableFileShrinking() {
        QueueFileShrinkManager.DISABLE_QUEUE_FILE_SHRINKING = true;
    }

    @After
    public void enableFileShrinking() {
        QueueFileShrinkManager.DISABLE_QUEUE_FILE_SHRINKING = false;
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

            assertEquals("--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    1093,\n" +
                    "    4694399254542\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 184,\n" +
                    "    lastIndex: 16\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 184, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 4\n" +
                    "  768,\n" +
                    "  860,\n" +
                    "  952,\n" +
                    "  1047,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 768, header: 0\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 1,\n" +
                    "  !int 0\n" +
                    "]\n" +
                    "# position: 791, header: 1\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 1,\n" +
                    "  !int 1\n" +
                    "]\n" +
                    "# position: 814, header: 2\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 1,\n" +
                    "  !int 2\n" +
                    "]\n" +
                    "# position: 837, header: 3\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 1,\n" +
                    "  !int 3\n" +
                    "]\n" +
                    "# position: 860, header: 4\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 1,\n" +
                    "  !int 4\n" +
                    "]\n" +
                    "# position: 883, header: 5\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 1,\n" +
                    "  !int 5\n" +
                    "]\n" +
                    "# position: 906, header: 6\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 1,\n" +
                    "  !int 6\n" +
                    "]\n" +
                    "# position: 929, header: 7\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 0\n" +
                    "]\n" +
                    "# position: 952, header: 8\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 1\n" +
                    "]\n" +
                    "# position: 975, header: 9\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 2\n" +
                    "]\n" +
                    "# position: 998, header: 10\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 3\n" +
                    "]\n" +
                    "# position: 1024, header: 11\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 4\n" +
                    "]\n" +
                    "# position: 1047, header: 12\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 5\n" +
                    "]\n" +
                    "# position: 1070, header: 13\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 6\n" +
                    "]\n" +
                    "# position: 1093, header: 14\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 2,\n" +
                    "  !int 7\n" +
                    "]\n" +
                    "# position: 1116, header: 14 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 129952 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    952,\n" +
                    "    4088808865800\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 184,\n" +
                    "    lastIndex: 12\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 184, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 3\n" +
                    "  768,\n" +
                    "  860,\n" +
                    "  952,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 768, header: 0\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 0\n" +
                    "]\n" +
                    "# position: 791, header: 1\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 1\n" +
                    "]\n" +
                    "# position: 814, header: 2\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 2\n" +
                    "]\n" +
                    "# position: 837, header: 3\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 3\n" +
                    "]\n" +
                    "# position: 860, header: 4\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 4\n" +
                    "]\n" +
                    "# position: 883, header: 5\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 5\n" +
                    "]\n" +
                    "# position: 906, header: 6\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 6\n" +
                    "]\n" +
                    "# position: 929, header: 7\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 7\n" +
                    "]\n" +
                    "# position: 952, header: 8\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 3,\n" +
                    "  !int 8\n" +
                    "]\n" +
                    "# position: 975, header: 8 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 130093 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  writePosition: [\n" +
                    "    975,\n" +
                    "    4187593113609\n" +
                    "  ],\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 32,\n" +
                    "    indexSpacing: 4,\n" +
                    "    index2Index: 184,\n" +
                    "    lastIndex: 12\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 184, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 32, used: 1\n" +
                    "  480,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 480, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 32, used: 3\n" +
                    "  768,\n" +
                    "  860,\n" +
                    "  952,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 768, header: 0\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 0\n" +
                    "]\n" +
                    "# position: 791, header: 1\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 1\n" +
                    "]\n" +
                    "# position: 814, header: 2\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 2\n" +
                    "]\n" +
                    "# position: 837, header: 3\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 3\n" +
                    "]\n" +
                    "# position: 860, header: 4\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 4\n" +
                    "]\n" +
                    "# position: 883, header: 5\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 5\n" +
                    "]\n" +
                    "# position: 906, header: 6\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 6\n" +
                    "]\n" +
                    "# position: 929, header: 7\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 7\n" +
                    "]\n" +
                    "# position: 952, header: 8\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 8\n" +
                    "]\n" +
                    "# position: 975, header: 9\n" +
                    "--- !!data #binary\n" +
                    "hi: [\n" +
                    "  !int 4,\n" +
                    "  !int 9\n" +
                    "]\n" +
                    "...\n" +
                    "# 130070 bytes remaining\n", queue.dump());
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
}
