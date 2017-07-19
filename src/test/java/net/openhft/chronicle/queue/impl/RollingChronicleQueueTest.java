package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.RollCycles.TEST2_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(Parameterized.class)
public class RollingChronicleQueueTest {
    private final boolean lazyIndexing;

    public RollingChronicleQueueTest(boolean lazyIndexing) {
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
    public void testCountExcerptsWhenTheCycleIsRolled() throws Exception {

        final AtomicLong time = new AtomicLong();

        File name = DirectoryUtils.tempDir("testCountExcerptsWhenTheCycleIsRolled");
        try (final SingleChronicleQueue q = binary(name)
                .testBlockSize()
                .timeProvider(time::get)
                .rollCycle(TEST2_DAILY)
                .build()) {

            final ExcerptAppender appender = q.acquireAppender().lazyIndexing(lazyIndexing);
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
            String expectedEager = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 742,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 4\n" +
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
                    "  # length: 16, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 2\n" +
                    "  704,\n" +
                    "  742,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 704, header: 0\n" +
                    "--- !!data\n" +
                    "1. some  text\n" +
                    "# position: 721, header: 1\n" +
                    "--- !!data\n" +
                    "2. some more text\n" +
                    "# position: 742, header: 2\n" +
                    "--- !!data\n" +
                    "3. some more text\n" +
                    "# position: 763, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 326913 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 704,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 2\n" +
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
                    "  # length: 16, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  704,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 704, header: 0\n" +
                    "--- !!data\n" +
                    "4. some text - first cycle\n" +
                    "# position: 734, header: 0 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 326942 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 735,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 377,\n" +
                    "    lastIndex: 2\n" +
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
                    "  # length: 16, used: 1\n" +
                    "  544,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 544, header: -1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  704,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 704, header: 0\n" +
                    "--- !!data\n" +
                    "5. some text - second cycle\n" +
                    "# position: 735, header: 1\n" +
                    "--- !!data\n" +
                    "some more text\n" +
                    "...\n" +
                    "# 326923 bytes remaining\n";
            String expectedLazy = "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 741,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 394,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: 0\n" +
                    "--- !!data\n" +
                    "1. some  text\n" +
                    "# position: 394, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  560,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 560, header: 0\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 720, header: 1\n" +
                    "--- !!data\n" +
                    "2. some more text\n" +
                    "# position: 741, header: 2\n" +
                    "--- !!data\n" +
                    "3. some more text\n" +
                    "# position: 762, header: 2 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 326914 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 377,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
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
                    "--- !!data\n" +
                    "4. some text - first cycle\n" +
                    "# position: 407, header: 0 EOF\n" +
                    "--- !!not-ready-meta-data! #binary\n" +
                    "...\n" +
                    "# 327269 bytes remaining\n" +
                    "--- !!meta-data #binary\n" +
                    "header: !SCQStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  writePosition: 408,\n" +
                    "  roll: !SCQSRoll {\n" +
                    "    length: !int 86400000,\n" +
                    "    format: yyyyMMdd,\n" +
                    "    epoch: 0\n" +
                    "  },\n" +
                    "  indexing: !SCQSIndexing {\n" +
                    "    indexCount: 16,\n" +
                    "    indexSpacing: 2,\n" +
                    "    index2Index: 426,\n" +
                    "    lastIndex: 0\n" +
                    "  },\n" +
                    "  lastAcknowledgedIndexReplicated: -1,\n" +
                    "  recovery: !TimedStoreRecovery {\n" +
                    "    timeStamp: 0\n" +
                    "  },\n" +
                    "  deltaCheckpointInterval: 0\n" +
                    "}\n" +
                    "# position: 377, header: 0\n" +
                    "--- !!data\n" +
                    "5. some text - second cycle\n" +
                    "# position: 408, header: 1\n" +
                    "--- !!data\n" +
                    "some more text\n" +
                    "# position: 426, header: 1\n" +
                    "--- !!meta-data #binary\n" +
                    "index2index: [\n" +
                    "  # length: 16, used: 1\n" +
                    "  592,\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "# position: 592, header: 1\n" +
                    "--- !!meta-data #binary\n" +
                    "index: [\n" +
                    "  # length: 16, used: 0\n" +
                    "  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0\n" +
                    "]\n" +
                    "...\n" +
                    "# 326924 bytes remaining\n";
            assertEquals(lazyIndexing ? expectedLazy : expectedEager, q.dump());

            assertEquals(5, q.countExcerpts(start, end));
        }
    }

    @After
    public void checkRegisteredBytes() {
        BytesUtil.checkRegisteredBytes();
    }
}