package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.Utils;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.RollCycles.TEST2_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class RollingChronicleQueueTest {

    @Test
    public void testCountExcerptsWhenTheCycleIsRolled() throws Exception {

        final AtomicLong time = new AtomicLong();

        File name = Utils.tempDir("testCountExcerptsWhenTheCycleIsRolled");
        final SingleChronicleQueue q = binary(name)
                .timeProvider(time::get)
                .rollCycle(TEST2_DAILY)
                .build();

        final ExcerptAppender appender = q.acquireAppender();
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
        assertEquals("--- !!meta-data #binary\n" +
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
                "# 83885313 bytes remaining\n" +
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
                "# 83885342 bytes remaining\n" +
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
                "# 83885323 bytes remaining\n", q.dump());

        assertEquals(5, q.countExcerpts(start, end));
    }

}