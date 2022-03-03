package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static net.openhft.chronicle.queue.RollCycles.TEST2_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RollingChronicleQueueTest extends ChronicleQueueTestBase {

    @Test
    public void testCountExcerptsWhenTheCycleIsRolled() {

        final AtomicLong time = new AtomicLong();

        File name = getTmpDir();
        try (final RollingChronicleQueue q = binary(name)
                .testBlockSize()
                .timeProvider(time::get)
                .rollCycle(TEST2_DAILY)
                .build()) {

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
            final String expected = "" +
                    "--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT,\n" +
                    "  metadata: !SCQMeta {\n" +
                    "    roll: !SCQSRoll { length: 86400000, format: yyyyMMdd'T2', epoch: 0 },\n" +
                    "    deltaCheckpointInterval: 64,\n" +
                    "    sourceId: 0\n" +
                    "  }\n" +
                    "}\n" +
                    "# position: 180, header: 0\n" +
                    "--- !!data #binary\n" +
                    "listing.highestCycle: 4\n" +
                    "# position: 216, header: 1\n" +
                    "--- !!data #binary\n" +
                    "listing.lowestCycle: 0\n" +
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
                    "...\n" +
                    "# 130596 bytes remaining\n" +
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
                    "# 130472 bytes remaining\n" +
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
                    "# 130508 bytes remaining\n" +
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
                    "# 130488 bytes remaining\n";
            assertEquals(5, q.countExcerpts(start, end));

            Thread.yield();
            assertEquals(expected,
                    q.dump());
        }
    }

    @Test
    public void testTailingWithEmptyCycles() {
        testTailing(p -> {
            try {
                p.execute();
            } catch (InvalidEventHandlerException e) {
                e.printStackTrace();
            }
            return 1;
        });
    }

    @Test
    public void testTailingWithMissingCycles() {
        testTailing(p -> 0);
    }

    private void testTailing(Function<Pretoucher, Integer> createGap) {
        final SetTimeProvider tp = new SetTimeProvider(0);
        final File tmpDir = getTmpDir();
        try (SingleChronicleQueue queue = builder(tmpDir, WireType.BINARY).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(tp).build();
             Pretoucher pretoucher = new Pretoucher(queue, null, c -> {
             }, true, true)) {
            int cyclesAdded = 0;
            ExcerptAppender appender = queue.acquireAppender();

            appender.writeText("0"); // to file ...000000
            assertEquals(1, listCQ4Files(tmpDir).length);

            tp.advanceMillis(1000);
            appender.writeText("1"); // to file ...000001
            final @Nullable File[] files = listCQ4Files(tmpDir);
            assertEquals(2, files.length);

            tp.advanceMillis(2000);
            cyclesAdded += createGap.apply(pretoucher);
            BackgroundResourceReleaser.releasePendingResources();
            final @Nullable File[] files2 = listCQ4Files(tmpDir);
            assertEquals(2 + cyclesAdded, files2.length);

            tp.advanceMillis(1000);
            appender.writeText("2"); // to file ...000004
            assertEquals(3 + cyclesAdded, listCQ4Files(tmpDir).length);

            tp.advanceMillis(2000);
            cyclesAdded += createGap.apply(pretoucher);
            assertEquals(3 + cyclesAdded, listCQ4Files(tmpDir).length);

            // now tail them all back
            int count = 0;
            ExcerptTailer tailer = queue.createTailer();
            long[] indexes = new long[3];
            while (true) {
                String text = tailer.readText();
                if (text == null)
                    break;
                indexes[count] = tailer.index() - 1;
                assertEquals(count++, Integer.parseInt(text));
            }
            assertEquals(indexes.length, count);

            // now make sure we can go direct to each index (like afterLastWritten)
            tailer.toStart();
            for (int i = 0; i < indexes.length; i++) {
                assertTrue(tailer.moveToIndex(indexes[i]));
                String text = tailer.readText();
                assertEquals(i, Integer.parseInt(text));
            }
        }
    }

    @Nullable
    private File[] listCQ4Files(File tmpDir) {
        return tmpDir.listFiles(file -> file.getName().endsWith("cq4"));
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(RollCycles.TEST4_DAILY).testBlockSize();
    }
}