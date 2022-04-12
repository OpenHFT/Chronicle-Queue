package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.onoes.LogLevel;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.RollCycles.*;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;

public class InternalAppenderWriteBytesTest extends ChronicleQueueTestBase {

    @Before
    public void before() {
        if (OS.isMacOSX())
            ignoreException(exceptionKey -> exceptionKey.clazz == DirectoryUtils.class, "Ignore DirectoryUtils");
        ignoreException(e -> e.level == LogLevel.PERF, "ignore all PERF");
    }

    @Test
    public void writeJustAfterLastIndex() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world again");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            // write at index 0
            appender.writeBytes(test);
            // append at index 1
            ((InternalAppender) appender).writeBytes(1, test2);

            ExcerptTailer tailer = q.createTailer();

            tailer.readBytes(result);
            assertEquals(test, result);
            result.clear();

            tailer.readBytes(result);
            assertEquals(test2, result);
            result.clear();
        }
    }

    @Test
    public void dontOverwriteExisting() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);

            expectException("Trying to overwrite index 0 which is before the end of the queue");
            // try to overwrite - will not overwrite
            ((InternalAppender) appender).writeBytes(0, Bytes.from("HELLO WORLD"));

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            assertEquals(test, result);
            assertEquals(1, tailer.index());
        }
    }

    @Test
    public void dontOverwriteExistingDifferentQueueInstance() {
        expectException("Trying to overwrite index 0 which is before the end of the queue");
        expectException("Trying to overwrite index 1 which is before the end of the queue");
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
                "    deltaCheckpointInterval: 64,\n" +
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
        try (SingleChronicleQueue q = createQueue(tmpDir)) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);
            appender.writeBytes(test2);
            index = appender.lastIndexAppended();
//            assertEquals(expected, q.dump());
        }
        assertEquals(1, index);

        // has to be the same tmpDir
        try (SingleChronicleQueue q = createQueue(tmpDir)) {
            InternalAppender appender = (InternalAppender) q.acquireAppender();
            appender.writeBytes(0, Bytes.from("HELLO WORLD"));
//            assertEquals(expected, q.dump());

            appender.writeBytes(1, Bytes.from("HELLO WORLD"));
//            assertEquals(expected, q.dump());

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

    @Test(expected = java.lang.IllegalStateException.class)
    public void cantAppendPastTheEnd() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);

            // this will throw because it is not in sequence
            ((InternalAppender) appender).writeBytes(2, test);
        }
    }

    @Test
    public void test3() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);

            ExcerptTailer tailer = q.createTailer();
            expectException("Trying to overwrite index 0 which is before the end of the queue");
            ((InternalAppender) appender).writeBytes(0, test);

            try (DocumentContext documentContext = tailer.readingDocument()) {
                result.write(documentContext.wire().bytes());
            }

            Assert.assertTrue("hello world".contentEquals(result));
            assertEquals(1, tailer.index());
            result.clear();

            ((InternalAppender) appender).writeBytes(1, test);

            try (DocumentContext dc = tailer.readingDocument()) {
                dc.rollbackOnClose();
            }

            assertEquals(1, tailer.index());

            ((InternalAppender) appender).writeBytes(2, test);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testJumpingAMessageThrowsAIllegalStateException() {

        try (SingleChronicleQueue q = binary(tempDir("q"))
                .rollCycle(MINUTELY)
                .timeProvider(() -> 0).build();

             ExcerptAppender appender = q.acquireAppender()) {
            appender.writeText("hello");
            appender.writeText("hello2");
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeLong(1);
            }

            final long l = appender.lastIndexAppended();
            final RollCycle rollCycle = q.rollCycle();
            final int currentCycle = rollCycle.toCycle(l);
            // try to write to next roll cycle and write at seqnum 1 (but miss the 0th seqnum of that roll cycle)
            final long index = rollCycle.toIndex(currentCycle + 1, 1);
            ((InternalAppender) appender).writeBytes(index, Bytes.from("text"));
        }
    }

    @Test
    public void appendToPreviousCycle() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test1 = Bytes.from("hello world again cycle1");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world cycle2");
        Bytes<?> result = Bytes.elasticHeapByteBuffer();
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(timeProvider).rollCycle(TEST_HOURLY).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);
            long nextIndexInFirstCycle = appender.lastIndexAppended() + 1;
            int firstCycle = q.rollCycle().toCycle(nextIndexInFirstCycle);

            timeProvider.advanceMillis(TimeUnit.SECONDS.toMillis(65 * 60));
            appender.writeBytes(test2);
//            System.out.println(q.dump());

            Assert.assertTrue(hasEOF(q, firstCycle));
            // here we try and write to previous cycle file. We will overwrite the EOF in doing so
            ignoreException("Incomplete header found at pos: 33048: c0000000, overwriting");
            ((InternalAppender) appender).writeBytes(nextIndexInFirstCycle, test1);
            Assert.assertFalse(hasEOF(q, firstCycle));

            // we have to manually fix. This is done by CQE at the end of backfilling
            appender.normaliseEOFs();

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            assertEquals(test, result);
            result.clear();
            tailer.readBytes(result);
            assertEquals(test1, result);
            result.clear();
            tailer.readBytes(result);
            assertEquals(test2, result);
        }
    }

    private boolean hasEOF(SingleChronicleQueue q, int cycle) {
        try (SingleChronicleQueueStore store = q.storeForCycle(cycle, 0, false, null)) {
            String dump = store.dump();
            System.out.println(dump);
            return dump.contains(" EOF") && dump.contains("--- !!not-ready-meta-data");
        }
    }
}
