package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.onoes.ExceptionKey;
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
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.RollCycles.MINUTELY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

public class InternalAppenderWriteBytesTest extends ChronicleQueueTestBase {

    @Override
    protected boolean hasExceptions(Map<ExceptionKey, Integer> exceptions) {
        // as we check for DEBUG exceptions in this test, don't call the standard Jvm.hasExceptions
        exceptions.keySet().removeIf(k -> k.level == LogLevel.PERF);
        return !exceptions.isEmpty();
    }

    @Before
    public void expectExceptions() {
        expectException("File released");
        expectException(/* Adding ... surefire....jar */ " to the classpath");
    }

    @Test
    public void writeJustAfterLastIndex() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        @NotNull Bytes<byte[]> test2 = Bytes.from("hello world again");
        Bytes result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            // write at index 0
            appender.writeBytes(test);
            // append at index 1
            ((InternalAppender) appender).writeBytes(1, test2);

            ExcerptTailer tailer = q.createTailer();

            tailer.readBytes(result);
            Assert.assertEquals(test, result);
            result.clear();

            tailer.readBytes(result);
            Assert.assertEquals(test2, result);
            result.clear();
        }
    }

    @Test
    public void dontOverwriteExisting() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        Bytes result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);

            expectException("Trying to overwrite index 0 which is before the end of the queue");
            // try to overwrite - will not overwrite
            ((InternalAppender) appender).writeBytes(0, Bytes.from("HELLO WORLD"));

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            Assert.assertEquals(test, result);
            Assert.assertEquals(1, tailer.index());
        }
    }

    @Ignore("TODO: FIX")
    @Test
    public void dontOverwriteExistingDifferentQueue() {
        @NotNull Bytes<byte[]> test = Bytes.from("hello world");
        Bytes result = Bytes.elasticHeapByteBuffer();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);
        }

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(() -> 0).build()) {
            ExcerptAppender appender = q.acquireAppender();
            // TODO: this overwrites because the appender's wire's headerNumber is positioned at the start
            ((InternalAppender) appender).writeBytes(0, Bytes.from("HELLO WORLD"));

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            Assert.assertEquals(test, result);
            Assert.assertEquals(1, tailer.index());
        }
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
        Bytes result = Bytes.elasticHeapByteBuffer();
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
            Assert.assertEquals(1, tailer.index());
            result.clear();

            ((InternalAppender) appender).writeBytes(1, test);

            try (DocumentContext dc = tailer.readingDocument()) {
                dc.rollbackOnClose();
            }

            Assert.assertEquals(1, tailer.index());

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
        Bytes result = Bytes.elasticHeapByteBuffer();
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).timeProvider(timeProvider).rollCycle(MINUTELY).build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeBytes(test);
            long nextIndexInFirstCycle = appender.lastIndexAppended() + 1;
            int firstCycle = q.rollCycle().toCycle(nextIndexInFirstCycle);

            timeProvider.advanceMillis(TimeUnit.SECONDS.toMillis(65));
            appender.writeBytes(test2);

            Assert.assertTrue(hasEOF(q, firstCycle));
            // here we try and write to previous cycle file. We will overwrite the EOF in doing so
            expectException("Incomplete header found at pos: 33048: c0000000, overwriting");
            ((InternalAppender) appender).writeBytes(nextIndexInFirstCycle, test1);
            Assert.assertFalse(hasEOF(q, firstCycle));

            // we have to manually fix. This is done by CQE at the end of backfilling
            appender.normaliseEOFs();

            ExcerptTailer tailer = q.createTailer();
            tailer.readBytes(result);
            Assert.assertEquals(test, result);
            result.clear();
            tailer.readBytes(result);
            Assert.assertEquals(test1, result);
            result.clear();
            tailer.readBytes(result);
            Assert.assertEquals(test2, result);
        }
    }

    private boolean hasEOF(SingleChronicleQueue q, int cycle) {
        try (SingleChronicleQueueStore store = q.storeForCycle(cycle, 0, false, null)) {
            String dump = store.dump();
            return dump.contains(" EOF") && dump.contains("--- !!not-ready-meta-data! #binary");
        }
    }
}
