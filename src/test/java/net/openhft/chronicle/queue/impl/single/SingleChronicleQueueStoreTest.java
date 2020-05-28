package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.util.ThrowingConsumer;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleChronicleQueueStoreTest {
    private static final int INDEX_SPACING = 4;
    private static final int RECORD_COUNT = INDEX_SPACING * 10;
    private static final RollCycles ROLL_CYCLE = RollCycles.DAILY;
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    private static void assertExcerptsAreIndexed(final RollingChronicleQueue queue, final long[] indices,
                                                 final Function<Integer, Boolean> shouldBeIndexed, final ScanResult expectedScanResult) {
        final SingleChronicleQueueStore wireStore = (SingleChronicleQueueStore)
                queue.storeForCycle(queue.cycle(), 0L, true);
        final SCQIndexing indexing = wireStore.indexing;
        for (int i = 0; i < RECORD_COUNT; i++) {
            final int startLinearScanCount = indexing.linearScanCount;
            final ScanResult scanResult = indexing.moveToIndex((SingleChronicleQueueExcerpts.StoreTailer) queue.createTailer(), indices[i]);
            assertEquals(expectedScanResult, scanResult);

            if (shouldBeIndexed.apply(i)) {
                assertEquals(startLinearScanCount, indexing.linearScanCount);
            } else {
                assertEquals(startLinearScanCount + 1, indexing.linearScanCount);
            }
        }
    }

    private static long[] writeMessagesStoreIndices(final ExcerptAppender appender, final ExcerptTailer tailer) {
        final long[] indices = new long[RECORD_COUNT];
        for (int i = 0; i < RECORD_COUNT; i++) {
            try (final DocumentContext ctx = appender.writingDocument()) {
                ctx.wire().getValueOut().int32(i);
            }
        }

        for (int i = 0; i < RECORD_COUNT; i++) {
            try (final DocumentContext ctx = tailer.readingDocument()) {
                assertTrue("Expected record at index " + i, ctx.isPresent());
                indices[i] = tailer.index();
            }
        }
        return indices;
    }

    @Test
    public void shouldPerformIndexingOnAppend() throws IOException {
        runTest(queue -> {
            final ExcerptAppender appender = queue.acquireAppender();
            final long[] indices = writeMessagesStoreIndices(appender, queue.createTailer());
            assertExcerptsAreIndexed(queue, indices, i -> i % INDEX_SPACING == 0, ScanResult.FOUND);
        });
    }

    private <T extends Exception> void runTest(final ThrowingConsumer<RollingChronicleQueue, T> testMethod) throws T, IOException {
        try (final RollingChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.newFolder()).
                testBlockSize().timeProvider(clock::get).
                rollCycle(ROLL_CYCLE).indexSpacing(INDEX_SPACING).
                build()) {
            testMethod.accept(queue);
        }
    }
}