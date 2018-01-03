package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class SingleChronicleQueueStoreTest {
    private static final int INDEX_SPACING = 4;
    private static final int RECORD_COUNT = INDEX_SPACING * 10;
    private static final RollCycles ROLL_CYCLE = RollCycles.DAILY;

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void shouldPerformIndexing() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir.newFolder()).
                testBlockSize().timeProvider(clock::get).
                rollCycle(ROLL_CYCLE).indexSpacing(INDEX_SPACING).
                build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            appender.lazyIndexing(false);
            final SingleChronicleQueueStore wireStore = (SingleChronicleQueueStore)
                    queue.storeForCycle(queue.cycle(), 0L, true);

            final long[] indices = new long[RECORD_COUNT];
            for (int i = 0; i < RECORD_COUNT; i++) {
                try (final DocumentContext ctx = appender.writingDocument()) {
                    ctx.wire().getValueOut().int32(i);
                }
            }

            final ExcerptTailer tailer = queue.createTailer();

            for (int i = 0; i < RECORD_COUNT; i++) {
                try (final DocumentContext ctx = tailer.readingDocument()) {
                    assertThat("Expected record at index " + i, ctx.isPresent(), is(true));
                    indices[i] = tailer.index();
                }
            }
            final Field field = SingleChronicleQueueStore.class.getDeclaredField("recovery");
            field.setAccessible(true);

            final TimedStoreRecovery recovery = (TimedStoreRecovery) field.get(wireStore);
            final SCQIndexing indexing = wireStore.indexing;

            for (int i = 0; i < RECORD_COUNT; i++) {
                final int startLinearScanCount = indexing.linearScanCount;
                assertThat(indexing.moveToIndex0(recovery, (SingleChronicleQueueExcerpts.StoreTailer) tailer, indices[i]),
                        is(ScanResult.FOUND));

                if (i % INDEX_SPACING == 0) {
                    assertThat(indexing.linearScanCount, is(startLinearScanCount));
                } else {
                    assertThat(indexing.linearScanCount, is(startLinearScanCount + 1));
                }
            }
        }
    }
}