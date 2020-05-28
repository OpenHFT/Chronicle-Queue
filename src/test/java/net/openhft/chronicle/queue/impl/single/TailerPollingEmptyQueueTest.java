package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class TailerPollingEmptyQueueTest {

    @Test
    public void shouldNotGenerateExcessGarbage() {
        try (final SingleChronicleQueue queue = createQueue()) {
            queue.path.mkdirs();
            assertEquals(0, queue.path.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)).length);

            final ExcerptTailer tailer = queue.createTailer();

            for (int i = 0; i < 50; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            final long startCollectionCount = GcControls.getGcCount();

            for (int i = 0; i < 1_000_000; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            assertEquals(0L, GcControls.getGcCount() - startCollectionCount);
        }
    }

    private SingleChronicleQueue createQueue() {
        return ChronicleQueue.singleBuilder(
                DirectoryUtils.tempDir(TailerPollingEmptyQueueTest.class.getName())).
                testBlockSize().
                build();
    }
}