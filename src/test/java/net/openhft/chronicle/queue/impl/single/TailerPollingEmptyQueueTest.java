package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public final class TailerPollingEmptyQueueTest {

    @Test
    public void shouldNotGenerateExcessGarbage() {
        try (final SingleChronicleQueue queue = createQueue()) {
            queue.path.mkdirs();
            assertThat(queue.path.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)).length, is(0));

            final ExcerptTailer tailer = queue.createTailer();

            for (int i = 0; i < 50; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            final long startCollectionCount = GcControls.getGcCount();

            for (int i = 0; i < 1_000_000; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            assertThat(GcControls.getGcCount() - startCollectionCount, is(0L));
        }
    }

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.binary(
                DirectoryUtils.tempDir(TailerPollingEmptyQueueTest.class.getName())).
                testBlockSize().
                build();
    }
}