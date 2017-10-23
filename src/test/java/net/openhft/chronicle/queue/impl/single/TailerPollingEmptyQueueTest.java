package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public final class TailerPollingEmptyQueueTest {

    @Test
    public void shouldNotGenerateExcessGarbage() throws Exception {
        try (final SingleChronicleQueue queue = createQueue()) {
            queue.path.mkdirs();
            assertThat(queue.path.list((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)).length, is(0));

            final ExcerptTailer tailer = queue.createTailer();

            for (int i = 0; i < 50; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            final long startCollectionCount = getCollectionCount();

            for (int i = 0; i < 1_000_000; i++) {
                assertFalse(tailer.readingDocument().isPresent());
            }

            assertThat(getCollectionCount() - startCollectionCount, is(0L));
        }
    }

    private long getCollectionCount() {
        final AtomicLong totalCollectionCount = new AtomicLong();
        ManagementFactory.getGarbageCollectorMXBeans().stream().
                mapToLong(GarbageCollectorMXBean::getCollectionCount).
                forEach(totalCollectionCount::addAndGet);
        return totalCollectionCount.get();
    }

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.binary(
                DirectoryUtils.tempDir(TailerPollingEmptyQueueTest.class.getName())).
                testBlockSize().
                build();
    }
}