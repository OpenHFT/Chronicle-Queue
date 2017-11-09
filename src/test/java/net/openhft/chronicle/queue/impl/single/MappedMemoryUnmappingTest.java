package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.RollCycles;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.fail;

public final class MappedMemoryUnmappingTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();
    private long baselineMappedMemory = 0L;

    @Test
    public void shouldUnmapMemoryAsCycleRolls() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());

        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.
                binary(tmp.newFolder()).testBlockSize().rollCycle(RollCycles.TEST_SECONDLY).
                timeProvider(clock::get).build()) {
            for (int i = 0; i < 100; i++) {
                queue.acquireAppender().writeDocument(System.nanoTime(), (d, t) -> d.int64(t));
                clock.addAndGet(TimeUnit.SECONDS.toMillis(1L));
                if (baselineMappedMemory == 0L) {
                    baselineMappedMemory = OS.memoryMapped();
                }
            }
        }

        final long timeoutAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
        while (System.currentTimeMillis() < timeoutAt) {
            if (OS.memoryMapped() < 2 * baselineMappedMemory) {
                return;
            }
        }

        fail(String.format("Mapped memory (%dB) did not fall below threshold (%dB)",
                OS.memoryMapped(), 2 * baselineMappedMemory));
    }
}
