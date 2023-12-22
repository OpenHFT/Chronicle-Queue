package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class CycleOverflowTest extends QueueTestCommon {

    @Test
    public void overflowingMaxMessagesInCycleShouldThrowException() {
        File path = getTmpDir();
        RollCycle rollCycle = TestRollCycles.TEST_DAILY;
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.set(System.currentTimeMillis());
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().timeProvider(timeProvider).rollCycle(rollCycle).path(path).build(); ExcerptAppender appender = queue.createAppender();) {
            assertThrows("Unable to index 64, the number of entries exceeds max number for the current rollcycle", IllegalStateException.class, () -> {
                for (int i = 0; i < rollCycle.maxMessagesPerCycle() + 1; i++) {
                    appender.writeText(Integer.toString(i));
                }
            });
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

}
