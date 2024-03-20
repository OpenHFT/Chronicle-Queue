package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WeeklyRollCycleTest {

    @Test
    public void testWeekly() {
        @NotNull String tmpDir = OS.getTarget()+"/testWeekly";
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(tmpDir).rollCycle(RollCycles.WEEKLY).build()) {
            // 1970-02-01 is a Sunday
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/01T00:00:00")));
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/02T00:00:00")));
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/03T00:00:00")));
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/04T00:00:00")));
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/05T00:00:00")));
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/06T00:00:00")));
            // 1970-02-07 is a Saturday
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/07T00:00:00")));
            assertEquals(4, queue.cycle(new SetTimeProvider("1970/02/07T23:59:59")));
            assertEquals(5, queue.cycle(new SetTimeProvider("1970/02/08T00:00:00")));
        }
    }
}
