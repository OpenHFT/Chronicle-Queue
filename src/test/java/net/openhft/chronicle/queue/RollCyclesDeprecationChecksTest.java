package net.openhft.chronicle.queue;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class RollCyclesDeprecationChecksTest extends QueueTestCommon {

    @SuppressWarnings("deprecation")
    @Test
    public void shouldWarnWhenDeprecatedRollCycleIsInUse() {
        expectException("You've configured your queue to use a deprecated RollCycle (net.openhft.chronicle.queue.RollCycles.HOURLY) please consider switching to net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.HOURLY, as the RollCycle constant you've nominated will be removed in a future release!");
        RollCycles.warnIfDeprecated(RollCycles.HOURLY);
    }

    @Test
    public void shouldNotWarnWhenNonDeprecatedRollCycleIsInUse() {
        RollCycles.warnIfDeprecated(RollCycles.FAST_HOURLY);
    }

    @Test
    public void shouldNotWarnWhenCustomRollCycleIsInUse() {
        RollCycles.warnIfDeprecated(new CustomRollCycle());
    }

    @Test
    public void shouldIndicateIfARollCycleIsDeprecated() {
        assertTrue(RollCycles.isDeprecated(RollCycles.HOURLY));
        assertFalse(RollCycles.isDeprecated(RollCycles.FAST_HOURLY));
        assertFalse(RollCycles.isDeprecated(new CustomRollCycle()));
    }

    @Test
    public void deprecationStatusIsCorrect() throws IllegalAccessException {
        Set<RollCycles> allRollCycles = new HashSet<>(Arrays.asList(RollCycles.values()));
        Set<RollCycles> checkedRollCycles = new HashSet<>();
        for (Field field : RollCycles.class.getFields()) {
            if (field.isEnumConstant()) {
                final RollCycles rollCycle = (RollCycles) field.get(null);
                assertEquals(field.getAnnotation(Deprecated.class) != null, RollCycles.isDeprecated(rollCycle));
                checkedRollCycles.add(rollCycle);
            }
        }
        assertEquals(allRollCycles, checkedRollCycles);
    }

    private static class CustomRollCycle implements RollCycle {
        @Override
        public @NotNull String format() {
            return "oooahhh";
        }

        @Override
        public int lengthInMillis() {
            return 0;
        }

        @Override
        public int defaultIndexCount() {
            return 0;
        }

        @Override
        public int defaultIndexSpacing() {
            return 0;
        }

        @Override
        public long toIndex(int cycle, long sequenceNumber) {
            return 0;
        }

        @Override
        public long toSequenceNumber(long index) {
            return 0;
        }

        @Override
        public int toCycle(long index) {
            return 0;
        }

        @Override
        public long maxMessagesPerCycle() {
            return 0;
        }
    }
}