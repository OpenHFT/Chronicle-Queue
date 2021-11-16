package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RollCycleDefaultingTest extends QueueTestCommon {
    @Test
    public void alias() {
        assertEquals(RollCycles.class, ObjectUtils.implementationToUse(RollCycle.class));
    }

    @After
    public void clearDefaultRollCycleProperty() {
        System.clearProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY);
    }

    @Test
    public void correctConfigGetsLoaded() {
        String aClass = RollCycles.HOURLY.getClass().getName();
        String configuredCycle = aClass + ":HOURLY";
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");
        assertEquals(RollCycles.HOURLY, builder.rollCycle());
    }

    @Test
    public void customDefinitionGetsLoaded() {
        String configuredCycle = MyRollcycle.class.getName();
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");

        assertTrue(builder.rollCycle() instanceof MyRollcycle);
    }

    @Test
    public void unknownClassDefaultsToDaily() {
        expectException("Default roll cycle class: foobarblah was not found");
        String configuredCycle = "foobarblah";
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");
        assertEquals(RollCycles.DEFAULT, builder.rollCycle());

    }

    @Test
    public void nonRollCycleDefaultsToDaily() {
        expectException("Configured default rollcycle is not a subclass of RollCycle");
        String configuredCycle = String.class.getName();
        System.setProperty(QueueSystemProperties.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");
        assertEquals(RollCycles.DEFAULT, builder.rollCycle());
    }

    public static class MyRollcycle implements RollCycle {
        private final RollCycle delegate = RollCycles.TEST_SECONDLY;

        @Override
        public @NotNull String format() {
            return "xyz";
        }

        @Override
        public int lengthInMillis() {
            return delegate.lengthInMillis();
        }

        @Override
        public int defaultIndexCount() {
            return delegate.defaultIndexCount();
        }

        @Override
        public int defaultIndexSpacing() {
            return delegate.defaultIndexSpacing();
        }

        @Override
        public int current(TimeProvider time, long epoch) {
            return delegate.current(time, epoch);
        }

        @Override
        public long toIndex(int cycle, long sequenceNumber) {
            return delegate.toIndex(cycle, sequenceNumber);
        }

        @Override
        public long toSequenceNumber(long index) {
            return delegate.toSequenceNumber(index);
        }

        @Override
        public int toCycle(long index) {
            return delegate.toCycle(index);
        }

        @Override
        public long maxMessagesPerCycle() {
            return 0;
        }

    }
}
