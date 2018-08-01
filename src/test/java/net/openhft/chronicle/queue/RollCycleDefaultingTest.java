package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

public class RollCycleDefaultingTest {
    @Test
    public void correctConfigGetsLoaded() {
        String aClass = RollCycles.HOURLY.getClass().getName();
        String configuredCycle = aClass + ":HOURLY";
        System.setProperty(AbstractChronicleQueueBuilder.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");
        Assert.assertEquals(RollCycles.HOURLY, builder.rollCycle());
    }

    @Test
    public void customDefinitionGetsLoaded() {
        String configuredCycle = MyRollcycle.class.getName();
        System.setProperty(AbstractChronicleQueueBuilder.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");

        Assert.assertTrue(builder.rollCycle() instanceof MyRollcycle);
    }

    @Test
    public void unknownClassDefaultsToDaily(){
        String configuredCycle = "foobarblah";
        System.setProperty(AbstractChronicleQueueBuilder.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");
        Assert.assertEquals(RollCycles.DAILY, builder.rollCycle());

    }

    @Test
    public void nonRollCycleDefaultsToDaily(){
        String configuredCycle = String.class.getName();
        System.setProperty(AbstractChronicleQueueBuilder.DEFAULT_ROLL_CYCLE_PROPERTY, configuredCycle);
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary("test");
        Assert.assertEquals(RollCycles.DAILY, builder.rollCycle());
    }

    public static class MyRollcycle implements RollCycle {
        @Override
        public String format() {
            return null;
        }

        @Override
        public int length() {
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
        public int current(TimeProvider time, long epoch) {
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
    }
}
