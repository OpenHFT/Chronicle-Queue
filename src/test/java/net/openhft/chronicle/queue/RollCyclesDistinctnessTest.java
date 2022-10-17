package net.openhft.chronicle.queue;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RollCyclesDistinctnessTest {
    
    @Test
    public void definedRollCycleFormatsAreDistinct() {
        Set<String> allPatterns = new HashSet<>();
        int count = 0;
        for (RollCycle cycle : RollCycles.all()) {
            allPatterns.add(cycle.format());
            count++;
        }
        assertEquals(allPatterns.size(), count);
    }
}
