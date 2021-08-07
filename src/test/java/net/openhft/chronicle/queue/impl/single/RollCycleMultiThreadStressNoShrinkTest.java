package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

public class RollCycleMultiThreadStressNoShrinkTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressNoShrinkTest() {
        super(StressTestType.VANILLA);
    }

    @Test
    public void stress() throws Exception {
        try {
            System.setProperty("chronicle.queue.disableFileShrinking", "true");
            super.stress();
        } finally {
            System.clearProperty("chronicle.queue.disableFileShrinking");
        }
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressNoShrinkTest().stress();
    }
}
