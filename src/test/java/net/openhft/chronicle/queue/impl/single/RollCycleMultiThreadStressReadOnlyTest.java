package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

public class RollCycleMultiThreadStressReadOnlyTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressReadOnlyTest() {
        super(StressTestType.READONLY);
    }

    @Test
    public void stress() throws Exception {
        super.stress();
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressReadOnlyTest().stress();
    }
}
