package net.openhft.chronicle.queue.impl.single.stress;

import org.junit.Test;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressPretouchTest() {
        super(StressTestType.PRETOUCH);
    }

    @Test
    public void stress() throws Exception {
        super.stress();
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressPretouchTest().stress();
    }
}