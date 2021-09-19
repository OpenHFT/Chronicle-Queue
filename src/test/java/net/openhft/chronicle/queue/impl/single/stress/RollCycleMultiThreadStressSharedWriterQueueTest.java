package net.openhft.chronicle.queue.impl.single.stress;

import org.junit.Test;

public class RollCycleMultiThreadStressSharedWriterQueueTest extends RollCycleMultiThreadStressTest {

    public RollCycleMultiThreadStressSharedWriterQueueTest() {
        super(StressTestType.SHAREDWRITEQ);
    }

    @Test
    public void stress() throws Exception {
        super.stress();
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressSharedWriterQueueTest().stress();
    }
}
