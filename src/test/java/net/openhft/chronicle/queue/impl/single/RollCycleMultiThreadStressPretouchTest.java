package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() {
        System.setProperty("pretouch", "true");
        super.stress();
    }
}