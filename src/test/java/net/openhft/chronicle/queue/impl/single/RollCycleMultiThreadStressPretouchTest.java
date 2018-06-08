package net.openhft.chronicle.queue.impl.single;

import org.junit.Ignore;
import org.junit.Test;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    @Test
    @Ignore("https://github.com/OpenHFT/Chronicle-Queue/issues/492")
    public void stress() {
        System.setProperty("pretouch", "true");
        super.stress();
    }
}