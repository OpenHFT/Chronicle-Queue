package net.openhft.chronicle.queue.impl.single;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() throws InterruptedException, IOException {
        super.stress();
    }

    static {
        System.setProperty("pretouch", "true");
    }
}