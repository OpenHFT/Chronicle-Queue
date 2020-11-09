package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

public class RollCycleMultiThreadStressDoubleBufferTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() throws Exception {
        super.stress();
    }

    static {
        System.setProperty("stress.double_buffer", "true");
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressDoubleBufferTest().stress();
    }
}
