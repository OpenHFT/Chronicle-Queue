package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import org.junit.Test;

public class RollCycleMultiThreadStressNoShrinkTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() throws Exception {
        super.stress();
    }

    static {
        System.setProperty("chronicle.queue.disableFileShrinking", "true");
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressDoubleBufferTest().stress();
    }
}
