package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.FlakyTestRunner;
import net.openhft.chronicle.core.Jvm;
import org.junit.Test;

public class RollCycleMultiThreadStressPretouchTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() throws Exception {
        super.stress();
    }

    static {
        System.setProperty("pretouch", "true");
    }

    public static void main(String[] args) throws Exception {
        new RollCycleMultiThreadStressPretouchTest().stress();
    }
}