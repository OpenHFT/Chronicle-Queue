package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import org.junit.Ignore;
import org.junit.Test;

public class RollCycleMultiThreadStressReadOnlyTest extends RollCycleMultiThreadStressTest {

    @Ignore("run manually because its a stress test")
    @Test
    public void repeatStress() throws InterruptedException {
        Jvm.setExceptionHandlers(null, null, null);
        for (int i = 0; i < 100; i++) {
            stress();
        }
    }

    @Ignore("run manually because its a stress test")
    @Test
    public void stress() throws InterruptedException {
        super.stress();
    }

    static {
        System.setProperty("read_only", "true");
    }
}
