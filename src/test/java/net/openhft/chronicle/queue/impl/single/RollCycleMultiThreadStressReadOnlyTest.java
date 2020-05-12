package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class RollCycleMultiThreadStressReadOnlyTest extends RollCycleMultiThreadStressTest {

    @Ignore("run manually because its a stress test")
    @Test
    public void repeatStress() throws InterruptedException, IOException {
        Jvm.setExceptionHandlers(null, null, null);
        for (int i = 0; i < 100; i++) {
            stress();
        }
    }

    @Ignore("run manually because its a stress test")
    @Test
    public void stress() throws InterruptedException, IOException {
        super.stress();
    }

    static {
        System.setProperty("read_only", "true");
    }
}
