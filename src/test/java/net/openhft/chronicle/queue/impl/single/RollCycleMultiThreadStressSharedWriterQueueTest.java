package net.openhft.chronicle.queue.impl.single;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class RollCycleMultiThreadStressSharedWriterQueueTest extends RollCycleMultiThreadStressTest {

    /*@Ignore("run manually")
    @Test
    public void repeatStress() throws InterruptedException {
        Jvm.setExceptionHandlers(null, null, null);
        for (int i = 0; i < 100; i++) {
            stress();
        }
    }*/

    @Ignore("flaky test")
    @Test
    public void stress() throws InterruptedException, IOException {
        super.stress();
    }

    static {
        System.setProperty("sharedWriteQ", "true");
    }
}
