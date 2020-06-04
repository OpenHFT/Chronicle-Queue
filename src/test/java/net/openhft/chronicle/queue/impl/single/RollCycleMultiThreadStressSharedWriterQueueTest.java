package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.FlakyTestRunner;
import net.openhft.chronicle.core.Jvm;
import org.junit.After;
import org.junit.Before;
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

    //    @Ignore("flaky test")
    @Test
    public void stress() throws InterruptedException, IOException {
        try {
            FlakyTestRunner.run(super::stress);
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }

    @Before
    public void sharedWriteQ() {
        SHARED_WRITE_QUEUE = true;
    }

    @After
    public void rm_sharedWriteQ() {
        SHARED_WRITE_QUEUE = false;
    }
}
