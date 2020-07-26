package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.FlakyTestRunner;
import net.openhft.chronicle.core.Jvm;
import org.junit.Test;

import java.io.IOException;

public class RollCycleMultiThreadStressSharedWriterQueueTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() throws InterruptedException, IOException {
        try {
            FlakyTestRunner.run(super::stress);
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }

    static {
        System.setProperty("sharedWriteQ", "true");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        new RollCycleMultiThreadStressSharedWriterQueueTest().stress();
    }
}
