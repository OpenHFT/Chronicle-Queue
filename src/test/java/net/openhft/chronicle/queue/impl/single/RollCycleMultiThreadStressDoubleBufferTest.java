package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

import java.io.IOException;

public class RollCycleMultiThreadStressDoubleBufferTest extends RollCycleMultiThreadStressTest {

    @Test
    public void stress() throws InterruptedException, IOException {
        super.stress();
    }

    static {
        System.setProperty("stress.double_buffer", "true");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        new RollCycleMultiThreadStressDoubleBufferTest().stress();
    }
}
