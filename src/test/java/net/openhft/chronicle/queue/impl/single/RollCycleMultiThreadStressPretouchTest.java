package net.openhft.chronicle.queue.impl.single;

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

    public static void main(String[] args) throws IOException, InterruptedException {
        new RollCycleMultiThreadStressPretouchTest().stress();
    }
}