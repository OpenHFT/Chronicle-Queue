package net.openhft.chronicle;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

public class NonBlockingRemoteTailerTest {
    @Test
    public void testNonBlockingRemoteTailer() throws IOException {
        long start = System.currentTimeMillis();
        try (Chronicle chronicle = ChronicleQueueBuilder.remoteTailer().connectAddress("localhost", 1).build()) {
            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < 1000; i++) {
                assertFalse(tailer.nextIndex());
            }
        }
        System.out.printf("Took %,d ms for 1000 failed attempts%n", System.currentTimeMillis() - start);
    }
}
