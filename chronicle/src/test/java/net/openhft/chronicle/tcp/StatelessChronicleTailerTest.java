package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;

public class StatelessChronicleTailerTest extends StatelessChronicleTestBase {

    @Test
    public void testRemoteTailerReconnect() throws IOException {
        long start = System.currentTimeMillis();
        try (Chronicle chronicle = ChronicleQueueBuilder
                .remoteTailer()
                .connectAddress("localhost", 1)
                .reconnectionAttempts(1)
                .build()) {

            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < 1000; i++) {
                assertFalse(tailer.nextIndex());
            }
        }

        System.out.printf("Took %,d ms for 1000 failed attempts%n", System.currentTimeMillis() - start);
    }

    @Test
    public void testRemoteTailerReconnectWithSpin() throws IOException {
        try (Chronicle chronicle = ChronicleQueueBuilder
                .remoteTailer()
                .connectAddress("localhost", 1)
                .reconnectionInterval(250, TimeUnit.MILLISECONDS)
                .reconnectionAttempts(4)
                .reconnectionWarningThreshold(3)
                .build()) {

            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < 10; i++) {
                assertFalse(tailer.nextIndex());
            }
        }
    }
}
