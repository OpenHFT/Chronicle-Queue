package net.openhft.chronicle.queue.cleanup;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OnReleaseTest extends QueueTestCommon {
    @Test
    public void onRelease() throws Throwable {
        FlakyTestRunner.builder(this::onRelease0).build().run();
    }

    public void onRelease0() {
        String path = OS.getTarget() + "/onRelease-" + Time.uniqueId();
        SetTimeProvider stp = new SetTimeProvider();
        AtomicInteger writeRoll = new AtomicInteger();
        AtomicInteger readRoll = new AtomicInteger();
        try (ChronicleQueue writeQ = SingleChronicleQueueBuilder
                .binary(path)
                .rollCycle(RollCycles.MINUTELY)
                .timeProvider(stp)
                .storeFileListener((c, f) -> {
                    System.out.println("write released " + f);
                    writeRoll.incrementAndGet();
                })
                .build();
             ChronicleQueue readQ = SingleChronicleQueueBuilder
                     .binary(path)
                     .rollCycle(RollCycles.MINUTELY)
                     .timeProvider(stp)
                     .storeFileListener((c, f) -> {
                         System.out.println("read released " + f);
                         readRoll.incrementAndGet();
                     })
                     .build()) {
            ExcerptAppender appender = writeQ.acquireAppender();
            ExcerptTailer tailer = readQ.createTailer();
            for (int i = 0; i < 500; i++) {
                appender.writeText("hello-" + i);
                assertNotNull(tailer.readText());
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(i, writeRoll.get());
                assertEquals(i, readRoll.get());
                stp.advanceMillis(66_000);
            }
        }

    }
}
