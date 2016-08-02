package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static junit.framework.TestCase.assertFalse;

/**
 * Created by skidder on 8/2/16.
 *
 * Targeting the problem of tailers in different threads where one writes very rarely, and the other nearly constantly.
 *
 * The rare appender will have very bad latency proportional to the number of messages written since it last appended.
 */
public class RareAppenderLatencyTest {
    private final static int HEAVY_MSGS = 1_000_000;
    private final static int RARE_MSGS = 50;

    @Test
    public void testRareAppenderLatency() throws IOException, InterruptedException {
        String pathname = OS.getTarget() + "/testRareAppenderLatency-" + System.nanoTime();
        new File(pathname).deleteOnExit();

        // Shared queue between two threads appending. One appends very rarely, another heavily.
        ChronicleQueue queue = new SingleChronicleQueueBuilder(pathname)
                .rollCycle(RollCycles.HOURLY)
                .build();

        String text = getText();

        // Write a some messages with an appender from Main thread.
        ExcerptAppender rareAppender = queue.acquireAppender();
        for (int i = 0; i < RARE_MSGS; i++) {
            try (DocumentContext ctx = rareAppender.writingDocument()) {
                ctx.wire()
                        .write(() -> "ts").int64(System.currentTimeMillis())
                        .write(() -> "msg").text(text);
            }
        }

        // Write a bunch of messages from another thread.
        Thread heavyWriter = new Thread(() -> {
            ExcerptAppender appender = queue.acquireAppender();
            long start = System.currentTimeMillis();
            for (int i = 0; i < HEAVY_MSGS; i++) {
                try (DocumentContext ctx = appender.writingDocument()) {
                    ctx.wire()
                            .write(() -> "ts").int64(System.currentTimeMillis())
                            .write(() -> "msg").text(text);
                }
            }

            System.out.println("Wrote heavy " + HEAVY_MSGS + " msgs in " + (System.currentTimeMillis() - start) + " ms");
        });

        heavyWriter.start();
        heavyWriter.join();

        // Write a message from the Main thread again (this will have unacceptable latency!)
        rareAppender = queue.acquireAppender();
        long now = System.currentTimeMillis();
        try (DocumentContext ctx = rareAppender.writingDocument()) {
            ctx.wire()
                    .write(() -> "ts").int64(System.currentTimeMillis())
                    .write(() -> "msg").text(text);
        }
        long l = System.currentTimeMillis() - now;


        // Write another message from the Main thread (this will be fast since we are caught up)
        now = System.currentTimeMillis();
        try (DocumentContext ctx = rareAppender.writingDocument()) {
            ctx.wire()
                    .write(() -> "ts").int64(System.currentTimeMillis())
                    .write(() -> "msg").text(text);
        }
        System.out.println("Wrote first rare one in " + l + " ms");
        System.out.println("Wrote another rare one in " + (System.currentTimeMillis() - now) + " ms");

        assertFalse("Appending from rare thread latency too high!", l > 150);
    }

    @NotNull
    protected String getText() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) sb.append(UUID.randomUUID());
        return sb.toString();
    }
}
