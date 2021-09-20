package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore("TODO FIX")
public class PretoucherEarlyCycleTest extends ChronicleQueueTestBase {
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final List<Integer> capturedCycles = new ArrayList<>();
    private final PretoucherTest.CapturingChunkListener chunkListener = new PretoucherTest.CapturingChunkListener();

    @Test
    public void shouldHandleEarlyCycleRollByPretoucher() {
        System.setProperty("SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs", "100");
        cycleRollByPretoucher(100);
    }

    private void cycleRollByPretoucher(int earlyMillis) {
        File dir = getTmpDir();
        clock.set(100);

        try (final SingleChronicleQueue queue = PretoucherTest.createQueue(dir, clock::get);
             final SingleChronicleQueue pretoucherQueue = PretoucherTest.createQueue(dir, clock::get);
             final Pretoucher pretoucher = new Pretoucher(pretoucherQueue, chunkListener, capturedCycles::add, true, true)) {

            range(0, 10).forEach(i -> {
                try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                    assertEquals(i == 0 ? 0 : i + 0.5, capturedCycles.size(), 0.5);
                    ctx.wire().write().int32(i);

                    ctx.wire().write().bytes(new byte[1024]);
                }
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1, capturedCycles.size());
                clock.addAndGet(950 - earlyMillis);
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                clock.addAndGet(50 + earlyMillis);
                assertEquals(i + 1.5, capturedCycles.size(), 0.5);
            });

            assertEquals(10.5, capturedCycles.size(), 0.5);
            assertFalse(chunkListener.chunkMap.isEmpty());
        }
    }
}