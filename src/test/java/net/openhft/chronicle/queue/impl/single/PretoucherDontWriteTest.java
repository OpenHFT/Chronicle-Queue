package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.IntStream.range;
import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static org.junit.Assert.assertEquals;

public class PretoucherDontWriteTest extends QueueTestCommon {
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final List<Integer> capturedCycles = new ArrayList<>();
    private final PretoucherTest.CapturingChunkListener chunkListener = new PretoucherTest.CapturingChunkListener();

    @Test
    public void dontWrite() {

        System.setProperty("SingleChronicleQueueExcerpts.dontWrite", "true");
        File dir = tempDir("shouldNotRoll");

try (final SingleChronicleQueue queue = PretoucherTest.createQueue(dir, clock::get);
     final SingleChronicleQueue pretoucherQueue = PretoucherTest.createQueue(dir, clock::get);
     final Pretoucher pretoucher = new Pretoucher(pretoucherQueue, chunkListener, capturedCycles::add)) {

    range(0, 10).forEach(i -> {
        try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
            assertEquals(i + 0.5, capturedCycles.size(), 0.5);
            ctx.wire().write().int32(i);
            ctx.wire().write().bytes(new byte[1024]);
        }
        try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1, capturedCycles.size());
                clock.addAndGet(TimeUnit.SECONDS.toMillis(5L));
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    throw Jvm.rethrow(e);
                }
                assertEquals(i + 1.5, capturedCycles.size(), 0.5);
            });

            assertEquals(10.5, capturedCycles.size(), 0.5);
        }
    }
}