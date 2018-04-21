package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.IntStream.range;
import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class PretoucherTest {
    private final File path = tempDir(PretoucherTest.class.getSimpleName());
    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final List<Integer> capturedCycles = new ArrayList<>();
    private final CapturingChunkListener chunkListener = new CapturingChunkListener();

    private static SingleChronicleQueue createQueue(final File path, final TimeProvider timeProvider) {
        return SingleChronicleQueueBuilder.
                binary(path).
                timeProvider(timeProvider).
                rollCycle(RollCycles.TEST_SECONDLY).
                testBlockSize().
                wireType(WireType.BINARY).
                build();
    }

    @Test
    public void shouldHandleCycleRoll() throws Exception {
        try (final SingleChronicleQueue queue = createQueue(path, clock::get)) {
            final Pretoucher pretoucher = new Pretoucher(queue, chunkListener, capturedCycles::add);

            range(0, 10).forEach(i -> {
                try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                    ctx.wire().write().int32(i);
                    pretoucher.execute();
                    ctx.wire().write().bytes(new byte[1024]);
                }
                pretoucher.execute();
                clock.addAndGet(TimeUnit.SECONDS.toMillis(5L));
            });

            assertThat(capturedCycles.size(), is(10));
            assertThat(chunkListener.chunkMap.isEmpty(), is(false));
        }
    }

    @After
    public void deleteDir() throws Exception {
        DirectoryUtils.deleteDir(path);
    }

    private static final class CapturingChunkListener implements NewChunkListener {
        private final TreeMap<String, List<Integer>> chunkMap = new TreeMap<>();

        @Override
        public void onNewChunk(final String filename, final int chunk, final long delayMicros) {
            chunkMap.computeIfAbsent(filename, f -> new ArrayList<>()).add(chunk);
        }
    }
}