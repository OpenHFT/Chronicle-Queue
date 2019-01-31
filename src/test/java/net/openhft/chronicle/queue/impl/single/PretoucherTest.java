package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.NewChunkListener;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Ignore;
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

    @Ignore("flakey test see http://teamcity.higherfrequencytrading.com/repository/download/OpenHFT_ReleaseJob_ReleaseByArtifact/320311:id/ReleaseAutomation/projects/chronicle-queue-runTests-1548952667748.log")
    @Test
    public void shouldHandleCycleRoll() {
        File dir = tempDir("shouldHandleCycleRoll");
        try (final SingleChronicleQueue queue = createQueue(dir, clock::get);
             final Pretoucher pretoucher = new Pretoucher(createQueue(dir, clock::get), chunkListener, capturedCycles::add)) {

            range(0, 10).forEach(i -> {
                try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                    assertThat(capturedCycles.size(), is(i));
                    ctx.wire().write().int32(i);
                    ctx.wire().write().bytes(new byte[1024]);
                }
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    e.printStackTrace();
                }
                assertThat(capturedCycles.size(), is(i + 1));
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    e.printStackTrace();
                }
                assertThat(capturedCycles.size(), is(i + 1));
                clock.addAndGet(TimeUnit.SECONDS.toMillis(5L));
            });

            assertThat(capturedCycles.size(), is(10));
            //assertThat(chunkListener.chunkMap.isEmpty(), is(false));
        }
    }

    @Test
    public void shouldHandleEarlyCycleRoll() {
        assert System.getProperty("SingleChronicleQueueExcerpts.earlyAcquireNextCycle") == null;
        assert System.getProperty("SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs") == null;
        System.setProperty("SingleChronicleQueueExcerpts.earlyAcquireNextCycle", "true");
        System.setProperty("SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs", "100");
        File dir = tempDir("shouldHandleEarlyCycleRoll");
        try (final SingleChronicleQueue queue = createQueue(dir, clock::get);
             final Pretoucher pretoucher = new Pretoucher(createQueue(dir, clock::get), chunkListener, capturedCycles::add)) {

            range(0, 10).forEach(i -> {
                try (final DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                    assertThat(capturedCycles.size(), is(i == 0 ? 0 : i + 1));
                    ctx.wire().write().int32(i);

                    ctx.wire().write().bytes(new byte[1024]);
                }
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    e.printStackTrace();
                }
                assertThat(capturedCycles.size(), is(i + 1));
                clock.addAndGet(950);
                try {
                    pretoucher.execute();
                } catch (InvalidEventHandlerException e) {
                    e.printStackTrace();
                }
                clock.addAndGet(50);
                assertThat(capturedCycles.size(), is(i + 2));
            });

            assertThat(capturedCycles.size(), is(11));
            assertThat(chunkListener.chunkMap.isEmpty(), is(false));
        } finally {
            System.clearProperty("SingleChronicleQueueExcerpts.earlyAcquireNextCycle");
            System.clearProperty("SingleChronicleQueueExcerpts.pretoucherPrerollTimeMs");
        }
    }

    private static final class CapturingChunkListener implements NewChunkListener {
        private final TreeMap<String, List<Integer>> chunkMap = new TreeMap<>();

        @Override
        public void onNewChunk(final String filename, final int chunk, final long delayMicros) {
            chunkMap.computeIfAbsent(filename, f -> new ArrayList<>()).add(chunk);
        }
    }
}