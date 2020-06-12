package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public final class EofMarkerOnEmptyQueueTest extends QueueTestCommon {
    private static final ReferenceOwner test = ReferenceOwner.temporary("test");
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void shouldRecoverFromEmptyQueueOnRoll() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        Assume.assumeFalse(OS.isWindows());
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (final RollingChronicleQueue queue =
                     ChronicleQueue.singleBuilder(tmpFolder.newFolder()).
                             rollCycle(RollCycles.TEST_SECONDLY).
                             timeProvider(clock::get).
                             timeoutMS(1_000).
                             testBlockSize().build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            final DocumentContext context = appender.writingDocument();
            // start to write a message, but don't close the context - simulates crashed writer
            final long expectedEofMarkerPosition = context.wire().bytes().writePosition() - Wires.SPB_HEADER_SIZE;
            context.wire().write("foo").int32(1);

            final int startCycle = queue.cycle();

            clock.addAndGet(TimeUnit.SECONDS.toMillis(1L));

            final int nextCycle = queue.cycle();

            // ensure that the cycle file will roll
            assertNotEquals(nextCycle, startCycle);

            ExecutorService appenderExecutor = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("Appender"));
            appenderExecutor.submit(() -> {
                try (final DocumentContext nextCtx = queue.acquireAppender().writingDocument()) {
                    nextCtx.wire().write("bar").int32(7);
                }
            }).get(Jvm.isDebug() ? 3000 : 3, TimeUnit.SECONDS);

            appenderExecutor.shutdown();
            appenderExecutor.awaitTermination(1, TimeUnit.SECONDS);

            final SingleChronicleQueueStore firstCycleStore = queue.storeForCycle(test, startCycle, 0, false, null);
            assertNull(firstCycleStore);
//            final long firstCycleWritePosition = firstCycleStore.writePosition();
//            // assert that no write was completed
//            assertEquals(0L, firstCycleWritePosition) ;
//            firstCycleStore.release(test);

            final ExcerptTailer tailer = queue.createTailer();
            int recordCount = 0;
            int lastItem = -1;
            while (true) {
                try (final DocumentContext readCtx = tailer.readingDocument()) {
                    if (!readCtx.isPresent()) {
                        break;
                    }

                    final StringBuilder name = new StringBuilder();
                    final ValueIn field = readCtx.wire().readEventName(name);
                    recordCount++;
                    lastItem = field.int32();
                }
            }
            assertEquals(1, recordCount);
            assertEquals(7, lastItem);
        }
    }
}