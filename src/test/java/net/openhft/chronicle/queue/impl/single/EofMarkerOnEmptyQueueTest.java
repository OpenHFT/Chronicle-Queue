package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public final class EofMarkerOnEmptyQueueTest {
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void shouldRecoverFromEmptyQueueOnRoll() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(tmpFolder.newFolder()).
                             rollCycle(RollCycles.TEST_SECONDLY).
                             timeProvider(clock::get).
                             timeoutMS(1_000).
                             testBlockSize().build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            final DocumentContext context = appender.writingDocument();
            // start to write a message, but don't close the context - simulates crashed writer
            final long expectedEofMarkerPosition = context.wire().bytes().writePosition() - Wires.SPB_HEADER_SIZE;
            context.wire().writeEventName("foo").int32(1);

            final int startCycle = queue.cycle();

            clock.addAndGet(TimeUnit.SECONDS.toMillis(1L));

            final int nextCycle = queue.cycle();

            // ensure that the cycle file will roll
            assertThat(startCycle, is(not(nextCycle)));

            Executors.newSingleThreadExecutor().submit(() -> {
                try (final DocumentContext nextCtx = queue.acquireAppender().writingDocument()) {
                    nextCtx.wire().writeEventName("bar").int32(7);
                }

            }).get(Jvm.isDebug() ? 3000 : 3, TimeUnit.SECONDS);

            final WireStore firstCycleStore = queue.storeForCycle(startCycle, 0, false);
            final long firstCycleWritePosition = firstCycleStore.writePosition();
            // assert that no write was completed
            assertThat(firstCycleWritePosition, is(0L));

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

            assertThat(firstCycleStore.bytes().readVolatileInt(expectedEofMarkerPosition),
                    is(Wires.END_OF_DATA));
            assertThat(recordCount, is(1));
            assertThat(lastItem, is(7));
        }
    }
}