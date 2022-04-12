package net.openhft.chronicle.queue.incubator.streaming.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.incubator.streaming.Accumulation;
import net.openhft.chronicle.queue.incubator.streaming.Accumulation.MapperTo;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static net.openhft.chronicle.queue.incubator.streaming.Accumulations.*;
import static net.openhft.chronicle.queue.incubator.streaming.Accumulation.builder;
import static org.junit.Assert.assertEquals;

public class LastIndexSeenTest extends ChronicleQueueTestBase {

    private static final String Q_NAME = LastIndexSeenTest.class.getSimpleName();

    @Before
    public void clearBefore() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @After
    public void clearAfter() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @Test
    public void lastIndexSeenCustom() {
        Accumulation<MapperTo<Long>> listener = builder(AtomicLong::new)
                // On each excerpt appended, this accumulator will be called and
                // incremented by one
                .withAccumulator(((al, wire, index) -> al.set(index)))
                // Add a mapper that will be applied on each inspection of the
                // underlying Accumulation as to prevent accidental modification
                .withMapper(AtomicLong::get)
                .build();

        writeToQueue(listener);
        long indexLastSeen = listener.accumulation().map();

        assertEquals("16d00000002", Long.toHexString(indexLastSeen));
    }

    @Test
    public void lastIndexSeen() {
        Accumulation<LongSupplier> listener = reducingLong(extractingIndex(), 0, (a, b) -> b);

        writeToQueue(listener);

        long indexLastSeen = listener.accumulation().getAsLong();
        assertEquals("16d00000002", Long.toHexString(indexLastSeen));
    }

    @Test
    public void minAndMaxIndexSeen() {
        Accumulation<LongSupplier> minListener = reducingLong(extractingIndex(), Long.MAX_VALUE, Math::min);
        Accumulation<LongSupplier> maxListener = reducingLong(extractingIndex(), Long.MIN_VALUE, Math::max);

        writeToQueue(minListener.andThen(maxListener));

        long min = minListener.accumulation().getAsLong();
        long max = maxListener.accumulation().getAsLong();

        assertEquals("16d00000000", Long.toHexString(min));
        assertEquals("16d00000002", Long.toHexString(max));
    }

    private void writeToQueue(AppenderListener listener) {
        final SetTimeProvider tp = new SetTimeProvider(TimeUnit.DAYS.toNanos(365));
        try (ChronicleQueue q = SingleChronicleQueueBuilder.builder()
                .path(Q_NAME)
                .timeProvider(tp)
                .appenderListener(listener)
                .build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeText("one");
            appender.writeText("two");
            appender.writeText("three");
        }
    }

}