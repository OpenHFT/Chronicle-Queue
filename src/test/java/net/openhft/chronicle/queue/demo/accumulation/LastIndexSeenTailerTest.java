package net.openhft.chronicle.queue.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.incubator.streaming.Accumulation;
import net.openhft.chronicle.queue.incubator.streaming.Accumulation.MapperTo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.incubator.streaming.Accumulation.builder;
import static org.junit.Assert.assertEquals;

public class LastIndexSeenTailerTest {

    private static final String Q_NAME = LastIndexSeenTailerTest.class.getSimpleName();

    @Before
    public void clearBefore() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @After
    public void clearAfter() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @Test
    public void lastIndexSeenTailer() {
        final SetTimeProvider tp = new SetTimeProvider(TimeUnit.DAYS.toNanos(365));

        // Add stuff that simulated existing values in the queue
        try (ChronicleQueue q = SingleChronicleQueueBuilder.builder()
                .path(Q_NAME)
                .timeProvider(tp)
                .build()) {
            ExcerptAppender appender = q.acquireAppender();
            appender.writeText("one");
            appender.writeText("two");
            appender.writeText("three");
        }

        Accumulation<MapperTo<Long>> listener = builder(AtomicLong::new)
                // On each excerpt appended, this accumulator will be called and
                // incremented by one
                .withAccumulator(((al, wire, index) -> al.set(index)))
                // Add a mapper that will be applied on each inspection of the
                // underlying Accumulation as to prevent accidental modification
                .withMapper(AtomicLong::get)
                .build();

        try (ChronicleQueue q = SingleChronicleQueueBuilder.builder()
                .appenderListener(listener)
                .path(Q_NAME)
                .timeProvider(tp)
                .build()) {

            // Bring the accumulation up to the current state
            listener.accept(q.createTailer());

            // Add new stuff
            ExcerptAppender appender = q.acquireAppender();
            appender.writeText("four");
        }

        assertEquals("16d00000003", Long.toHexString(listener.accumulation().map()));

    }

}