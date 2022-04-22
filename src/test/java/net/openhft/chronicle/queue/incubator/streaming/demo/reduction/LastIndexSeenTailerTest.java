package net.openhft.chronicle.queue.incubator.streaming.demo.reduction;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.incubator.streaming.Reduction;
import net.openhft.chronicle.queue.incubator.streaming.Reductions;
import net.openhft.chronicle.queue.incubator.streaming.ToLongDocumentExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

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

        final Reduction<LongSupplier> listener = Reductions.reducingLong(ToLongDocumentExtractor.extractingIndex(), 0, (a, b) -> b);

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

        assertEquals("16d00000003", Long.toHexString(listener.reduction().getAsLong()));

    }

}