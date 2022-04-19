package net.openhft.chronicle.queue.incubator.streaming.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.AppenderListener;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.incubator.streaming.Reduction;
import net.openhft.chronicle.queue.incubator.streaming.Reductions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static net.openhft.chronicle.queue.incubator.streaming.Reductions.reducingLong;
import static net.openhft.chronicle.queue.incubator.streaming.ToLongExcerptExtractor.extractingIndex;
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
    public void lastIndexSeen() {
        Reduction<LongSupplier> listener = Reductions.reducingLong(extractingIndex(), 0, (a, b) -> b);

        writeToQueue(listener);

        long indexLastSeen = listener.reduction().getAsLong();
        assertEquals("16d00000002", Long.toHexString(indexLastSeen));
    }

    @Test
    public void minAndMaxIndexSeen() {
        Reduction<LongSupplier> minListener = reducingLong(extractingIndex(), Long.MAX_VALUE, Math::min);
        Reduction<LongSupplier> maxListener = reducingLong(extractingIndex(), Long.MIN_VALUE, Math::max);

        writeToQueue(minListener.andThen(maxListener));

        long min = minListener.reduction().getAsLong();
        long max = maxListener.reduction().getAsLong();

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