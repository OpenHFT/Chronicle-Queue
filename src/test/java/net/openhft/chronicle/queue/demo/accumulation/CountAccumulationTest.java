package net.openhft.chronicle.queue.demo.accumulation;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.AppenderListener.Accumulation;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static net.openhft.chronicle.queue.AppenderListener.Accumulation.builder;
import static org.junit.Assert.assertEquals;

public class CountAccumulationTest extends ChronicleQueueTestBase {

    private static final String Q_NAME = CountAccumulationTest.class.getSimpleName();

    @Before
    public void clearBefore() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @After
    public void clearAfter() {
        IOTools.deleteDirWithFiles(Q_NAME);
    }

    @Test
    public void countCustom() {
        Accumulation<AtomicLong> listener = builder(AtomicLong::new)
                // On each excerpt appended, this accumulator will be called and
                // incremented by one
                .withAccumulator(((al, wire, index) -> al.getAndIncrement()))
                .build();

        count(listener);
        assertEquals(3, listener.accumulation().get());
    }

    @Test
    public void builtInCustom() {
        Accumulation<LongSupplier> listener = Accumulations.counting();
        count(listener);
        assertEquals(3, listener.accumulation().getAsLong());
    }

    private void count(AppenderListener listener) {
        final SetTimeProvider tp = new SetTimeProvider(1_000_000_000);
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