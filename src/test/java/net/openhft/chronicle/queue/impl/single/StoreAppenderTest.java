package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.RollCycles.MINUTELY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

public class StoreAppenderTest {

    @Test(expected = IllegalStateException.class)
    public void testJumpingAMessageThrowsAIllegalStateException() {

        try (SingleChronicleQueue q = binary(tempDir("q"))
                .rollCycle(MINUTELY)
                .timeProvider(() -> 0).build();

             ExcerptAppender appender = q.acquireAppender()) {
            appender.writeText("hello");
            appender.writeText("hello");
            try (final DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeLong(1);
            }

            final long l = appender.lastIndexAppended();
            final RollCycle rollCycle = q.rollCycle();
            final long index = rollCycle.toIndex(rollCycle.toCycle(l) + 1, 1);
            ((InternalAppender) appender).writeBytes(index, Bytes.from("text"));

        }

    }
}