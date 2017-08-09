package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.builder;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RollCycleGetterTest {

    private static final RollCycles ROLL_CYCLE = RollCycles.TEST_SECONDLY;
    private static final WireType WIRE_TYPE = WireType.BINARY;

    @Test
    public void shouldRetrieveCorrectRollCycleFromExistingQueueFile() throws Exception {
        final File queuePath = tempDir(RollCycleGetterTest.class.getSimpleName() + System.nanoTime());

        final SingleChronicleQueueBuilder builder =
                builder(queuePath, WIRE_TYPE).testBlockSize().rollCycle(ROLL_CYCLE);

        try (final SingleChronicleQueue queue = builder.build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            try (final DocumentContext context = appender.writingDocument()) {
                context.wire().write("foo").text("bar");
            }
        }

        assertThat(RollCycleGetter.getRollCycle(queuePath.toPath(), WIRE_TYPE, builder.blockSize()).isPresent(), is(true));
        assertThat(RollCycleGetter.getRollCycle(queuePath.toPath(), WIRE_TYPE, builder.blockSize()).get(), is(ROLL_CYCLE));
    }
}