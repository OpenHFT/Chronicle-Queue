package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static net.openhft.chronicle.queue.impl.single.RollCycleRetriever.getRollCycle;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.builder;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RollCycleRetrieverTest {

    private static final RollCycles ROLL_CYCLE = RollCycles.TEST_SECONDLY;
    private static final WireType WIRE_TYPE = WireType.BINARY;

    @Test
    public void shouldRetrieveCorrectRollCycleFromExistingQueueFile() throws Exception {
        final File queuePath = tempDir(RollCycleRetrieverTest.class.getSimpleName() + System.nanoTime());

        final SingleChronicleQueueBuilder builder =
                builder(queuePath, WIRE_TYPE).testBlockSize().rollCycle(ROLL_CYCLE);

        try (final SingleChronicleQueue queue = builder.build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            try (final DocumentContext context = appender.writingDocument()) {
                context.wire().write("foo").text("bar");
            }
        }

        assertThat(getRollCycle(queuePath.toPath(), WIRE_TYPE, builder.blockSize()).isPresent(), is(true));
        assertThat(getRollCycle(queuePath.toPath(), WIRE_TYPE, builder.blockSize()).get(), is(ROLL_CYCLE));
    }

    @Test
    public void shouldReturnEmptyWhenQueueDirDoesNotExist() throws Exception {
        final Path nonExistentPath = Paths.get("non", "existent", "file", Long.toHexString(System.nanoTime()));
        assertThat(getRollCycle(nonExistentPath, WIRE_TYPE, 4096).isPresent(), is(false));
    }

    @Test
    public void shouldReturnEmptyWhenNoQueueFilesHaveBeenWritten() throws Exception {
        final File queuePath = tempDir(RollCycleRetrieverTest.class.getSimpleName() + System.nanoTime());
        assertTrue(queuePath.mkdirs());
        assertThat(getRollCycle(queuePath.toPath(), WIRE_TYPE, 4096).isPresent(), is(false));
    }
}