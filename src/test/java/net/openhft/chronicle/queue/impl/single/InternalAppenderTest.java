package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class InternalAppenderTest extends QueueTestCommon {

    @Test
    public void replicationTest() throws Exception {
        final File file = Files.createTempDirectory("queue").toFile();
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.single(file).build()) {
            final long index = queue.rollCycle().toIndex(queue.cycle(), 0);
            final InternalAppender appender = (InternalAppender) queue.acquireAppender();

            // First, we "replicate" a message, using the InternalAppender
            // interface because we need to preserve index numbers.
            appender.writeBytes(index, Bytes.from("Replicated"));

            // Next, a message is written locally by another app (usually a different process).
            try (final SingleChronicleQueue app = SingleChronicleQueueBuilder.single(file).build()) {
                app.acquireAppender().writeBytes(Bytes.from("Written locally"));
            }

            // The other app exits, and at some point later we need to start replicating again.

            appender.writeBytes(index + 2, Bytes.from("Replicated 2"));

            // We should have three messages in our queue.
            assertEquals(3, queue.entryCount());

        } finally {
            IOTools.deleteDirWithFiles(file);
        }
    }
}