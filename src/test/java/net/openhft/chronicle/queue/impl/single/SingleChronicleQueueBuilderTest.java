package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_FILE_FILTER;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SingleChronicleQueueBuilderTest {
    @Test
    public void shouldDetermineQueueDirectoryFromQueueFile() throws Exception {
        final File queuePath = DirectoryUtils.tempDir(getClass().getSimpleName());
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(queuePath)
                             .testBlockSize()
                             .build()) {
            try (DocumentContext ctx = queue.acquireAppender().writingDocument()) {
                ctx.wire().write("key").text("value");
            }
        }
        final Path path = queuePath.listFiles(QUEUE_FILE_FILTER)[0].toPath();
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(path)
                             .testBlockSize()
                             .build()) {
            assertThat(queue.createTailer().readingDocument().isPresent(), is(true));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfQueuePathIsFileWithIncorrectExtension() throws Exception {
        final File tempFile = File.createTempFile(SingleChronicleQueueBuilderTest.class.getSimpleName(), ".txt");
        tempFile.deleteOnExit();
        SingleChronicleQueueBuilder.
                binary(tempFile);
    }
}