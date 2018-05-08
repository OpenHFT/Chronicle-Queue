package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SingleChronicleQueueBuilderTest {
    private static final String TEST_QUEUE_FILE = "src/test/resources/tr2/20170320.cq4";

    @Test
    public void shouldDetermineQueueDirectoryFromQueueFile() throws Exception {
        final Path path = Paths.get(OS.USER_DIR, TEST_QUEUE_FILE);
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(path)
                             .testBlockSize()
                             .build()) {
            assertThat(queue.createTailer().readingDocument().isPresent(), is(false));
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