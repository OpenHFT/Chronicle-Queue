package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.DirectoryUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SingleChronicleQueueBuilderTest {
    private static final String TEST_QUEUE_FILE = "src/test/resources/tr2/20170320.cq4";

    @Test
    public void shouldDetermineQueueDirectoryFromQueueFile() {
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

    Path tmpDir = DirectoryUtils.tempDir(StuckQueueTest.class.getSimpleName()).toPath();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void setup() throws Exception {
        tmpDir.toFile().mkdirs();
        Path templatePath = Paths.get(StuckQueueTest.class.getResource("/tr2/directory-listing.cq4t").getFile());
        Path to = tmpDir.resolve(templatePath.getFileName());
        Files.copy(templatePath, to, StandardCopyOption.REPLACE_EXISTING);
        File file = tmpDir.resolve("20170320.cq4").toFile();
        new FileOutputStream(file).close();
    }

    @After
    public void cleanup() {
        DirectoryUtils.deleteDir(tmpDir.toFile());
    }

    @Test
    @Ignore("crashes JVM")
    public void shouldHandleEmptyFile() {
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(tmpDir)
                             .testBlockSize()
                             .readOnly(true)
                             .build()) {
            assertThat(queue.createTailer().readingDocument().isPresent(), is(false));
        }
    }
}