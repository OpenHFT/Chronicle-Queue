package net.openhft.chronicle.queue.namedtailer;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NamedTailerVersioningTest extends QueueTestCommon {

    @Test
    public void verifyBackwardsCompatibility_tailerPositionsAreRetained() throws IOException {
        // Copy the data from src/test/resources
        Path templatePath = Paths.get(this.getClass().getResource("/named-tailer/5.25ea1-backwards-compat").getFile());
        Path targetPath = Paths.get(OS.getTarget()).resolve(templatePath.getFileName());
        copyFolder(templatePath, targetPath);

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(targetPath).build();
             ExcerptTailer tailerOne = queue.createTailer("tailerOne");
             ExcerptTailer tailerTwo = queue.createTailer("tailerTwo");
             ExcerptTailer tailerThree = queue.createTailer("tailerThree")) {

            assertEquals(84486301679617L, tailerOne.index());
            assertEquals(84486301679617L, tailerTwo.index());
            assertEquals(84486301679617L, tailerThree.index());

            try (LongValue tailerOneVersion = queue.indexVersionForId("tailerOne");
                 LongValue tailerTwoVersion = queue.indexVersionForId("tailerTwo");
                 LongValue tailerThreeVersion = queue.indexVersionForId("tailerThree")) {
                assertEquals(0, tailerOneVersion.getValue());
                assertEquals(0, tailerTwoVersion.getValue());
                assertEquals(0, tailerThreeVersion.getValue());
            }

        } finally {
            IOTools.deleteDirWithFiles(targetPath.toString());
        }
    }

    @Test
    public void versionAndIndexRetentionAcrossMultipleLifecycles() {
        File queuePath = getTmpDir();

        // Open for first time
        long index;
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("named_1")) {

            appender.writeText("hello");
            tailer.readText();
            index = tailer.index();
            assertNotEquals(0, index);
        }

        // Open for the second time ensure that the tailer position was retained
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptTailer tailer = queue.createTailer("named_1");) {
            assertEquals(index, tailer.index());
        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    public void noVersionIncrements() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("named_1")) {

            LongValue indexVersion = queue.indexVersionForId("named_1");
            assertEquals(-1, indexVersion.getValue());
            indexVersion.close();

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    @Test
    public void multipleVersionIncrements() {
        File queuePath = getTmpDir();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path(queuePath).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer("named_1")) {

            int versions = 100;
            for (int i = 0; i < versions; i++) {
                appender.writeText("test");
                tailer.readText();
            }

            LongValue indexVersion = queue.indexVersionForId("named_1");
            assertEquals(100, indexVersion.getValue());
            indexVersion.close();

        } finally {
            IOTools.deleteDirWithFiles(queuePath);
        }
    }

    public void copyFolder(Path src, Path dest) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> copy(source, dest.resolve(src.relativize(source))));
        }
    }

    private void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest, REPLACE_EXISTING);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
