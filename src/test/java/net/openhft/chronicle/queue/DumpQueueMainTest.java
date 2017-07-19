package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class DumpQueueMainTest {

    @Test
    public void shouldBeAbleToDumpReadOnlyQueueFile() throws Exception {
        final File dataDir = DirectoryUtils.tempDir(DumpQueueMainTest.class.getSimpleName());
        final SingleChronicleQueue queue = SingleChronicleQueueBuilder.
                binary(dataDir).
                build();

        final ExcerptAppender excerptAppender = queue.acquireAppender();
        excerptAppender.writeText("first");
        excerptAppender.writeText("last");

        final Path queueFile = Files.list(dataDir.toPath()).findFirst().orElseThrow(() ->
                new AssertionError("Could not find queue file in directory " + dataDir));
        assertThat(queueFile.toFile().setWritable(false), is(true));

        final CountingOutputStream countingOutputStream = new CountingOutputStream();
        DumpQueueMain.dump(queueFile.toFile(), new PrintStream(countingOutputStream), Long.MAX_VALUE);

        assertThat(countingOutputStream.bytes, is(not(0L)));
    }

    private static final class CountingOutputStream extends OutputStream {
        private long bytes;
        @Override
        public void write(final int b) throws IOException {
            bytes++;
        }
    }
}