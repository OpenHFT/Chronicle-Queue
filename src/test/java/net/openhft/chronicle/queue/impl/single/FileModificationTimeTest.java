package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.junit.Assert.fail;

public final class FileModificationTimeTest extends ChronicleQueueTestBase {
    private final AtomicInteger fileCount = new AtomicInteger();

    private static void waitForDiff(final long a, final LongSupplier b) {
        final long timeout = System.currentTimeMillis() + 15_000L;
        while ((!Thread.currentThread().isInterrupted()) && System.currentTimeMillis() < timeout) {
            if (a != b.getAsLong()) {
                return;
            }
            Jvm.pause(1_000L);
        }

        fail("Values did not become different");
    }

    @Test
    public void shouldUpdateDirectoryModificationTime() {
        final File dir = getTmpDir();
        dir.mkdirs();

        final long startModTime = dir.lastModified();

        modifyDirectoryContentsUntilVisible(dir, startModTime);

        final long afterOneFile = dir.lastModified();

        modifyDirectoryContentsUntilVisible(dir, afterOneFile);
    }

    private void modifyDirectoryContentsUntilVisible(final File dir, final long startTime) {
        waitForDiff(startTime, () -> {
            createFile(dir, fileCount.getAndIncrement() + ".txt");
            return dir.lastModified();
        });
    }

    private void createFile(
            final File dir, final String filename) {
        final File file = new File(dir, filename);
        try (final FileWriter writer = new FileWriter(file)) {

            writer.append("foo");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
