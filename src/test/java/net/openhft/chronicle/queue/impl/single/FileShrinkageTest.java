package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileShrinkageTest extends ChronicleQueueTestBase {

    @Test
    public void testNoShrinkage() throws IOException, InterruptedException {

        final File dataDir = getTmpDir();

        SetTimeProvider timeProvider = new SetTimeProvider();

        File file;
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .fileShrinkage(FileShrinkage.SHRINK_SYNCHRONOUSLY)
                .timeProvider(timeProvider).build()) {
            ExcerptAppender excerptAppender = queue.acquireAppender();
            excerptAppender.writeText("hello");
            file = excerptAppender.currentFile();
        }

        timeProvider.advanceMillis(2_000);
        Thread.sleep(2_000);
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dataDir)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(timeProvider)
                .fileShrinkage(FileShrinkage.SHRINK_SYNCHRONOUSLY)
                .build()) {

            queue.acquireAppender();

            RandomAccessFile raf = new RandomAccessFile(file, "r");
            long len = raf.length();

            System.out.println("len=" + len + ", file=" + file.getAbsolutePath());
            Assert.assertTrue(len > 5206000);
            Assert.assertTrue(len < 530000);
            System.out.println(len);
        }
    }


}
