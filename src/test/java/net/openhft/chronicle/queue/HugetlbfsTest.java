package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.bytes.internal.ChunkedMappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;

/**
 * FIXME Needs to be configured to only run in CI where a hugetlbfs mount exists.
 */
public class HugetlbfsTest {

    @Test
    public void mappedBytes_tmp() throws IOException {
        File file = new File("/tmp/testfile");
        file.createNewFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        int hugePageSize = 2048 * 1024;
        randomAccessFile.setLength(hugePageSize);
        MappedBytes bytes = MappedBytes.mappedBytes(file, OS.pageSize(), OS.pageSize(), hugePageSize, false);
        bytes.writeVolatileInt(0, 1);
        int value = bytes.readVolatileInt(0);
    }

    @Test
    public void mappedBytes_hugetlbfs() throws IOException {
        File file = new File("/mnt/huge/tom/testfile");
        file.createNewFile();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        int hugePageSize = 2048 * 1024;
        randomAccessFile.setLength(hugePageSize);
        MappedBytes bytes = MappedBytes.mappedBytes(file, OS.pageSize(), OS.pageSize(), false);
        bytes.writeVolatileInt(0, 33);
        assertEquals(33, bytes.readVolatileInt(0));
    }

    @Test
    public void queue_tmp() {
        String path = "/tmp/test-queue";
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single().path(path).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("1");
            assertEquals("1", tailer.readText());
        }
        IOTools.deleteDirWithFiles(path);
    }

    @Test
    public void queue_hugetlbfs() {
        String path = "/mnt/huge/tom/test-queue";
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single()
                .path(path)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("1");
            assertEquals("1", tailer.readText());
        }
        IOTools.deleteDirWithFiles(path);
    }

}
