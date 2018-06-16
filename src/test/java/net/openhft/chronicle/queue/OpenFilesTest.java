package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.DirectoryListing;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.SUFFIX;

/**
 * Created by Rob Austin
 */
public class OpenFilesTest {

    private static List<String> listOpenFiles(File dir) throws IOException,
            InterruptedException {

        final int processId = OS.getProcessId();
        final List<String> fileList = new ArrayList<>();

        final Process pmap = new ProcessBuilder("lsof", "-p", Integer.toString(processId), dir.getAbsolutePath())
                .start();
        pmap.waitFor();
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(pmap.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(SUFFIX)) {
                    fileList.add(line);
                }
            }
        }

        return fileList;
    }

    @Ignore("long running test")
    @Test
    public void test() throws IOException, InterruptedException {
        File tmp = new File("/tmp/tmp");

        try (SingleChronicleQueue q = ChronicleQueueBuilder.single(tmp).rollCycle(RollCycles.TEST_SECONDLY)
                .build()) {
            ExcerptAppender appender = q.acquireAppender();

            for (int i = 0; i < 10; i++) {

                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("hello").text("world");
                }

                Thread.sleep(1000L);
            }

            createTailer(q);

            long start = System.currentTimeMillis();

            Thread.sleep(5_000);

            //  Runtime.getRuntime().gc();

            System.out.println("-----");
            List<String> mappedQueueFileCount = listOpenFiles(tmp);
            Assert.assertTrue(!mappedQueueFileCount.isEmpty());
            removeDirectoryListing(mappedQueueFileCount);

            for (final String s : mappedQueueFileCount) {
                System.out.println(s);
            }

            System.out.println("");
            long pause = 1000 - (System.currentTimeMillis() - start);
            if (pause > 0)
                Thread.sleep(pause);

        }

        List<String> mappedQueueFileCount = listOpenFiles(tmp);
        Assert.assertTrue(!mappedQueueFileCount.isEmpty());

        Runtime.getRuntime().gc();
        System.runFinalization();

        List<String> listFileHandles = listOpenFiles(tmp);
        Assert.assertTrue(listFileHandles.isEmpty());
        System.out.println(listFileHandles);

    }

    private void createTailer(final SingleChronicleQueue q) {
        ExcerptTailer tailer = q.createTailer();
        for (; ; ) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    break;
                }
                Assert.assertEquals("world", dc.wire().read("hello").text());
            }
        }
    }

    private void removeDirectoryListing(final List<String> mappedQueueFileCount) {
        Iterator<String> iterator = mappedQueueFileCount.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().endsWith(DirectoryListing.DIRECTORY_LISTING_FILE))
                iterator.remove();
        }
    }

}

