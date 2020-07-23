package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assume.assumeFalse;

public class TestDeleteQueueFile extends ChronicleQueueTestBase {

    private Path tempQueueDir = getTmpDir().toPath();

    @Test
    public void testQueueFileDeletionWhileInUse() throws IOException {

        assumeFalse(OS.isWindows());

        SetTimeProvider timeProvider = new SetTimeProvider();

        String queueName = "unitTestQueue";

        QueueStoreFileListener listener = new QueueStoreFileListener(queueName);

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(tempQueueDir + "/" + queueName).
                timeProvider(timeProvider).storeFileListener(listener)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

//            System.out.println("first index : " + queue.firstIndex());
            Assert.assertEquals(Long.MAX_VALUE, queue.firstIndex());

            //write 10 records should go to first day file
            for (int i = 0; i < 10; i++) {
                appender.writeText("test");
            }

            long firstIndex = queue.firstIndex();

            long indexAfter10Records = appender.lastIndexAppended();
//            System.out.println("index after writing 10 records: " + indexAfter10Records);

            //roll to next day file
            timeProvider.advanceMillis(24 * 60 * 60 * 1000);

            appender.writeText("test2");
            long firstIndexOfSecondCycle = appender.lastIndexAppended();

            //write 5 records in next file
            for (int i = 0; i < 4; i++) {
                appender.writeText("test2");
            }

            Map<String, List<String>> queueToRollFilesOnAcquireMap = listener.getQueueToRollFilesOnAcquireMap();
            Map<String, List<String>> queueToRollFilesOnReleaseMap = listener.getQueueToRollFilesOnReleaseMap();

            Assert.assertEquals(1, queueToRollFilesOnAcquireMap.size());
            List<String> files = queueToRollFilesOnAcquireMap.get(queueName);
            Assert.assertEquals(1, files.size());
            String secondFile = files.get(0);

            //other will have 1 as only first file is released
            files = queueToRollFilesOnReleaseMap.get(queueName);
            Assert.assertEquals(1, files.size());
            String firstFile = files.get(0);

            Assert.assertNotEquals(firstFile, secondFile);

            long indexAfter5Records = appender.lastIndexAppended();
//            System.out.println("index after writing 5 records: " + indexAfter5Records);

            //now lets create one reader which will read all content
            ExcerptTailer excerptTailer = queue.createTailer();
            for (int i = 0; i < 10; i++) {
                Assert.assertEquals("test", excerptTailer.readText());
            }

//            System.out.println("index after reading 10 records: " + excerptTailer.index());
            Assert.assertEquals(firstIndex, excerptTailer.index() - 10);
            for (int i = 0; i < 5; i++) {
                Assert.assertEquals("test2", excerptTailer.readText());
            }

//            System.out.println("index after reading 5 records: " + excerptTailer.index());
            Assert.assertEquals(indexAfter5Records, excerptTailer.index() - 1);

            //lets delete first file
//            System.out.println("Deleting first release file: " + firstFile);

            Files.delete(Paths.get(firstFile));

            queue.refreshDirectoryListing();

            Assert.assertEquals(queue.firstIndex(), firstIndexOfSecondCycle);

            // and create a tailer it should only read
            //data in second file
            ExcerptTailer excerptTailer2 = queue.createTailer();
//            System.out.println("index before reading 5: " + excerptTailer2.index());

            //AFTER CREATING A BRAND NEW TAILER, BELOW ASSERTION ALSO FAILS
            //WAS EXPECTING THAT TAILER CAN READ FROM START OF QUEUE BUT INDEX IS LONG.MAX

            Assert.assertEquals(indexAfter5Records - 5, excerptTailer2.index() - 1);

            //BELOW THROWS NPE, WAS EXPECTING THAT WE CAN READ FROM SECOND DAILY QUEUE FILE
//            System.out.println("excerptTailer2: " + excerptTailer2.peekDocument());
            for (int i = 0; i < 5; i++) {
                Assert.assertEquals("test2", excerptTailer2.readText());
            }
        }
 }

    final class QueueStoreFileListener implements StoreFileListener {

        private String queueName;
        private Map<String, List<String>> queueToRollFilesOnReleaseMap = new HashMap<>();
        private Map<String, List<String>> queueToRollFilesOnAcquireMap = new HashMap<>();

        public QueueStoreFileListener(String queueName) {
            this.queueName = queueName;
        }

        @Override
        public void onReleased(int cycle, File file) {
//            System.out.println("onReleased called cycle: " + cycle + "file: " + file);

            List<String> files = queueToRollFilesOnReleaseMap.get(queueName);
            if (files == null) {
                files = new ArrayList<>();
            }

            String fileAbsPath = file.getAbsolutePath();
            if (!files.contains(fileAbsPath)) {
                files.add(fileAbsPath);
            }
            queueToRollFilesOnReleaseMap.put(queueName, files);

            //update acquire file map
            List<String> acqfiles = queueToRollFilesOnAcquireMap.get(queueName);
            acqfiles.remove(file.getAbsolutePath());
            queueToRollFilesOnAcquireMap.put(queueName, acqfiles);

        }

        @Override
        public void onAcquired(int cycle, File file) {
//            System.out.println("onAcquired called cycle: " + cycle + "file: " + file);

            List<String> files = queueToRollFilesOnAcquireMap.get(queueName);
            if (files == null) {
                files = new ArrayList<>();
            }

            String fileAbsPath = file.getAbsolutePath();
            if (!files.contains(fileAbsPath)) {
                files.add(fileAbsPath);
            }

            queueToRollFilesOnAcquireMap.put(queueName, files);

        }

        public Map<String, List<String>> getQueueToRollFilesOnAcquireMap() {
            return queueToRollFilesOnAcquireMap;
        }

        public Map<String, List<String>> getQueueToRollFilesOnReleaseMap() {
            return queueToRollFilesOnReleaseMap;
        }
    }
}