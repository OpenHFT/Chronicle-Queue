package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.junit.Assume.assumeFalse;

public class StuckQueueTest extends ChronicleQueueTestBase {

    @Test
    public void test() throws IOException {


        // java.nio.file.InvalidPathException: Illegal char <:> at index 2: /D:/BuildAgent/work/1e5875c1db7235db/target/test-classes/stuck.queue.test/20180508-1249.cq4
        assumeFalse(OS.isWindows());

        Path tmpDir = getTmpDir().toPath();

        expectException("Failback to readonly tablestore");
        ignoreException("reading control code as text");
        expectException("Unexpected field lastAcknowledgedIndexReplicated");
//        expectException("Unable to copy TimedStoreRecovery safely will try anyway");
//        expectException("Unable to copy SCQStore safely will try anyway");
//        expectException("Unable to copy SCQSRoll safely");
//        expectException("Unable to copy SCQSIndexing safely");

        tmpDir.toFile().mkdirs();

        Path templatePath = Paths.get(StuckQueueTest.class.getResource("/stuck.queue.test/20180508-1249.cq4").getFile());
        Path to = tmpDir.resolve(templatePath.getFileName());
        Files.copy(templatePath, to, StandardCopyOption.REPLACE_EXISTING);

        try (RollingChronicleQueue q = ChronicleQueue.singleBuilder(tmpDir).rollCycle(RollCycles.MINUTELY).readOnly(true).build();
             ExcerptTailer tailer = q.createTailer()) {
//            System.out.println(q.dump());

            int cycle = q.rollCycle().toCycle(0x18406e100000000L);

            try (SingleChronicleQueueStore wireStore = q.storeForCycle(cycle, q.epoch(), false, null)) {
                String absolutePath = wireStore.file().getAbsolutePath();
                // System.out.println(absolutePath);
                Assert.assertTrue(absolutePath.endsWith("20180508-1249.cq4"));
            }

            // Assert.assertTrue(tailer.moveToIndex(0x18406e100000000L));

            try (DocumentContext dc = tailer.readingDocument()) {
                // Assert.assertTrue(!dc.isPresent());
                // System.out.println(Long.toHexString(dc.index()));
            }

            // Assert.assertTrue(tailer.moveToIndex(0x183efe300000000L));
            try (final SingleChronicleQueue q2 = ChronicleQueue.singleBuilder(tmpDir).rollCycle(RollCycles.MINUTELY).build()) {
                try (DocumentContext dc = q2.acquireAppender().writingDocument()) {
                    dc.wire().write("hello").text("world");
                }
            }
            ExcerptTailer tailer2 = q.createTailer();
            try (DocumentContext dc = tailer2.readingDocument()) {
                Assert.assertTrue(dc.isPresent());
                String actual = dc.wire().read("hello").text();
                Assert.assertEquals("world", actual);
                // System.out.println(Long.toHexString(dc.index()));
            }
        }
    }
}

