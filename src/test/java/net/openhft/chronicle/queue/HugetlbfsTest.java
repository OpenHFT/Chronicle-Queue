package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.getHugetlbfsQueueDirectory;
import static net.openhft.chronicle.queue.util.HugetlbfsTestUtil.isHugetlbfsAvailable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class HugetlbfsTest {

    @Rule
    public TestName testName = new TestName();

    @Test
    public void queueHugetlbfsEndToEndSimpleAcceptanceTest() {
        assumeTrue(isHugetlbfsAvailable());
        String path = getHugetlbfsQueueDirectory(testName);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single()
                .path(path)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            appender.writeText("1");
            assertEquals("1", tailer.readText());
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

}
