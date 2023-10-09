package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class SkewedRollCycleTest extends QueueTestCommon {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    private Path queuePath;

    private SetTimeProvider timeProvider;

    private SingleChronicleQueue queue;

    private ExcerptAppender appender;

    @Before
    public void before() {
        queuePath = Paths.get(temporaryFolder.getRoot().toString(), testName.getMethodName());
        timeProvider = new SetTimeProvider();
        queue = SingleChronicleQueueBuilder.builder()
                .path(queuePath)
                .rollCycle(TestRollCycles.TEST_SECONDLY)
                .timeProvider(timeProvider)
                .epoch(-100)
                .build();
        appender = queue.createAppender();
    }

    @After
    public void after() {
        appender.close();
        queue.close();
        IOTools.deleteDirWithFiles(temporaryFolder.getRoot().toString());
    }

    @Test
    public void skewEarly() {
        appender.writeText("test1");
        assertEquals(1, listQueueFiles().size());
        timeProvider.advanceMillis(900);
        appender.writeText("test2");
        List<String> queueFiles = listQueueFiles();
        assertEquals(2, queueFiles.size());
    }

    private List<String> listQueueFiles() {
        return Stream.of(queuePath.toFile().listFiles())
                .filter(file -> file.getName().endsWith("cq4"))
                .map(File::getName)
                .collect(Collectors.toList());
    }

}
