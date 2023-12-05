package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NegativeEpochTest extends QueueTestCommon {

    @Test
    public void negativeEpoch() {
        File queuePath = getTmpDir();
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .path(queuePath)
                .timeProvider(timeProvider)
                .rollCycle(TestRollCycles.TEST_SECONDLY)
                .epoch(-500)
                .build();
             ExcerptAppender appender = queue.createAppender()) {

            appender.writeText("test1");
            assertEquals(1, listCycles(queuePath).size());

            timeProvider.advanceMillis(499);
            appender.writeText("test2");
            assertEquals(1, listCycles(queuePath).size());

            timeProvider.advanceMillis(1);
            appender.writeText("test3");
            assertEquals(2, listCycles(queuePath).size());

        } finally {
            IOTools.deleteDirWithFilesOrThrow(queuePath);
        }
    }

    public Set<String> listCycles(File queuePath) {
        return Stream.of(queuePath.listFiles())
                .filter(file -> file.getName().endsWith(".cq4"))
                .map(File::getName)
                .collect(Collectors.toSet());
    }

}
