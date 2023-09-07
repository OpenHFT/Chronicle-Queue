package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class EmptyRollCycleInMiddleTest extends QueueTestCommon {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() {
        // Only required when ported to newer versions
        // expectException("deprecated RollCycle");
    }

    /**
     * A empty middle cycle files with missing EOF markers. The critical part of this issue reproduction
     * is to have <strong>multiple</strong> empty queue files in the stream, not just one, all missing EOFs. 
     * In this case it seems that EOF markers are not added to the intermediate cycle files.
     */
    @Test
    public void emptyRollCycleInMiddleWithMissingEOFs() {
        createQueueFilesWithEmptyRollCycleInTheMiddle(queuePath());
        try (SingleChronicleQueue queue = queue(queuePath()); ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < 2; i++) {
                try (DocumentContext context = tailer.readingDocument()) {
                    assertTrue("Expected there to be a document context at position " + i, context.isPresent());
                }
            }
        }
    }

    /**
     * Will create N queue files. Due to the pretoucher some intermediate files will be missing EOF markers and have
     * no data.
     */
    private void createQueueFilesWithEmptyRollCycleInTheMiddle(Path queuePath) {
        SetTimeProvider timeProvider = new SetTimeProvider();

        try (SingleChronicleQueue queue = queue(queuePath, timeProvider)) {
            try (ExcerptAppender appender = queue.acquireAppender()) {

                // Write something to the first file
                writeData(appender, "text_0");

                timeProvider.advanceMillis(1_000);

                touch(appender);

                timeProvider.advanceMillis(1_000);

                touch(appender);

                timeProvider.advanceMillis(1_000);

                // Write to the next file
                writeData(appender, "text_1");
            }
        }
    }

    private void writeData(ExcerptAppender appender, String message) {
        try (DocumentContext context = appender.writingDocument()) {
            context.wire().writeText(message);
        }
    }

    private void touch(ExcerptAppender appender) {
        appender.pretouch();
    }

    private SingleChronicleQueue queue(Path queuePath) {
        return queue(queuePath, null);
    }

    private SingleChronicleQueue queue(Path queuePath, TimeProvider timeProvider) {
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.builder();

        if (timeProvider != null) {
            builder.timeProvider(timeProvider);
        }

        return builder
                .path(queuePath)
                .rollCycle(RollCycles.TEST_SECONDLY)
                .build();
    }

    private Path queuePath() {
        return Paths.get(temporaryFolder.getRoot().toString(), testName.getMethodName());
    }

    private Set<String> listFiles(String dir) {
        return Stream.of(Objects.requireNonNull(new File(dir).listFiles())).filter(file -> !file.isDirectory()).map(File::getName).collect(Collectors.toSet());
    }


}
