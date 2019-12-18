package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class FileUtilTest extends ChronicleQueueTestBase {

    @Test
    public void stateNonExisting() {
        assumeFalse(OS.isWindows());
        assertEquals(FileState.NON_EXISTENT, FileUtil.state(new File("sjduq867q3jqq3t3q3r")));
    }

    @Test
    public void state() throws IOException {
        assumeFalse(OS.isWindows());
        final Path dir = Files.createTempDirectory("openByAnyProcess");
        try {
            final File testFile =  dir.resolve("tmpFile").toFile();
            Files.write(testFile.toPath(), "A".getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            // The file is created but not open
            assertEquals(FileState.CLOSED, FileUtil.state(testFile));

            try (BufferedReader br = new BufferedReader(new FileReader(testFile))) {
                // The file is now held open
                assertEquals(FileState.OPEN, FileUtil.state(testFile));
            };

            // The file is now released again
            assertEquals(FileState.CLOSED, FileUtil.state(testFile));

        } finally {
            deleteDir(dir.toFile());
        }
    }

    @Test
    public void hasQueueSuffixFalse() {
        final File file = new File("foo");
        assertFalse(FileUtil.hasQueueSuffix(file));
    }

    @Test
    public void hasQueueSuffixTrue() {
        final File file = new File("a" + SingleChronicleQueue.SUFFIX);
        assertTrue(FileUtil.hasQueueSuffix(file));
    }

    @Test
    public void removableQueueFileCandidates(){
        assumeFalse(OS.isWindows());
        final int rolls = 4;
        final int intermediateRolls = rolls / 2;
        final Comparator<File> earliestFirst = comparing(File::getName);

        final SetTimeProvider tp = new SetTimeProvider(0);
        final File tmpDir = getTmpDir();

        try (SingleChronicleQueue queue = builder(tmpDir, WireType.BINARY).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(tp).build()) {
            final ExcerptAppender appender = queue.acquireAppender();
            final ExcerptTailer tailer = queue.createTailer();
            for (int i = 0; i < rolls; i++) {
                appender.writeText(Integer.toString(i)); // to file ...00000iT
                tp.advanceMillis(1000);
            }

            // Allow files to be seen
            Jvm.pause(1000);

            // Force the tailer to open the first file
            tailer.toStart();

            final File[] files = tmpDir.listFiles(FileUtil::hasQueueSuffix);
            assertNotNull(files);
            final List<File> createdFiles = Stream.of(files).sorted(earliestFirst).collect(toList());

            final List<File> candidatesBeforeTailing = FileUtil.removableRollFileCandidates(tmpDir).collect(toList());
            assertSorted(candidatesBeforeTailing, earliestFirst);
            // We have a tailer open but have not read yet -> no files can be removed
            assertEquals(emptyList(), candidatesBeforeTailing);

            for (int i = 0; i < intermediateRolls; i++) {
                final String text = tailer.readText();
                if (text == null) break;
            }

            // Allow files to be closed
            Jvm.pause(1000);

            final List<File> candidatesAfterIntermediateTailing = FileUtil.removableRollFileCandidates(tmpDir).collect(toList());
            assertSorted(candidatesAfterIntermediateTailing, earliestFirst);
            // We have a tailer open and have read `intermediateRolls` -> `intermediateRolls` - 1 files can be removed
            assertEquals(createdFiles.subList(0, intermediateRolls - 1), candidatesAfterIntermediateTailing);

            for (int i = intermediateRolls; i < rolls; i++) {
                final String text = tailer.readText();
                if (text == null) break;
            }

            // Allow files to be closed
            Jvm.pause(1000);

            final List<File> candidatesAfterAllTailing = FileUtil.removableRollFileCandidates(tmpDir).collect(toList());
            assertSorted(candidatesAfterAllTailing, earliestFirst);
            // We have no tailed all the rolls -> `rolls` - 1 files can be removed (because the appender has one open)
            assertEquals(createdFiles.subList(0, rolls - 1), candidatesAfterAllTailing);

        }
    }

    private <T> void assertSorted(List<T> list, Comparator<T> comparator) {
        assertEquals(list.stream().sorted(comparator).collect(toList()), list);
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(RollCycles.TEST4_DAILY).testBlockSize();
    }

    private void documentExamples() {
        final File file = new File("foo");

        if (FileUtil.hasQueueSuffix(file)) {
            //     doStuffWithTheRollFile(file);
        }

        switch (FileUtil.state(file)) {
            case CLOSED: {
                // processClosed(file);
                break;
            }
            case OPEN: {
                // processOpen(file);
                break;
            }
            case NON_EXISTENT: {
                // processNonExistent(file);
                break;
            }
            case UNDETERMINED: {
                // processUndetermined(file);
                break;
            }
        }

    }


}