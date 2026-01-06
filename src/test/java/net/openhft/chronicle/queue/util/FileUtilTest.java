/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.*;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static net.openhft.chronicle.queue.internal.util.InternalFileUtil.getAllOpenFilesIsSupportedOnOS;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SuppressWarnings({"deprecation", "removal"})
public class FileUtilTest extends QueueTestCommon {

    @Test
    @DisplayName("File state reports non-existent for missing file")
    @Timeout(30)
    public void stateNonExisting() {
        assumeTrue(getAllOpenFilesIsSupportedOnOS(),
                "Open file listing is not supported on this OS for non-existing file test");
        assertEquals(FileState.NON_EXISTENT, FileUtil.state(new File("sjduq867q3jqq3t3q3r")),
                "FileUtil.state() should return NON_EXISTENT for non-existing file path");
    }

    @Test
    @DisplayName("File state reports open and closed correctly")
    @Timeout(30)
    public void state() throws IOException {
        assumeTrue(getAllOpenFilesIsSupportedOnOS(),
                "Open file listing is not supported on this OS for open/closed state test");
        final Path dir = IOTools.createTempDirectory("openByAnyProcess");
        dir.toFile().mkdir();
        try {
            final File testFile = dir.resolve("tmpFile").toFile();
            RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
            raf.setLength(PageUtil.getPageSize(dir.toString()));
            raf.close();

            // Allow things to stabilize
            Jvm.pause(100);

            // The file is created but not open
            assertEquals(FileState.CLOSED, FileUtil.state(testFile),
                    "FileUtil.state() should return CLOSED for a created file that has been released");

            try (BufferedReader br = new BufferedReader(new FileReader(testFile))) {
                assertNotNull(br, "BufferedReader should be successfully created for test file");
                // The file is now held open
                assertEquals(FileState.OPEN, FileUtil.state(testFile),
                        "FileUtil.state() should return OPEN when file is actively held by BufferedReader");
            }

            // The file is now released again
            assertEquals(FileState.CLOSED, FileUtil.state(testFile),
                    "FileUtil.state() should return CLOSED after BufferedReader is closed and file handle is released");

        } finally {
            IOTools.deleteDirWithFiles(dir.toFile());
        }
    }

    @Test
    @DisplayName("File state reports non-existent on Windows when tracing disabled")
    public void stateWindows() {
        assumeTrue(OS.isWindows(), "Windows-only test for FileUtil.state");

        expectException("closable tracing disabled");
        AbstractCloseable.disableCloseableTracing();

        FileState foo = FileUtil.state(new File("foo"));
        assertEquals(FileState.NON_EXISTENT, foo,
                "FileUtil.state() should return NON_EXISTENT on Windows when closeable tracing is disabled");
    }

    @Test
    @DisplayName("Queue suffix check returns false for other files")
    @Timeout(30)
    public void hasQueueSuffixFalse() {
        final File file = new File("foo");
        assertFalse(FileUtil.hasQueueSuffix(file),
                "FileUtil.hasQueueSuffix() should return false for file without Chronicle Queue suffix");
    }

    @Test
    @DisplayName("Queue suffix check returns true for .cq4 queue files")
    @Timeout(30)
    public void hasQueueSuffixTrue() {
        final File file = new File("a" + SingleChronicleQueue.SUFFIX);
        assertTrue(FileUtil.hasQueueSuffix(file),
                "FileUtil.hasQueueSuffix() should return true for file with Chronicle Queue suffix (.cq4)");
    }

    @Test
    @DisplayName("Removable roll file candidates follow tailer progress")
    @Timeout(30)
    public void removableQueueFileCandidates() {
        assumeTrue(getAllOpenFilesIsSupportedOnOS(),
                "Open file listing is not supported on this OS for removable roll candidates test");
        final int rolls = 4;
        final int intermediateRolls = rolls / 2;
        final Comparator<File> earliestFirst = comparing(File::getName);

        final SetTimeProvider tp = new SetTimeProvider(0);
        final File tmpDir = getTmpDir();

        try (SingleChronicleQueue queue = builder(tmpDir, WireType.BINARY).rollCycle(TEST_SECONDLY).timeProvider(tp).build();
             final ExcerptAppender appender = queue.createAppender()) {
            final ExcerptTailer tailer = queue.createTailer();
            for (int i = 0; i < rolls; i++) {
                appender.writeText(Integer.toString(i)); // to file ...00000iT
                tp.advanceMillis(1000);
            }

            // Allow files to be seen
            Jvm.pause(300);

            // Force the tailer to open the first file
            tailer.toStart();

            final File[] files = tmpDir.listFiles(FileUtil::hasQueueSuffix);
            assertNotNull(files, "listFiles() should return non-null array of queue files from temp directory");
            final List<File> createdFiles = Stream.of(files).sorted(earliestFirst).collect(toList());

            final List<File> candidatesBeforeTailing = FileUtil.removableRollFileCandidates(tmpDir).collect(toList());
            assertSorted(candidatesBeforeTailing, earliestFirst,
                    "Candidates before tailing should be sorted by earliest file name");
            // We have a tailer open but have not read yet -> no files can be removed
            assertEquals(emptyList(), candidatesBeforeTailing,
                    "removableRollFileCandidates() should return empty list when tailer is at start position and has not read any files");

            for (int i = 0; i < intermediateRolls; i++) {
                final String text = tailer.readText();
                if (text == null) break;
            }

            // Allow files to be closed
            Jvm.pause(1000);

            final List<File> candidatesAfterIntermediateTailing = FileUtil.removableRollFileCandidates(tmpDir).collect(toList());
            assertSorted(candidatesAfterIntermediateTailing, earliestFirst,
                    "Candidates after intermediate tailing should be sorted by earliest file name");
            // We have a tailer open and have read `intermediateRolls` -> `intermediateRolls` - 1 files can be removed
            assertEquals(createdFiles.subList(0, intermediateRolls - 1), candidatesAfterIntermediateTailing,
                    "removableRollFileCandidates() should return (intermediateRolls - 1) files after tailer reads through half the rolls, keeping current file");

            for (int i = intermediateRolls; i < rolls; i++) {
                final String text = tailer.readText();
                if (text == null) break;
            }

            // Allow files to be closed
            Jvm.pause(1000);

            final List<File> candidatesAfterAllTailing = FileUtil.removableRollFileCandidates(tmpDir).collect(toList());
            assertSorted(candidatesAfterAllTailing, earliestFirst,
                    "Candidates after full tailing should be sorted by earliest file name");
            // We have no tailed all the rolls -> `rolls` - 1 files can be removed (because the appender has one open)
            assertEquals(createdFiles.subList(0, rolls - 1), candidatesAfterAllTailing,
                    "removableRollFileCandidates() should return (rolls - 1) files after tailer reads all rolls, excluding file currently held by appender");

        }
    }

    @Test
    @DisplayName("Removable roll file candidates are unsupported on Windows")
    @Timeout(30)
    public void removableQueueFileCandidatesWindows() {
        assumeTrue(OS.isWindows(), "Windows-only test for removable roll file candidates");
        expectException("closable tracing disabled");
        AbstractCloseable.disableCloseableTracing();
        assertThrows(UnsupportedOperationException.class, () -> FileUtil.removableRollFileCandidates(new File("foo")),
                "removableRollFileCandidates() should throw UnsupportedOperationException on Windows platform");
    }

    private <T> void assertSorted(List<T> list, Comparator<T> comparator, String message) {
        assertEquals(list.stream().sorted(comparator).collect(toList()), list, message);
    }

    @NotNull
    private SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(TEST4_DAILY).testBlockSize();
    }

    @Test
    @DisplayName("Open files include current process ID")
    public void testOpenFilesWithPid() throws IOException {
        assumeTrue(getAllOpenFilesIsSupportedOnOS(),
                "Open file listing is not supported on this OS for open files with PID test");

        // open file for writing, keeping file handle open
        File temporaryFile = new File(OS.getTmp(), "testOpenFilesWithPid.txt-" + System.nanoTime());
        FileWriter fstream = new FileWriter(temporaryFile);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("somedata");

        Map<String, String> filesWithPid = FileUtil.getAllOpenFiles();
        assertEquals(Integer.toString(Jvm.getProcessId()), filesWithPid.get(temporaryFile.getAbsolutePath()),
                "getAllOpenFiles() should map the temporary file path to the current process ID while file is held open");

        // close file
        out.close();

        filesWithPid = FileUtil.getAllOpenFiles();
        assertFalse(filesWithPid.containsKey(temporaryFile.getAbsolutePath()),
                "getAllOpenFiles() should not contain the temporary file path after file has been closed and handle released");
    }
}
