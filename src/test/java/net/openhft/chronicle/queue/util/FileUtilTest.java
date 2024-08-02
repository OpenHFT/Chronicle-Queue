/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import net.openhft.chronicle.queue.internal.util.InternalFileUtil;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

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
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class FileUtilTest extends QueueTestCommon {

    @Test(timeout = 30_000)
    public void stateNonExisting() {
        assumeTrue(getAllOpenFilesIsSupportedOnOS());
        assertEquals(FileState.NON_EXISTENT, FileUtil.state(new File("sjduq867q3jqq3t3q3r")));
    }

    @Test(timeout = 30_000)
    public void state() throws IOException {
        assumeTrue(getAllOpenFilesIsSupportedOnOS());
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
            assertEquals(FileState.CLOSED, FileUtil.state(testFile));

            try (BufferedReader br = new BufferedReader(new FileReader(testFile))) {
                assertNotNull(br);
                // The file is now held open
                assertEquals(FileState.OPEN, FileUtil.state(testFile));
            }

            // The file is now released again
            assertEquals(FileState.CLOSED, FileUtil.state(testFile));

        } finally {
            IOTools.deleteDirWithFiles(dir.toFile());
        }
    }

    @Test(expected = UnsupportedOperationException.class, timeout = 30_000)
    public void stateWindows() {
        assumeTrue(OS.isWindows());

        expectException("closable tracing disabled");
        AbstractCloseable.disableCloseableTracing();

        FileUtil.state(new File("foo"));
    }

    @Test(timeout = 30_000)
    public void hasQueueSuffixFalse() {
        final File file = new File("foo");
        assertFalse(FileUtil.hasQueueSuffix(file));
    }

    @Test(timeout = 30_000)
    public void hasQueueSuffixTrue() {
        final File file = new File("a" + SingleChronicleQueue.SUFFIX);
        assertTrue(FileUtil.hasQueueSuffix(file));
    }

    @Test(timeout = 30_000)
    public void removableQueueFileCandidates() {
        assumeTrue(getAllOpenFilesIsSupportedOnOS());
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

    @Test(expected = UnsupportedOperationException.class, timeout = 30_000)
    public void removableQueueFileCandidatesWindows() {
        assumeTrue(OS.isWindows());
        expectException("closable tracing disabled");
        AbstractCloseable.disableCloseableTracing();
        FileUtil.removableRollFileCandidates(new File("foo"));
    }

    private <T> void assertSorted(List<T> list, Comparator<T> comparator) {
        assertEquals(list.stream().sorted(comparator).collect(toList()), list);
    }

    @NotNull
    protected SingleChronicleQueueBuilder builder(@NotNull File file, @NotNull WireType wireType) {
        return SingleChronicleQueueBuilder.builder(file, wireType).rollCycle(TEST4_DAILY).testBlockSize();
    }

    @Test
    public void testOpenFilesWithPid() throws IOException {
        assumeTrue(getAllOpenFilesIsSupportedOnOS());

        // open file for writing, keeping file handle open
        File temporaryFile = IOTools.createTempFile("testOpenFilesWithPid.txt");
        FileWriter fstream = new FileWriter(temporaryFile);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("somedata");

        Map<String, String> filesWithPid = FileUtil.getAllOpenFiles();
        assertEquals(Integer.toString(Jvm.getProcessId()), filesWithPid.get(temporaryFile.getAbsolutePath()));

        // close file
        out.close();

        filesWithPid = FileUtil.getAllOpenFiles();
        assertFalse(filesWithPid.keySet().contains(temporaryFile.getAbsolutePath()));
    }
}
