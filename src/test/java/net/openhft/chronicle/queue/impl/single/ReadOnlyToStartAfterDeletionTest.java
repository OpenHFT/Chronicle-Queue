/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class ReadOnlyToStartAfterDeletionTest extends QueueTestCommon {
    @Test
    public void healthyReadOnlyToStartDoesNotScanOrWrite() throws Exception {
        assumeFalse(OS.isWindows());
        Fixture f = fixture();
        Map<String, String> before = snapshot(f.directory);
        try (SingleChronicleQueue queue = builder(f, true).build(); ExcerptTailer tailer = queue.createTailer()) {
            assertTrue(queue.isReadOnly());
            f.directory.listCalls = 0;
            for (int i = 0; i < 3; i++) {
                assertEquals(f.indexes[0], tailer.toStart().index());
                assertEquals("entry-0", tailer.readText());
            }
            assertEquals("healthy positioning needs no directory snapshot", 0, f.directory.listCalls);
            assertEquals(before, snapshot(f.directory));
        }
        BackgroundResourceReleaser.releasePendingResources();
        assertEquals("close must not write", before, snapshot(f.directory));
    }

    @Test
    public void deletedHistoricalFirstRollIsSkippedWithoutWriting() throws Exception {
        verifyHistoricalDeletion(1);
    }

    @Test
    public void multipleDeletedHistoricalRollsUseOneSnapshot() throws Exception {
        verifyHistoricalDeletion(2);
    }

    @Test
    public void disappearingSnapshotBoundaryDoesNotRetryWithinTheSameCall() throws Exception {
        assumeFalse(OS.isWindows());
        Fixture f = fixture();
        try (SingleChronicleQueue queue = builder(f, true).build(); ExcerptTailer tailer = queue.createTailer()) {
            assertEquals(f.indexes[0], tailer.toStart().index());
            Files.delete(f.rolls.get(0));
            f.directory.listCalls = 0;
            f.directory.afterNextList = () -> {
                try {
                    // A second actor removes the next historical roll after the snapshot was taken.
                    Files.delete(f.rolls.get(1));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            };
            try {
                tailer.toStart();
                fail("a disappeared snapshot boundary must end this bounded attempt");
            } catch (MissingStoreFileException expected) {
                assertEquals("no scan/retry loop", 1, f.directory.listCalls);
            }
            Map<String, String> survivors = snapshot(f.directory);
            assertEquals(f.indexes[2], tailer.toStart().index());
            assertEquals("entry-2", tailer.readText());
            assertEquals(survivors, snapshot(f.directory));
        }
    }

    private void verifyHistoricalDeletion(int deletedRolls) throws Exception {
        assumeFalse(OS.isWindows());
        Fixture f = fixture();
        Map<String, String> beforeOpen = snapshot(f.directory);
        Map<String, String> survivors;
        try (SingleChronicleQueue queue = builder(f, true).build(); ExcerptTailer tailer = queue.createTailer()) {
            assertTrue(queue.isReadOnly());
            assertEquals(f.indexes[0], tailer.toStart().index());
            assertEquals(beforeOpen, snapshot(f.directory));
            for (int i = 0; i < deletedRolls; i++)
                Files.delete(f.rolls.get(i));
            survivors = snapshot(f.directory);

            //! The writer is closed before the read-only queue opens. No writable publisher refreshes its table minimum.
            //! Keep the final two rolls: this control does not extend the supported current/sole-roll deletion policy.
            for (int attempt = 0; attempt < 3; attempt++) {
                f.directory.listCalls = 0;
                assertEquals(f.indexes[deletedRolls], tailer.toStart().index());
                assertEquals("one directory snapshot per missing-boundary recovery", 1, f.directory.listCalls);
                for (int i = deletedRolls; i < f.indexes.length; i++) {
                    assertEquals("entry-" + i, tailer.readText());
                    assertEquals(f.indexes[i], tailer.lastReadIndex());
                }
                assertNull(tailer.readText());
                assertEquals(f.indexes[f.indexes.length - 1] + 1, tailer.toEnd().index());
                assertFalse(tailer.moveToIndex(f.indexes[0]));
                assertEquals("names, bytes, lengths and mtimes must survive", survivors, snapshot(f.directory));
            }
        }
        BackgroundResourceReleaser.releasePendingResources();
        assertEquals("close must not write", survivors, snapshot(f.directory));
    }

    private Fixture fixture() throws Exception {
        Fixture f = new Fixture(new CountingDirectory(getTmpDir()));
        try (SingleChronicleQueue queue = builder(f, false).build(); ExcerptAppender appender = queue.createAppender()) {
            for (int i = 0; i < f.indexes.length; i++) {
                appender.writeText("entry-" + i);
                f.indexes[i] = appender.lastIndexAppended();
                f.time.advanceMillis(RollCycles.FAST_DAILY.lengthInMillis());
            }
        }
        BackgroundResourceReleaser.releasePendingResources();
        try (Stream<Path> files = Files.list(f.directory.toPath())) {
            f.rolls = files.filter(p -> p.toString().endsWith(SingleChronicleQueue.SUFFIX))
                    .sorted().collect(Collectors.toList());
        }
        assertEquals(f.indexes.length, f.rolls.size());
        return f;
    }

    private static SingleChronicleQueueBuilder builder(Fixture f, boolean readOnly) {
        return SingleChronicleQueueBuilder.binary(f.directory).rollCycle(RollCycles.FAST_DAILY)
                .indexCount(8).indexSpacing(1).testBlockSize().timeProvider(f.time).readOnly(readOnly);
    }

    private static Map<String, String> snapshot(File directory) throws Exception {
        Map<String, String> result = new TreeMap<>();
        try (Stream<Path> files = Files.list(directory.toPath())) {
            for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                assertTrue("bounded fixture", Files.size(file) <= 64L * 1024 * 1024);
                byte[] bytes = Files.readAllBytes(file);
                byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
                result.put(file.getFileName().toString(), bytes.length + ":" + Files.getLastModifiedTime(file)
                        + ":" + Arrays.toString(hash));
            }
        }
        return result;
    }

    private static final class Fixture {
        final CountingDirectory directory;
        final SetTimeProvider time = new SetTimeProvider(0);
        final long[] indexes = new long[4];
        List<Path> rolls;

        Fixture(CountingDirectory directory) {
            this.directory = directory;
        }
    }

    private static final class CountingDirectory extends File {
        private static final long serialVersionUID = 1L;
        int listCalls;
        Runnable afterNextList;

        CountingDirectory(File directory) {
            super(directory.getPath());
        }

        @Override
        public String[] list() {
            listCalls++;
            String[] snapshot = super.list();
            if (afterNextList != null) {
                Runnable action = afterNextList;
                afterNextList = null;
                action.run();
            }
            return snapshot;
        }
    }
}
