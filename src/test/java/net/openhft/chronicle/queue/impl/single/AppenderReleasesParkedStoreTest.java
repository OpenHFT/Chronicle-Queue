/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.util.FileUtil;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static net.openhft.chronicle.queue.internal.util.InternalFileUtil.getAllOpenFilesIsSupportedOnOS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies through {@link FileUtil#removableRollFileCandidates} that an appender does not keep an old
 * roll-cycle file open once it has rolled into the past. Removal stops at the first still-open file
 * (oldest first), so a pinned old file blocks everything after it.
 * <p>
 * These tests inspect open descriptors via {@code /proc/self/fd} and are skipped where unsupported.
 */
class AppenderReleasesParkedStoreTest extends IndexingTestCommon {

    private static final Comparator<File> EARLIEST_FIRST = Comparator.comparing(File::getName);

    // Control: with only the writer active, the current cycle stays open and earlier cycles are removable.
    @Test
    void writerAloneLeavesEveryEarlierCycleRemovable() {
        assumeTrue(getAllOpenFilesIsSupportedOnOS());

        appender.writeText("cycle-0");
        timeProvider.advanceMillis(1_001);
        appender.writeText("cycle-1");
        timeProvider.advanceMillis(1_001);
        appender.writeText("cycle-2");

        final List<File> created = cycleFiles();
        assertEquals(3, created.size(), "expected three roll-cycle files, got " + created);
        assertEquals(created.subList(0, 2), removableCandidates());
    }

    // A fresh appender parks on the current cycle at construction and never writes; after the writer rolls
    // past it, it must not keep the now-old file open.
    @Test
    void parkedAppenderDoesNotPinAnOldCycle() {
        assumeTrue(getAllOpenFilesIsSupportedOnOS());

        appender.writeText("cycle-0");
        timeProvider.advanceMillis(1_001);
        appender.writeText("cycle-1");

        final StoreAppender parked = (StoreAppender) queue.createAppender();
        try {
            timeProvider.advanceMillis(1_001);
            appender.writeText("cycle-2");

            final List<File> created = cycleFiles();
            assertEquals(3, created.size(), "expected three roll-cycle files, got " + created);
            assertEquals(created.subList(0, 2), removableCandidates());
        } finally {
            parked.close();
        }
    }

    // All roll-cycle files, earliest first.
    private List<File> cycleFiles() {
        final File[] files = queue.file().listFiles(FileUtil::hasQueueSuffix);
        assertNotNull(files, "no queue files found in " + queue.file());
        return Stream.of(files).sorted(EARLIEST_FIRST).collect(toList());
    }

    // Removable candidates, earliest first. The parked store's file descriptor is dropped on a
    // background thread, so drain the releaser before inspecting open descriptors.
    private List<File> removableCandidates() {
        BackgroundResourceReleaser.releasePendingResources();
        final List<File> candidates = FileUtil.removableRollFileCandidates(queue.file()).collect(toList());
        assertEquals(candidates.stream().sorted(EARLIEST_FIRST).collect(toList()), candidates);
        return candidates;
    }
}
