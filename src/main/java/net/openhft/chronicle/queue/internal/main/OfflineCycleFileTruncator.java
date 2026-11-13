/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.core.Jvm;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Explicit offline maintenance only; never called by Queue appending, rolling or closing.
 * All users/mappings must be closed and new opens prevented throughout mutation.
 * The file lock alone excludes cooperating maintenance tools, not Queue writers.
 */
final class OfflineCycleFileTruncator {
    enum Outcome { NO_EOF, SKIPPED_NONZERO_TAIL, OFFLINE_CONFIRMATION_REQUIRED, ALREADY_TRIMMED, TRUNCATED }

    private OfflineCycleFileTruncator() {
    }

    /**
     * Inspects one existing cycle file and optionally removes unused bytes following EOF.
     *
     * @param file an existing, little-endian, dataFormat 1 Queue cycle
     * @param confirmedOffline caller confirmation that every user/mapping is closed and new opens are prevented
     * @return the maintenance outcome; absence of EOF is normal for a live cycle
     * @throws IOException if inspection or truncation fails; the file is never created or extended
     */
    static Outcome truncate(Path file, boolean confirmedOffline) throws IOException {
        return truncate(file, confirmedOffline, CycleFileIdentity.capture(file));
    }

    static Outcome truncate(Path file, boolean confirmedOffline, CycleFileIdentity expected) throws IOException {
        return truncate(file, confirmedOffline, expected, java.nio.file.Paths.get("/proc"));
    }

    static Outcome truncate(Path file, boolean confirmedOffline, CycleFileIdentity expected, Path procRoot) throws IOException {
        expected.check(file);
        OfflineFileUsers.requireClosed(file, procRoot);
        if (!java.nio.file.Files.isWritable(file))
            throw new IOException("REFUSED_NOT_WRITABLE: " + file);
        if (!confirmedOffline) {
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                expected.check(file);
                long end = CycleFileEnd.find(channel);
                if (end == CycleFileEnd.NONZERO_TAIL)
                    return Outcome.SKIPPED_NONZERO_TAIL;
                if (end < 0)
                    return noEof(file);
                return Outcome.OFFLINE_CONFIRMATION_REQUIRED;
            }
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE, java.nio.file.LinkOption.NOFOLLOW_LINKS);
             FileLock lock = channel.tryLock()) {
            if (lock == null || !lock.isValid())
                throw new IOException("Maintenance lock unavailable: " + file);
            expected.check(file);
            long size = channel.size();
            long end = CycleFileEnd.find(channel);
            if (end == CycleFileEnd.NONZERO_TAIL)
                return Outcome.SKIPPED_NONZERO_TAIL;
            if (end < 0)
                return noEof(file);
            expected.check(file);
            if (channel.size() != size || CycleFileEnd.find(channel) != end)
                throw new IOException("Cycle file changed during offline inspection: " + file);
            if (size == end)
                return Outcome.ALREADY_TRIMMED;
            expected.check(file);
            channel.truncate(end);
            channel.force(true);
            return Outcome.TRUNCATED;
        } catch (java.nio.channels.OverlappingFileLockException e) {
            throw new IOException("REFUSED_LOCK_OVERLAP: " + file, e);
        }
    }

    private static Outcome noEof(Path file) {
        if (Jvm.isDebugEnabled(OfflineCycleFileTruncator.class))
            Jvm.debug().on(OfflineCycleFileTruncator.class, "Not truncating cycle file " + file
                    + ": no EOF marker at the logical end; truncation is deferred until EOF is present"
                    + " because a writer may still be active.");
        return Outcome.NO_EOF;
    }

}
