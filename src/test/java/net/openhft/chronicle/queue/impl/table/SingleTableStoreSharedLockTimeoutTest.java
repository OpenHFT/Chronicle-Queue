/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SingleTableStoreSharedLockTimeoutTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void callerCanSupplySharedLockTimeout() throws Exception {
        File file = temporaryFolder.newFile("metadata.cq4t");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(1);
        }

        String value = SingleTableStore.doWithSharedLock(file, 0,
                ignored -> "locked", () -> null);

        assertEquals("locked", value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeSharedLockTimeoutIsRejected() throws Exception {
        File file = temporaryFolder.newFile("metadata.cq4t");
        SingleTableStore.doWithSharedLock(file, -1,
                ignored -> "unreachable", () -> null);
    }

    @Test
    public void zeroTimeoutPerformsOneAttemptWhenTheStructuralLockIsHeld() throws Exception {
        File file = temporaryFolder.newFile("locked-metadata.cq4t");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(1);
            try (FileLock lock = randomAccessFile.getChannel()
                    .lock(Long.MAX_VALUE - 1, 1, false)) {
                assertTrue(lock.isValid());
                try {
                    SingleTableStore.doWithSharedLock(file, 0,
                            value -> "unreachable", () -> null);
                    fail("shared lock should not have been acquired");
                } catch (IllegalStateException expected) {
                    // A zero timeout still performs one non-blocking attempt.
                }
            }
        }
    }
}
