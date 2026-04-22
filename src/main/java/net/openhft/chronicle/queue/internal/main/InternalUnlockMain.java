/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.TableStoreWriteLock;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.BusyTimedPauser;
import org.jetbrains.annotations.NotNull;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.QUEUE_METADATA_FILE;

/**
 * The InternalUnlockMain class is responsible for unlocking a Chronicle Queue's table store write locks.
 * This is useful in cases where a queue's lock has been left in an inconsistent state and needs to be manually unlocked.
 * The class requires a queue directory as input and operates on the queue metadata file located within that directory.
 */
public final class InternalUnlockMain {
    static {
        SingleChronicleQueueBuilder.addAliases();
    }

    /**
     * Main method to execute the unlocking process.
     *
     * @param args Arguments provided, where the first argument should be the path to the queue directory.
     */
    public static void main(String[] args) {
        // CSPrivilegedOperatorTool REVIEW emit unlock here because this operator-facing diagnostic in InternalUnlockMain#main still needs an explicit reviewed operator-diagnostic contract.
        unlock(args[0]);
    }

    /**
     * Unlocks the queue's metadata file locks located within the provided directory.
     * It forcefully unlocks both the appender lock and the main write lock.
     *
     * @param dir The directory path containing the queue metadata file.
     *            Must be a valid queue directory with a metadata file.
     */
    private static void unlock(@NotNull String dir) {
        // CSPathFromInput REVIEW keep File here because this filesystem boundary in InternalUnlockMain#unlock still needs an explicit reviewed path-handling contract.
        File path = new File(dir);
        if (!path.isDirectory()) {
            // CSStdoutStderrOutput REVIEW System.err.println because this direct console output in InternalUnlockMain#unlock still needs either structured Chronicle diagnostics or an explicit reviewed operator-diagnostic contract.
            System.err.println("Path argument must be a queue directory");
            // CSSystemExitInLibrary REVIEW keep System.exit here because this runtime execution boundary in InternalUnlockMain#unlock still needs an explicit reviewed runtime-admission contract.
            System.exit(1);
        }

        // CSPathFromInput REVIEW keep File here because this filesystem boundary in InternalUnlockMain#unlock still needs an explicit reviewed path-handling contract.
        File storeFilePath = new File(path, QUEUE_METADATA_FILE);

        if (!storeFilePath.exists()) {
            // CSStdoutStderrOutput REVIEW System.err.println because this direct console output in InternalUnlockMain#unlock still needs either structured Chronicle diagnostics or an explicit reviewed operator-diagnostic contract.
            System.err.println("Metadata file not found, nothing to unlock");
            // CSSystemExitInLibrary REVIEW keep System.exit(1); here because this runtime execution boundary in InternalUnlockMain#unlock still needs an explicit reviewed runtime-admission contract.
            System.exit(1);
        }

        final TableStore<?> store = SingleTableBuilder.binary(storeFilePath, Metadata.NoMeta.INSTANCE).readOnly(false).build();

        // appender lock
        // CSForceUnlock REVIEW keep TableStoreWriteLock here because this recovery override in InternalUnlockMain#unlock still needs an explicit reviewed recovery contract.
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L, TableStoreWriteLock.APPEND_LOCK_KEY)).forceUnlock();

        // write lock
        // CSForceUnlock REVIEW keep TableStoreWriteLock here because this recovery override in InternalUnlockMain#unlock still needs an explicit reviewed recovery contract.
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L)).forceUnlock();

        // CSStdoutStderrOutput REVIEW System.out.println because this direct console output in InternalUnlockMain#unlock still needs either structured Chronicle diagnostics or an explicit reviewed operator-diagnostic contract.
        System.out.println("Done");
    }
}
