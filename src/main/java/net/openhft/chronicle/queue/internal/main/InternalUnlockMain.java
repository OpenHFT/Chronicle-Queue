/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
        File path = new File(dir);
        if (!path.isDirectory()) {
            System.err.println("Path argument must be a queue directory");
            System.exit(1);
        }

        File storeFilePath = new File(path, QUEUE_METADATA_FILE);

        if (!storeFilePath.exists()) {
            System.err.println("Metadata file not found, nothing to unlock");
            System.exit(1);
        }

        final TableStore<?> store = SingleTableBuilder.binary(storeFilePath, Metadata.NoMeta.INSTANCE).readOnly(false).build();

        // appender lock
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L, TableStoreWriteLock.APPEND_LOCK_KEY)).forceUnlock();

        // write lock
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L)).forceUnlock();

        System.out.println("Done");
    }
}
