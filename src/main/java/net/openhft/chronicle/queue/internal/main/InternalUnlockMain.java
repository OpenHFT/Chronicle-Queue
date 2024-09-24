/*
 * Copyright 2014-2020 chronicle.software
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

    // Adds aliases for the SingleChronicleQueueBuilder to support configuration shortcuts
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

        // Validate that the provided path is a directory
        if (!path.isDirectory()) {
            System.err.println("Path argument must be a queue directory");
            System.exit(1);
        }

        // Path to the metadata file
        File storeFilePath = new File(path, QUEUE_METADATA_FILE);

        // Check if the metadata file exists
        if (!storeFilePath.exists()) {
            System.err.println("Metadata file not found, nothing to unlock");
            System.exit(1);
        }

        // Load the table store from the metadata file in read-write mode
        final TableStore<?> store = SingleTableBuilder.binary(storeFilePath, Metadata.NoMeta.INSTANCE)
                                                      .readOnly(false)
                                                      .build();

        // Force unlock the appender lock
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L, TableStoreWriteLock.APPEND_LOCK_KEY)).forceUnlock();

        // Force unlock the main write lock
        (new TableStoreWriteLock(store, BusyTimedPauser::new, 0L)).forceUnlock();

        System.out.println("Done");
    }
}
