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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.TableStore;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * TableDirectoryListingReadOnly is a specialized version of TableDirectoryListing intended for read-only queues.
 * It overrides behavior to ensure that read-only queues do not modify cycle metadata or perform any operations
 * that require writing. Instead, the operations such as file creation and rolling cycles are treated as no-ops.
 */
class TableDirectoryListingReadOnly extends TableDirectoryListing {

    /**
     * Constructs a read-only version of TableDirectoryListing.
     *
     * @param tableStore The TableStore that holds the cycle metadata.
     */
    TableDirectoryListingReadOnly(final @NotNull TableStore<?> tableStore) {
        super(tableStore, null, null);  // Passes null for queuePath and fileNameToCycleFunction since they are unused
    }

    /**
     * Overrides the check for read-only mode, as this implementation is inherently read-only.
     *
     * @param tableStore The TableStore to check.
     */
    @Override
    protected void checkReadOnly(@NotNull TableStore<?> tableStore) {
        // no-op, as this class is designed for read-only usage
    }

    /**
     * Initializes the directory listing for read-only queues, ensuring that partially written long values are handled.
     * Retries the initialization for up to 500 milliseconds if it fails due to incomplete writes.
     */
    @Override
    public void init() {
        throwExceptionIfClosedInSetter();

        // Retry logic to handle cases where the long values might be only partially written
        final long timeoutMillis = System.currentTimeMillis() + 500;
        while (true) {
            try {
                initLongValues(); // Initialize LongValues safely
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > timeoutMillis)
                    throw e;  // Timeout exceeded, rethrow exception
                Jvm.pause(1);  // Pause for a short time before retrying
            }
        }
    }

    /**
     * Refreshes the directory listing, but this is a no-op for read-only queues.
     *
     * @param force Whether to force a refresh (ignored in this implementation).
     */
    @Override
    public void refresh(final boolean force) {
        // no-op, as refresh is not needed for read-only queues
    }

    /**
     * Handles file creation in the read-only queue, but this is treated as a no-op.
     *
     * @param file  The created file (ignored in this implementation).
     * @param cycle The cycle associated with the file (ignored in this implementation).
     */
    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);  // Call onRoll, but since onRoll is a no-op, it does nothing
    }

    /**
     * Handles cycle rolling in the read-only queue, but this is treated as a no-op.
     *
     * @param cycle The new cycle (ignored in this implementation).
     */
    @Override
    public void onRoll(int cycle) {
        // no-op, as rolling cycles is not applicable to read-only queues
    }
}
