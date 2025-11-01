/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.TableStore;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * TableDirectoryListingReadOnly is a specialized version of TableDirectoryListing intended for read-only queues.
 * It overrides behavior to ensure that read-only queues do not modify cycle metadata or perform any operations
 * that require writing. Instead, the operations such as file creation and rolling cycles are treated as no-ops.
 */
class TableDirectoryListingReadOnly extends TableDirectoryListing {

    TableDirectoryListingReadOnly(final @NotNull TableStore<?> tableStore,
                                  final TimeProvider time) {
        super(tableStore, null, null, time);
    }

    /**
     * Overrides the check for read-only mode, as this implementation is inherently read-only.
     *
     * @param tableStore The TableStore to check.
     */
    @Override
    protected void checkReadOnly(@NotNull TableStore<?> tableStore) {
        // no-op
    }

    /**
     * Initializes the directory listing for read-only queues, ensuring that partially written long values are handled.
     * Retries the initialization for up to 500 milliseconds if it fails due to incomplete writes.
     */
    @Override
    public void init() {
        throwExceptionIfClosedInSetter();

        // it is possible if r/o queue created at same time as r/w queue for longValues to be only half-written
        final long timeoutMillis = System.currentTimeMillis() + 500;
        while (true) {
            try {
                initLongValues();
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() > timeoutMillis)
                    throw e;
                Jvm.pause(1);
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
        // no-op
    }

    /**
     * Handles file creation in the read-only queue, but this is treated as a no-op.
     *
     * @param file  The created file (ignored in this implementation).
     * @param cycle The cycle associated with the file (ignored in this implementation).
     */
    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);
    }

    /**
     * Handles cycle rolling in the read-only queue, but this is treated as a no-op.
     *
     * @param cycle The new cycle (ignored in this implementation).
     */
    @Override
    public void onRoll(int cycle) {
        // no-op
    }
}
