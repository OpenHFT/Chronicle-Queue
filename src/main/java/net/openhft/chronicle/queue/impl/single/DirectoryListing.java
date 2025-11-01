/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;

import java.io.File;

/**
 * The {@code DirectoryListing} interface defines the contract for managing and tracking files
 * within a Chronicle Queue directory. Implementations of this interface are responsible for
 * monitoring files, managing cycles, and handling file creation events.
 * <p>
 * It provides methods to refresh the directory, retrieve cycle information, and handle file events
 * associated with the queue's rolling mechanism.
 */
public interface DirectoryListing extends Closeable {

    /**
     * Initializes the directory listing. This method is intended for any setup required after
     * the implementation is instantiated. The default implementation does nothing.
     */
    default void init() {
    }

    /**
     * Refreshes the state of the directory listing, optionally forcing the refresh.
     * This method updates the internal state of the directory listing, checking for any changes
     * to the files and cycles managed by the queue.
     *
     * @param force If true, forces a refresh even if the conditions for an automatic refresh are not met.
     */
    void refresh(boolean force);

    /**
     * Returns the timestamp of the last refresh operation.
     *
     * @return The time of the last refresh in milliseconds since the epoch.
     */
    long lastRefreshTimeMS();

    /**
     * Called when a new file is created in the directory.
     * This method is invoked when a new file corresponding to a specific cycle is detected.
     *
     * @param file  The newly created file.
     * @param cycle The cycle number associated with the file.
     */
    void onFileCreated(File file, int cycle);

    /**
     * Returns the minimum cycle number created so far.
     *
     * @return The minimum cycle number.
     */
    int getMinCreatedCycle();

    /**
     * Returns the maximum cycle number created so far.
     *
     * @return The maximum cycle number.
     */
    int getMaxCreatedCycle();

    /**
     * Returns the modification count, which represents the number of changes made to the directory
     * listing since it was last refreshed.
     *
     * @return The modification count.
     */
    long modCount();

    /**
     * Called when a roll occurs, indicating that the cycle has changed.
     * This method is invoked to signal that a roll to a new cycle has been completed.
     *
     * @param cycle The new cycle number.
     */
    void onRoll(int cycle);
}
