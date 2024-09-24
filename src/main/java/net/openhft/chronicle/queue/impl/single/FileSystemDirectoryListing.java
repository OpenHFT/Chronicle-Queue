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

import net.openhft.chronicle.core.io.SimpleCloseable;

import java.io.File;
import java.util.function.ToIntFunction;

import static net.openhft.chronicle.queue.impl.single.TableDirectoryListing.*;

/**
 * The {@code FileSystemDirectoryListing} class is responsible for managing the listing of files
 * in a queue directory on the file system. It tracks the minimum and maximum cycle numbers
 * of created files and provides methods for refreshing the directory state and handling file
 * creation events.
 * <p>
 * This class extends {@link SimpleCloseable} and implements {@link DirectoryListing}, allowing
 * it to manage resources and handle cleanup when closed.
 */
final class FileSystemDirectoryListing extends SimpleCloseable implements DirectoryListing {
    private final File queueDir; // The directory containing the queue files
    private final ToIntFunction<String> fileNameToCycleFunction; // Function to extract cycle number from filenames
    private int minCreatedCycle = Integer.MAX_VALUE; // Minimum cycle number of created files
    private int maxCreatedCycle = Integer.MIN_VALUE; // Maximum cycle number of created files
    private long lastRefreshTimeMS; // The timestamp of the last directory refresh

    /**
     * Constructs a {@code FileSystemDirectoryListing} with the specified directory and function
     * for determining cycle numbers from filenames.
     *
     * @param queueDir                The directory containing the queue files.
     * @param fileNameToCycleFunction A function that converts a filename to a cycle number.
     */
    FileSystemDirectoryListing(final File queueDir,
                               final ToIntFunction<String> fileNameToCycleFunction) {
        this.queueDir = queueDir;
        this.fileNameToCycleFunction = fileNameToCycleFunction;
    }

    /**
     * Called when a new file is created in the directory, updating the internal cycle tracking.
     *
     * @param file  The newly created file.
     * @param cycle The cycle number associated with the file.
     */
    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle); // Update cycle tracking on file creation
    }

    /**
     * Refreshes the directory listing, scanning for queue files and updating the minimum and
     * maximum cycle numbers. If {@code force} is true, forces a directory scan even if conditions
     * for automatic refresh are not met.
     *
     * @param force If true, forces a refresh.
     */
    @Override
    public void refresh(boolean force) {
        lastRefreshTimeMS = System.currentTimeMillis(); // Record the time of the refresh

        final String[] fileNamesList = queueDir.list(); // Get the list of filenames in the directory
        String minFilename = INITIAL_MIN_FILENAME;
        String maxFilename = INITIAL_MAX_FILENAME;
        if (fileNamesList != null) {
            for (String fileName : fileNamesList) {
                if (fileName.endsWith(SingleChronicleQueue.SUFFIX)) {
                    if (minFilename.compareTo(fileName) > 0)
                        minFilename = fileName; // Track the minimum filename

                    if (maxFilename.compareTo(fileName) < 0)
                        maxFilename = fileName; // Track the maximum filename
                }
            }
        }

        // Update the minimum and maximum cycles based on the filenames
        int min = UNSET_MIN_CYCLE;
        if (!INITIAL_MIN_FILENAME.equals(minFilename))
            min = fileNameToCycleFunction.applyAsInt(minFilename);

        int max = UNSET_MAX_CYCLE;
        if (!INITIAL_MAX_FILENAME.equals(maxFilename))
            max = fileNameToCycleFunction.applyAsInt(maxFilename);

        minCreatedCycle = min;
        maxCreatedCycle = max;
    }

    /**
     * Returns the timestamp of the last refresh in milliseconds since the epoch.
     *
     * @return The timestamp of the last directory refresh.
     */
    @Override
    public long lastRefreshTimeMS() {
        return lastRefreshTimeMS;
    }

    /**
     * Returns the minimum cycle number of the created files in the directory.
     *
     * @return The minimum cycle number.
     */
    @Override
    public int getMinCreatedCycle() {
        return minCreatedCycle;
    }

    /**
     * Returns the maximum cycle number of the created files in the directory.
     *
     * @return The maximum cycle number.
     */
    @Override
    public int getMaxCreatedCycle() {
        return maxCreatedCycle;
    }

    /**
     * Returns the modification count of the directory listing, indicating the number of changes
     * since the last refresh. This implementation returns {@code -1}, as no modification tracking
     * is implemented.
     *
     * @return The modification count, or {@code -1} if unsupported.
     */
    @Override
    public long modCount() {
        return -1;
    }

    /**
     * Updates the cycle tracking when a new cycle is rolled.
     * Adjusts the minimum and maximum cycle numbers based on the provided cycle.
     *
     * @param cycle The new cycle number.
     */
    @Override
    public void onRoll(int cycle) {
        minCreatedCycle = Math.min(minCreatedCycle, cycle); // Update the minimum cycle
        maxCreatedCycle = Math.max(maxCreatedCycle, cycle); // Update the maximum cycle
    }
}
