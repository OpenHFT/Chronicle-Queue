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
import net.openhft.chronicle.core.time.TimeProvider;

import java.io.File;
import java.util.function.ToIntFunction;

import static net.openhft.chronicle.queue.impl.single.TableDirectoryListing.*;

final class FileSystemDirectoryListing extends SimpleCloseable implements DirectoryListing {
    private final File queueDir;
    private final ToIntFunction<String> fileNameToCycleFunction;
    private final TimeProvider time;
    private int minCreatedCycle = Integer.MAX_VALUE;
    private int maxCreatedCycle = Integer.MIN_VALUE;
    private long lastRefreshTimeMS;

    FileSystemDirectoryListing(final File queueDir,
                               final ToIntFunction<String> fileNameToCycleFunction,
                               final TimeProvider time) {
        this.queueDir = queueDir;
        this.fileNameToCycleFunction = fileNameToCycleFunction;
        this.time = time;
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);
    }

    @Override
    public void refresh(boolean force) {
        lastRefreshTimeMS = time.currentTimeMillis();

        final String[] fileNamesList = queueDir.list();
        String minFilename = INITIAL_MIN_FILENAME;
        String maxFilename = INITIAL_MAX_FILENAME;
        if (fileNamesList != null) {
            for (String fileName : fileNamesList) {
                if (fileName.endsWith(SingleChronicleQueue.SUFFIX)) {
                    if (minFilename.compareTo(fileName) > 0)
                        minFilename = fileName;

                    if (maxFilename.compareTo(fileName) < 0)
                        maxFilename = fileName;
                }
            }
        }

        int min = UNSET_MIN_CYCLE;
        if (!INITIAL_MIN_FILENAME.equals(minFilename))
            min = fileNameToCycleFunction.applyAsInt(minFilename);

        int max = UNSET_MAX_CYCLE;
        if (!INITIAL_MAX_FILENAME.equals(maxFilename))
            max = fileNameToCycleFunction.applyAsInt(maxFilename);

        minCreatedCycle = min;
        maxCreatedCycle = max;
    }

    @Override
    public long lastRefreshTimeMS() {
        return lastRefreshTimeMS;
    }

    @Override
    public int getMinCreatedCycle() {
        return minCreatedCycle;
    }

    @Override
    public int getMaxCreatedCycle() {
        return maxCreatedCycle;
    }

    @Override
    public long modCount() {
        return -1;
    }

    @Override
    public void onRoll(int cycle) {
        minCreatedCycle = Math.min(minCreatedCycle, cycle);
        maxCreatedCycle = Math.max(maxCreatedCycle, cycle);
    }
}
