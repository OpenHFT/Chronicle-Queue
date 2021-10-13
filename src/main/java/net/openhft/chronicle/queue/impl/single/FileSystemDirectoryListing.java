package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.SimpleCloseable;

import java.io.File;
import java.util.function.ToIntFunction;

import static net.openhft.chronicle.queue.impl.single.TableDirectoryListing.*;

final class FileSystemDirectoryListing extends SimpleCloseable implements DirectoryListing {
    private final File queueDir;
    private final ToIntFunction<String> fileNameToCycleFunction;
    private int minCreatedCycle = Integer.MAX_VALUE;
    private int maxCreatedCycle = Integer.MIN_VALUE;
    private long lastRefreshTimeMS;

    FileSystemDirectoryListing(final File queueDir,
                               final ToIntFunction<String> fileNameToCycleFunction) {
        this.queueDir = queueDir;
        this.fileNameToCycleFunction = fileNameToCycleFunction;
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);
    }

    @Override
    public void refresh(boolean force) {
        lastRefreshTimeMS = System.currentTimeMillis();

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
