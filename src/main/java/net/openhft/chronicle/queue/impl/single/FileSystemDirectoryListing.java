package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.SimpleCloseable;

import java.io.File;
import java.util.function.ToIntFunction;

final class FileSystemDirectoryListing extends SimpleCloseable implements DirectoryListing {
    private final File queueDir;
    private final ToIntFunction<File> fileToCycleFunction;

    FileSystemDirectoryListing(final File queueDir,
                               final ToIntFunction<File> fileToCycleFunction) {
        this.queueDir = queueDir;
        this.fileToCycleFunction = fileToCycleFunction;
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        throwExceptionIfClosed();

    }

    @Override
    public int getMaxCreatedCycle() {
        throwExceptionIfClosed();

        int maxCycle = Integer.MIN_VALUE;
        final File[] files = queueDir.listFiles((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX));
        if (files != null) {
            for (File file : files) {
                maxCycle = Math.max(maxCycle, fileToCycleFunction.applyAsInt(file));
            }
        }
        return maxCycle;
    }

    @Override
    public int getMinCreatedCycle() {
        throwExceptionIfClosed();

        int minCycle = Integer.MAX_VALUE;
        final File[] files = queueDir.listFiles((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX));
        if (files != null) {
            for (File file : files) {
                minCycle = Math.min(minCycle, fileToCycleFunction.applyAsInt(file));
            }
        }
        return minCycle;
    }

    @Override
    public long modCount() {
        throwExceptionIfClosed();

        return -1;
    }
}
