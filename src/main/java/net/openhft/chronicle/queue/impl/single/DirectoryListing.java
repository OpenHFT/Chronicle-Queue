package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;

import java.io.File;
import java.nio.file.Path;
import java.util.function.ToIntFunction;

public final class DirectoryListing {
    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle";
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";
    private final TableStore tableStore;
    private final Path queuePath;
    private final ToIntFunction<File> fileToCycleFunction;
    private final LongValue maxCycleValue;
    private final LongValue minCycleValue;


    DirectoryListing(
            final TableStore tableStore, final Path queuePath,
            final ToIntFunction<File> fileToCycleFunction) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileToCycleFunction = fileToCycleFunction;
        maxCycleValue = tableStore.acquireValueFor(HIGHEST_CREATED_CYCLE);
        minCycleValue = tableStore.acquireValueFor(LOWEST_CREATED_CYCLE);
    }

    void refresh() {
        // TODO this must take out a lock
        maxCycleValue.setOrderedValue(Integer.MIN_VALUE);
        minCycleValue.setOrderedValue(Integer.MAX_VALUE);
        final File[] queueFiles = queuePath.toFile().
                listFiles((d, f) -> f.endsWith(SingleChronicleQueue.SUFFIX));
        if (queueFiles != null) {
            for (File queueFile : queueFiles) {
                onFileCreated(queueFile, fileToCycleFunction.applyAsInt(queueFile));
            }
        }
    }

    void onFileCreated(final File file, final int cycle) {
//        System.out.printf("cycle file created: %d%n", cycle);
        maxCycleValue.setMaxValue(cycle);
        minCycleValue.setMinValue(cycle);
    }

    int getMaxCreatedCycle() {
        final int value = (int) maxCycleValue.getVolatileValue();
//        System.out.printf("read max cycle: %d%n", value);
        return value;
    }

    int getMinCreatedCycle() {
        final int value = (int) minCycleValue.getVolatileValue();
//        System.out.printf("read min cycle: %d%n", value);
        return value;
    }

    boolean fileExists(final File file) {
        return tableStore.acquireValueFor(file.getPath()).getValue() != 0;
    }
}
