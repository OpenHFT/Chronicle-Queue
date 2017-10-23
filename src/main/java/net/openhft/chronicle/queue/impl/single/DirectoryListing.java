package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

final class DirectoryListing {
    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle";
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";
    private static final String LOCK = "listing.exclusiveLock";
    private final TableStore tableStore;
    private final Path queuePath;
    private final ToIntFunction<File> fileToCycleFunction;
    private final LongValue maxCycleValue;
    private final LongValue minCycleValue;
    private final LongValue lock;

    DirectoryListing(
            final TableStore tableStore, final Path queuePath,
            final ToIntFunction<File> fileToCycleFunction) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileToCycleFunction = fileToCycleFunction;
        maxCycleValue = tableStore.acquireValueFor(HIGHEST_CREATED_CYCLE);
        minCycleValue = tableStore.acquireValueFor(LOWEST_CREATED_CYCLE);
        lock = tableStore.acquireValueFor(LOCK);
    }

    // TODO timeouts as system properties
    void refresh() {
        tryWithLock(this::refreshIndex);
    }

    void onFileCreated(final File file, final int cycle) {
        tryWithLock(() -> {
            maxCycleValue.setMaxValue(cycle);
            minCycleValue.setMinValue(cycle);
            return 0;
        });
    }

    int getMaxCreatedCycle() {
        return tryWithLock(this::getMaxCycleValue);
    }

    int getMinCreatedCycle() {
        return tryWithLock(this::getMinCycleValue);
    }

    private int getMaxCycleValue() {
        return (int) maxCycleValue.getVolatileValue();
    }

    private int getMinCycleValue() {
        return (int) minCycleValue.getVolatileValue();
    }

    private int tryWithLock(final IntSupplier function) {
        final long lockAcquisitionTimeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
        long currentTime;
        while ((currentTime = System.currentTimeMillis()) < lockAcquisitionTimeout) {
            if (lock.compareAndSwapValue(0, currentTime)) {
                try {
                    return function.getAsInt();
                } finally {
                    lock.setOrderedValue(0);
                }

            } else  {
                // assume that previous lock holder has died
                final long lastLockTime = lock.getValue();
                if (lastLockTime < currentTime - TimeUnit.SECONDS.toMillis(5L)) {
                    if (lock.compareAndSwapValue(lastLockTime, currentTime)) {
                        try {
                            refreshIndex();
                        } finally {
                            lock.setOrderedValue(0);
                        }
                        return function.getAsInt();
                    }
                }
                Thread.yield();
            }
        }

        throw new IllegalStateException("Unable to acquire exclusive lock on directory listing");
    }

    private int refreshIndex() {
        maxCycleValue.setOrderedValue(Integer.MIN_VALUE);
        minCycleValue.setOrderedValue(Integer.MAX_VALUE);
        final File[] queueFiles = queuePath.toFile().
                listFiles((d, f) -> f.endsWith(SingleChronicleQueue.SUFFIX));
        if (queueFiles != null) {
            for (File queueFile : queueFiles) {
                maxCycleValue.setMaxValue(fileToCycleFunction.applyAsInt(queueFile));
                minCycleValue.setMinValue(fileToCycleFunction.applyAsInt(queueFile));

            }
        }
        return 0;
    }
}