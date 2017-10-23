package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

final class DirectoryListing {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryListing.class);
    private static final long LOCK_ACQUISITION_TIMEOUT_MILLIS =
            Long.getLong("chronicle.listing.lock.timeout", TimeUnit.SECONDS.toMillis(20L));
    private static final long LOCK_MAX_AGE_MILLIS =
            Long.getLong("chronicle.listing.lock.maxAge", TimeUnit.SECONDS.toMillis(10L));

    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle";
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";
    private static final String LOCK = "listing.exclusiveLock";
    private final TableStore tableStore;
    private final Path queuePath;
    private final ToIntFunction<File> fileToCycleFunction;
    private final LongValue maxCycleValue;
    private final LongValue minCycleValue;
    private final LongValue lock;
    private final boolean readOnly;

    DirectoryListing(
            final TableStore tableStore, final Path queuePath,
            final ToIntFunction<File> fileToCycleFunction,
            final boolean readOnly) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileToCycleFunction = fileToCycleFunction;
        maxCycleValue = tableStore.acquireValueFor(HIGHEST_CREATED_CYCLE);
        minCycleValue = tableStore.acquireValueFor(LOWEST_CREATED_CYCLE);
        lock = tableStore.acquireValueFor(LOCK);
        this.readOnly = readOnly;
    }

    void refresh() {
        if (readOnly) {
            return;
        }
        tryWithLock(this::refreshIndex);
    }

    void onFileCreated(final File file, final int cycle) {
        if (readOnly) {
            LOGGER.warn("DirectoryListing is read-only, not updating listing");
            return;
        }
        tryWithLock(() -> {
            maxCycleValue.setMaxValue(cycle);
            minCycleValue.setMinValue(cycle);
            return 0;
        });
    }

    int getMaxCreatedCycle() {
        if (readOnly) {
            return getMaxCycleValue();
        }
        return tryWithLock(this::getMaxCycleValue);
    }

    int getMinCreatedCycle() {
        if (readOnly) {
            return getMinCycleValue();
        }
        return tryWithLock(this::getMinCycleValue);
    }

    private int getMaxCycleValue() {
        return (int) maxCycleValue.getVolatileValue();
    }

    private int getMinCycleValue() {
        return (int) minCycleValue.getVolatileValue();
    }

    private int tryWithLock(final IntSupplier function) {
        final long lockAcquisitionTimeout = System.currentTimeMillis() + LOCK_ACQUISITION_TIMEOUT_MILLIS;
        long currentTime;
        while ((currentTime = System.currentTimeMillis()) < lockAcquisitionTimeout) {
            if (lock.compareAndSwapValue(0, currentTime)) {
                try {
                    return function.getAsInt();
                } finally {
                    if (!lock.compareAndSwapValue(currentTime, 0)) {
                        throw new IllegalStateException("Unable to reset lock state");
                    }
                }

            } else  {
                final long lastLockTime = lock.getValue();
                if (lastLockTime < currentTime - LOCK_MAX_AGE_MILLIS) {
                    // assume that previous lock holder has died
                    if (lock.compareAndSwapValue(lastLockTime, currentTime)) {
                        try {
                            return function.getAsInt();
                        } finally {
                            if (!lock.compareAndSwapValue(currentTime, 0)) {
                                throw new IllegalStateException("Unable to reset lock state");
                            }
                        }
                    }
                }
                Thread.yield();
            }
        }

        throw new IllegalStateException("Unable to acquire exclusive lock on directory listing.\n" +
                "Consider changing system properties chronicle.listing.lock.timeout/chronicle.listing.lock.maxAge");
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

    void close() {
        tableStore.close();
    }
}