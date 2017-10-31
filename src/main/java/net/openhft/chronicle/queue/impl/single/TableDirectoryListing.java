package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

final class TableDirectoryListing implements DirectoryListing {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDirectoryListing.class);
    private static final long LOCK_ACQUISITION_TIMEOUT_MILLIS =
            Long.getLong("chronicle.listing.lock.timeout", TimeUnit.SECONDS.toMillis(20L));
    private static final long LOCK_MAX_AGE_MILLIS =
            Long.getLong("chronicle.listing.lock.maxAge", TimeUnit.SECONDS.toMillis(10L));

    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle";
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";
    // visible for testing
    static final String LOCK = "listing.exclusiveLock";
    static final String MOD_COUNT = "listing.modCount";
    private final TableStore tableStore;
    private final Path queuePath;
    private final ToIntFunction<File> fileToCycleFunction;
    private volatile LongValue maxCycleValue;
    private volatile LongValue minCycleValue;
    private volatile LongValue lock;
    private volatile LongValue modCount;
    private final boolean readOnly;

    TableDirectoryListing(
            final TableStore tableStore, final Path queuePath,
            final ToIntFunction<File> fileToCycleFunction,
            final boolean readOnly) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileToCycleFunction = fileToCycleFunction;
        this.readOnly = readOnly;
    }

    @Override
    public void init() {
        final long timeoutAt = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(20L);
        boolean warnedOnFailure = false;
        while (System.currentTimeMillis() < timeoutAt) {
            try (final FileChannel channel = FileChannel.open(tableStore.file().toPath(),
                    StandardOpenOption.WRITE);
                 final FileLock fileLock = channel.tryLock()) {
                maxCycleValue = tableStore.acquireValueFor(HIGHEST_CREATED_CYCLE);
                minCycleValue = tableStore.acquireValueFor(LOWEST_CREATED_CYCLE);
                lock = tableStore.acquireValueFor(LOCK);
                modCount = tableStore.acquireValueFor(MOD_COUNT);
                if (lock.getVolatileValue() == Long.MIN_VALUE) {
                    lock.compareAndSwapValue(Long.MIN_VALUE, 0);
                }
                if (modCount.getVolatileValue() == Long.MIN_VALUE) {
                    modCount.compareAndSwapValue(Long.MIN_VALUE, 0);
                }
                return;
            } catch (IOException | RuntimeException e) {
                // failed to acquire the lock, wait until other operation completes
                if (!warnedOnFailure) {
                    LOGGER.warn("Failed to acquire a lock on the directory-listing file: {}:{}. Retrying.",
                            e.getClass().getSimpleName(), e.getMessage());
                    warnedOnFailure = true;
                }
                Jvm.pause(50L);
            }
        }

        throw new IllegalStateException("Unable to claim exclusive lock on file " + tableStore.file());
    }

    @Override
    public void refresh() {
        if (readOnly) {
            return;
        }
        tryWithLock(this::refreshIndex);
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        if (readOnly) {
            LOGGER.warn("DirectoryListing is read-only, not updating listing");
            return;
        }
        final long newModCount = modCount.addAtomicValue(1);
        new RuntimeException(System.currentTimeMillis() + "/new modCount: " + newModCount + "/" + file.getName()).printStackTrace(System.out);
        System.out.println(file.getParentFile().listFiles((d, n) -> n.endsWith(SingleChronicleQueue.SUFFIX)).length);
        tryWithLock(() -> {
            maxCycleValue.setMaxValue(cycle);
            minCycleValue.setMinValue(cycle);
            return 0;
        });
    }

    @Override
    public int getMaxCreatedCycle() {
        if (readOnly) {
            return getMaxCycleValue();
        }
        return tryWithLock(this::getMaxCycleValue);
    }

    @Override
    public int getMinCreatedCycle() {
        if (readOnly) {
            return getMinCycleValue();
        }
        return tryWithLock(this::getMinCycleValue);
    }

    @Override
    public long modCount() {
        return modCount.getVolatileValue();
    }

    @Override
    public String toString() {
        return tableStore.dump();
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
                if (lastLockTime != 0 && lastLockTime < currentTime - LOCK_MAX_AGE_MILLIS) {
                    // assume that previous lock holder has died
                    if (lock.compareAndSwapValue(lastLockTime, currentTime)) {
                        LOGGER.warn("Forcing lock on directory listing as it is {}sec old",
                                (currentTime - lastLockTime) / 1000);
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