package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.function.ToIntFunction;

final class TableDirectoryListing extends AbstractCloseable implements DirectoryListing {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDirectoryListing.class);
    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle";
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";
    private static final String MOD_COUNT = "listing.modCount";
    private static final int UNSET_MAX_CYCLE = Integer.MIN_VALUE;
    private static final int UNSET_MIN_CYCLE = Integer.MAX_VALUE;
    private final TableStore<?> tableStore;
    private final Path queuePath;
    private final ToIntFunction<File> fileToCycleFunction;
    private final boolean readOnly;
    private volatile LongValue maxCycleValue;
    private volatile LongValue minCycleValue;
    private volatile LongValue modCount;

    TableDirectoryListing(
            @NotNull TableStore<?> tableStore, final Path queuePath,
            final ToIntFunction<File> fileToCycleFunction,
            final boolean readOnly) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileToCycleFunction = fileToCycleFunction;
        this.readOnly = readOnly;
    }

    @Override
    public void init() {
        tableStore.doWithExclusiveLock(ts -> {
            maxCycleValue = ts.acquireValueFor(HIGHEST_CREATED_CYCLE);
            minCycleValue = ts.acquireValueFor(LOWEST_CREATED_CYCLE);
            minCycleValue.compareAndSwapValue(Long.MIN_VALUE, UNSET_MIN_CYCLE);
            modCount = ts.acquireValueFor(MOD_COUNT);
            if (modCount.getVolatileValue() == Long.MIN_VALUE) {
                modCount.compareAndSwapValue(Long.MIN_VALUE, 0);
            }
            return this;
        });
    }

    @Override
    public void refresh() {
        closeCheck();
        if (readOnly) {
            return;
        }
        while (true) {
            long currentMax = maxCycleValue.getVolatileValue();
            final File[] queueFiles = queuePath.toFile().
                    listFiles((d, f) -> f.endsWith(SingleChronicleQueue.SUFFIX));
            int min = UNSET_MIN_CYCLE;
            int max = UNSET_MAX_CYCLE;
            if (queueFiles != null) {
                for (File queueFile : queueFiles) {
                    min = Math.min(fileToCycleFunction.applyAsInt(queueFile), min);
                    max = Math.max(fileToCycleFunction.applyAsInt(queueFile), max);
                }
            }
            minCycleValue.setOrderedValue(min);
            if (maxCycleValue.compareAndSwapValue(currentMax, max))
                break;
        }
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        closeCheck();
        if (readOnly) {
            LOGGER.warn("DirectoryListing is read-only, not updating listing");
            return;
        }
        modCount.addAtomicValue(1);
        if (cycle > getMaxCreatedCycle()) {
            maxCycleValue.setMaxValue(cycle);
        }
        if (cycle < getMinCycleValue()) {
            minCycleValue.setMinValue(cycle);
        }
    }

    @Override
    public int getMaxCreatedCycle() {
        closeCheck();
        return getMaxCycleValue();
    }

    @Override
    public int getMinCreatedCycle() {
        closeCheck();
        return getMinCycleValue();
    }

    @Override
    public long modCount() {
        closeCheck();
        return modCount.getVolatileValue();
    }

    @Override
    public String toString() {
        return tableStore.dump();
    }

    protected void performClose() {
        Closeable.closeQuietly(minCycleValue, maxCycleValue, modCount);
    }

    private void closeCheck() {
        if (tableStore.isClosed()) {
            throw new IllegalStateException("Underlying TableStore is already closed - was the Queue closed?");
        }
    }

    private int getMaxCycleValue() {
        return (int) maxCycleValue.getVolatileValue();
    }

    private int getMinCycleValue() {
        return (int) minCycleValue.getVolatileValue();
    }
}