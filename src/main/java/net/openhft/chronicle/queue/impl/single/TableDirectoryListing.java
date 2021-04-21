package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.ManagedCloseable;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.ReadonlyTableStore;
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
            final @NotNull TableStore<?> tableStore,
            final Path queuePath,
            final ToIntFunction<File> fileToCycleFunction,
            final boolean readOnly) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileToCycleFunction = fileToCycleFunction;
        this.readOnly = readOnly;
    }

    @Override
    public void init() {
        throwExceptionIfClosedInSetter();

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
    public void refresh(final boolean force) {

        if (readOnly || !force) {
            return;
        }

        throwExceptionIfClosed();

        while (true) {
            throwExceptionIfClosed();
            ((ManagedCloseable) tableStore).throwExceptionIfClosed();
            Jvm.safepoint();
            final long currentMax = maxCycleValue.getVolatileValue();
            Jvm.safepoint();
            final File[] queueFiles = queuePath.toFile().
                    listFiles((d, f) -> f.endsWith(SingleChronicleQueue.SUFFIX));
            int min = UNSET_MIN_CYCLE;
            int max = UNSET_MAX_CYCLE;
            if (queueFiles != null) {
                for (File queueFile : queueFiles) {
                    int cycle = fileToCycleFunction.applyAsInt(queueFile);
                    min = Math.min(cycle, min);
                    max = Math.max(cycle, max);
                }
            }
            minCycleValue.setOrderedValue(min);
            if (maxCycleValue.compareAndSwapValue(currentMax, max))
                break;
            Jvm.nanoPause();
        }
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
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
        return getMaxCycleValue();
    }

    @Override
    public int getMinCreatedCycle() {
        return getMinCycleValue();
    }

    @Override
    public long modCount() {
        return modCount.getVolatileValue();
    }

    @Override
    public String toString() {
        return tableStore.dump();
    }

    protected void performClose() {
        Closeable.closeQuietly(minCycleValue, maxCycleValue, modCount);
    }

    private int getMaxCycleValue() {
        return (int) maxCycleValue.getVolatileValue();
    }

    private int getMinCycleValue() {
        return (int) minCycleValue.getVolatileValue();
    }

    @Override
    protected boolean threadSafetyCheck(final boolean isUsed) {
        // TDL are thread safe
        return true;
    }
}