package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.function.ToIntFunction;

final class TableDirectoryListing extends AbstractCloseable implements DirectoryListing {

    private static final String HIGHEST_CREATED_CYCLE = "listing.highestCycle";
    private static final String LOWEST_CREATED_CYCLE = "listing.lowestCycle";
    private static final String MOD_COUNT = "listing.modCount";
    static final int UNSET_MAX_CYCLE = Integer.MIN_VALUE;
    static final int UNSET_MIN_CYCLE = Integer.MAX_VALUE;
    static final String INITIAL_MIN_FILENAME = Character.toString(Character.MAX_VALUE);
    static final String INITIAL_MAX_FILENAME = Character.toString(Character.MIN_VALUE);
    private final TableStore<?> tableStore;
    private final Path queuePath;
    private final ToIntFunction<String> fileNameToCycleFunction;
    private volatile LongValue maxCycleValue;
    private volatile LongValue minCycleValue;
    private volatile LongValue modCount;
    private long lastRefreshTimeMS = 0;

    TableDirectoryListing(
            final @NotNull TableStore<?> tableStore,
            final Path queuePath,
            final ToIntFunction<String> fileNameToCycleFunction) {
        this.tableStore = tableStore;
        this.queuePath = queuePath;
        this.fileNameToCycleFunction = fileNameToCycleFunction;

        if (tableStore.readOnly()) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " should only be used for writable queues");
        }
        disableThreadSafetyCheck(true);
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

        if (!force) {
            return;
        }

        lastRefreshTimeMS = System.currentTimeMillis();

        final long currentMin0 = minCycleValue.getVolatileValue();
        final long currentMax0 = maxCycleValue.getVolatileValue();

        while (true) {
            throwExceptionIfClosed();
            tableStore.throwExceptionIfClosed();
            Jvm.safepoint();
            final long currentMax = maxCycleValue.getVolatileValue();

            final String[] fileNamesList = queuePath.toFile().list();
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

            if (currentMin0 == min && currentMax0 == max)
                return;

            minCycleValue.setOrderedValue(min);
            if (maxCycleValue.compareAndSwapValue(currentMax, max)) {
                modCount.addAtomicValue(1);
                break;
            }
            Jvm.nanoPause();
        }
    }

    @Override
    public void onFileCreated(final File file, final int cycle) {
        onRoll(cycle);
    }

    @Override
    public void onRoll(int cycle) {
        minCycleValue.setMinValue(cycle);
        maxCycleValue.setMaxValue(cycle);
        modCount.addAtomicValue(1);
    }

    @Override
    public long lastRefreshTimeMS() {
        return lastRefreshTimeMS;
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

}