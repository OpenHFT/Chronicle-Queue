package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * Factory for creating instances of {@link IndexUpdater}.
 */
public class IndexUpdaterFactory {

    /**
     * Create an instance of an {@link IndexUpdater} depending on the values provided.
     */
    @Nullable
    public static IndexUpdater createIndexUpdater(@Nullable String tailerName, @NotNull SingleChronicleQueue queue) {
        if (tailerName == null) {
            // A null index updater is used when a plain (unnamed) tailer is in use
            // Note this nullness is not ideal and needs to be tackled in a future refactor of StoreTailer
            return null;
        } else if (tailerName.startsWith(SingleChronicleQueue.REPLICATED_NAMED_TAILER_PREFIX)) {
            // Replicated named tailers use an additional version field updated on each index mutation
            return new VersionedIndexUpdater(
                    queue.indexForId(tailerName),
                    queue.indexVersionForId(tailerName)
            );
        } else {
            // Normal named tailers use a simple unversioned scheme
            return new StandardIndexUpdater(queue.indexForId(tailerName));
        }
    }

    /**
     * An index updater that simply sets the index value on update. No versioning.
     */
    public static class StandardIndexUpdater implements IndexUpdater, Closeable {

        private final LongValue indexValue;

        public StandardIndexUpdater(@NotNull LongValue indexValue) {
            this.indexValue = indexValue;
        }

        @Override
        public void close() throws IOException {
            closeQuietly(indexValue);
        }

        @Override
        public void update(long index) {
            indexValue.setValue(index);
        }

        @Override
        public LongValue index() {
            return indexValue;
        }

    }

    /**
     * An index updater that increments a version field on every update.
     */
    public static class VersionedIndexUpdater implements IndexUpdater, Closeable {

        private final LongValue indexValue;

        private final LongValue indexVersionValue;

        public VersionedIndexUpdater(@NotNull LongValue indexValue, @NotNull LongValue indexVersionValue) {
            this.indexValue = indexValue;
            this.indexVersionValue = indexVersionValue;
        }

        @Override
        public void close() throws IOException {
            closeQuietly(indexValue, indexVersionValue);
        }

        @Override
        public void update(long index) {
            indexValue.setValue(index);
            indexVersionValue.addAtomicValue(1);
        }

        @Override
        public LongValue index() {
            return indexValue;
        }
    }
}
