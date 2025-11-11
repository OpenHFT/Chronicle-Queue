/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.TableStoreWriteLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * The {@code IndexUpdaterFactory} class provides factory methods for creating instances of
 * {@link IndexUpdater}. Depending on the type of tailer (named or unnamed, replicated or not),
 * the appropriate {@link IndexUpdater} implementation is returned.
 */
public class IndexUpdaterFactory {

    /**
     * Creates an {@link IndexUpdater} based on the provided {@code tailerName} and {@code queue}.
     * <p>
     * If the tailer is unnamed, this method returns {@code null}. For replicated named tailers,
     * a versioned updater is returned. Otherwise, a standard unversioned index updater is used.
     *
     * @param tailerName the name of the tailer, or {@code null} if unnamed
     * @param queue the {@link SingleChronicleQueue} instance
     * @return the appropriate {@link IndexUpdater} instance, or {@code null} for unnamed tailers
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
                    tailerName,
                    queue,
                    queue.indexForId(tailerName),
                    queue.indexVersionForId(tailerName)
            );
        } else {
            // Normal named tailers use a simple unversioned scheme
            return new StandardIndexUpdater(queue.indexForId(tailerName));
        }
    }

    /**
     * The {@code StandardIndexUpdater} class implements {@link IndexUpdater} for tailers that do not
     * require versioning. It simply updates the index value without any additional tracking.
     */
    public static class StandardIndexUpdater implements IndexUpdater, Closeable {

        private final LongValue indexValue;

        /**
         * Constructs a new {@code StandardIndexUpdater} with the provided index value.
         *
         * @param indexValue the {@link LongValue} representing the current index
         */
        public StandardIndexUpdater(@NotNull LongValue indexValue) {
            this.indexValue = indexValue;
        }

        /**
         * Closes this {@code StandardIndexUpdater}, releasing any resources held.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void close() throws IOException {
            closeQuietly(indexValue);
        }

        /**
         * Updates the index value to the specified {@code index}.
         *
         * @param index the new index value
         */
        @Override
        public void update(long index) {
            indexValue.setValue(index);
        }

        /**
         * Returns the current index value.
         *
         * @return the {@link LongValue} representing the current index
         */
        @Override
        public LongValue index() {
            return indexValue;
        }
    }

    /**
     * The {@code VersionedIndexUpdater} class extends {@link IndexUpdater} for replicated named tailers.
     * In addition to updating the index, it increments a version field on each update, ensuring version tracking.
     */
    public static class VersionedIndexUpdater implements IndexUpdater, Closeable {

        private final TableStoreWriteLock versionIndexLock;
        private final LongValue indexValue;
        private final LongValue indexVersionValue;

        /**
         * Constructs a new {@code VersionedIndexUpdater} with the provided values.
         *
         * @param tailerName the name of the tailer
         * @param queue the {@link SingleChronicleQueue} instance
         * @param indexValue the {@link LongValue} representing the current index
         * @param indexVersionValue the {@link LongValue} representing the version of the index
         */
        public VersionedIndexUpdater(@NotNull String tailerName,
                                     @NotNull SingleChronicleQueue queue,
                                     @NotNull LongValue indexValue,
                                     @NotNull LongValue indexVersionValue) {
            this.versionIndexLock = queue.versionIndexLockForId(tailerName);
            this.versionIndexLock.forceUnlockIfProcessIsDead();
            this.indexValue = indexValue;
            this.indexVersionValue = indexVersionValue;
        }

        /**
         * Closes this {@code VersionedIndexUpdater}, releasing any resources held.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void close() throws IOException {
            closeQuietly(versionIndexLock, indexValue, indexVersionValue);
        }

        /**
         * Updates the index value and increments the version field.
         * This operation is performed within a lock to ensure consistency.
         *
         * @param index the new index value
         */
        @Override
        public void update(long index) {
            try {
                versionIndexLock.lock();
                indexValue.setVolatileValue(index);
                indexVersionValue.addAtomicValue(1);
            } finally {
                versionIndexLock.unlock();
            }
        }

        /**
         * Returns the current index value.
         *
         * @return the {@link LongValue} representing the current index
         */
        @Override
        public LongValue index() {
            return indexValue;
        }
    }
}
