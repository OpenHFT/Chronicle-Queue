/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.function.Function;

/**
 * The {@code ReadonlyTableStore} class is a read-only implementation of the {@link TableStore} interface.
 * It provides metadata access while throwing {@link UnsupportedOperationException} for any modification attempts.
 *
 * @param <T> the type of the metadata used by this table store
 */
public class ReadonlyTableStore<T extends Metadata> extends AbstractCloseable implements TableStore<T> {
    private final T metadata;

    /**
     * Constructs a {@code ReadonlyTableStore} with the specified metadata.
     *
     * @param metadata the metadata associated with this store
     */
    @SuppressWarnings("this-escape")
    public ReadonlyTableStore(T metadata) {
        this.metadata = metadata;
        singleThreadedCheckDisabled(true);
    }

    /**
     * Returns the metadata associated with this store.
     *
     * @return the metadata
     */
    @Override
    public T metadata() {
        return metadata;
    }

    /**
     * No-op close method for read-only table store.
     */
    @Override
    protected void performClose() {
    }

    /**
     * Unsupported operation for acquiring a value in a read-only store.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return never returns as this is unsupported
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public LongValue acquireValueFor(CharSequence key, long defaultValue) {
        throw new UnsupportedOperationException("Read only");
    }

    /**
     * Unsupported operation for iterating over keys in a read-only store.
     *
     * @param accumulator the accumulator
     * @param tsIterator the iterator
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public <T> void forEachKey(T accumulator, TableStoreIterator<T> tsIterator) {
        throw new UnsupportedOperationException("Read only");
    }

    /**
     * Unsupported operation for exclusive locking in a read-only store.
     *
     * @param code the code block to execute with the lock
     * @param <R> the result type
     * @return never returns as this is unsupported
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public <R> R doWithExclusiveLock(Function<TableStore<T>, ? extends R> code) {
        throw new UnsupportedOperationException("Read only");
    }

    /**
     * Unsupported operation for retrieving the file associated with this store.
     *
     * @return never returns as this is unsupported
     * @throws UnsupportedOperationException always thrown
     */
    @Nullable
    @Override
    public File file() {
        throwExceptionIfClosed();
        throw new UnsupportedOperationException("Read only");
    }

    /**
     * Unsupported operation for retrieving the bytes associated with this store.
     *
     * @return never returns as this is unsupported
     * @throws UnsupportedOperationException always thrown
     */
    @NotNull
    @Override
    public MappedBytes bytes() {
        throwExceptionIfClosed();
        throw new UnsupportedOperationException("Read only");
    }

    /**
     * Returns a string representation of the metadata for dumping.
     *
     * @param wireType the wire type to format the output
     * @return the string representation of the metadata
     */
    @Override
    public String dump(WireType wireType) {
        return metadata.toString();
    }

    /**
     * Unsupported operation for writing marshallable data in a read-only store.
     *
     * @param wire the wire to write to
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        throw new UnsupportedOperationException("Read only");
    }

    /**
     * Returns true indicating that this store is read-only.
     *
     * @return true, as this store is read-only
     */
    @Override
    public boolean readOnly() {
        return true;
    }
}
