/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.domestic.ReentrantFileLock;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.scoped.ScopedResource;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataField;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The {@code SingleTableStore} class is a concrete implementation of {@link TableStore}.
 * It provides functionality for managing metadata and working with mapped bytes in Chronicle Queue.
 * It handles both shared and exclusive file locks for thread-safety during operations.
 *
 * @param <T> Metadata type, which extends {@link Metadata}.
 */
public class SingleTableStore<T extends Metadata> extends AbstractCloseable implements TableStore<T> {
    public static final String SUFFIX = ".cq4t";
    private static final String SHARED_LOCK_TYPE = "shared";
    private static final String EXCLUSIVE_LOCK_TYPE = "exclusive";
    private static final int EXCLUSIVE_LOCK_SIZE = 1;
    /**
     * We need to be able to acquire an "exclusive" lock while fine-grained long-running locks are being held.
     * For this reason the "exclusive" lock doesn't lock the whole file, but as long as everyone agrees
     * on what section constitutes an "exclusive" lock this should be fine.
     */
    private static final long EXCLUSIVE_LOCK_START = Long.MAX_VALUE - EXCLUSIVE_LOCK_SIZE;

    private static final long timeoutMS = Jvm.getLong("chronicle.table.store.timeoutMS", 10_000L);
    @NotNull
    private final WireType wireType;
    @NotNull
    private final T metadata;
    @NotNull
    private final MappedBytes mappedBytes;
    @NotNull
    private final MappedFile mappedFile;
    @NotNull
    private final Wire mappedWire;

    /**
     * Constructor for creating a {@code SingleTableStore} via deserialization using {@link Demarshallable}.
     *
     * @param wire The {@link WireIn} instance used to read the serialized data.
     */
    @SuppressWarnings("unused")
    @UsedViaReflection
    private SingleTableStore(@NotNull final WireIn wire) {
        this.wireType = Objects.requireNonNull(wire.read(MetaDataField.wireType).object(WireType.class));
        this.mappedBytes = (MappedBytes) (wire.bytes());
        this.mappedFile = mappedBytes.mappedFile();

        wire.consumePadding();
        if (wire.bytes().readRemaining() > 0) {
            this.metadata = Objects.requireNonNull(wire.read(MetaDataField.metadata).typedMarshallable());
        } else {
            @SuppressWarnings("unchecked")
            T instance = (T) Metadata.NoMeta.INSTANCE;
            this.metadata = instance;
        }

        mappedWire = wireType.apply(mappedBytes);
        mappedWire.usePadding(true);

        singleThreadedCheckDisabled(true);
    }

    /**
     * Constructs a {@code SingleTableStore} with the provided wire type, mapped bytes, and metadata.
     *
     * @param wireType    The {@link WireType} being used.
     * @param mappedBytes The {@link MappedBytes} for the data store file.
     * @param metadata    The {@link Metadata} associated with this store.
     */
    SingleTableStore(@NotNull final WireType wireType,
                     @NotNull final MappedBytes mappedBytes,
                     @NotNull final T metadata) {
        this.wireType = wireType;
        this.metadata = metadata;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        mappedWire = wireType.apply(mappedBytes);
        mappedWire.usePadding(true);

        singleThreadedCheckDisabled(true);
    }

    /**
     * Executes a code block with a shared lock on the specified file.
     *
     * @param file   The file to lock.
     * @param code   The function to execute with the locked file.
     * @param target A supplier providing the target object for the code block.
     * @param <T>    Type of the target object.
     * @param <R>    Return type of the function.
     * @return The result of the function applied to the target.
     */
    public static <T, R> R doWithSharedLock(@NotNull final File file,
                                            @NotNull final Function<T, ? extends R> code,
                                            @NotNull final Supplier<T> target) {
        return doWithLock(file, code, target, true, timeoutMS);
    }

    /**
     * Executes a callback under a shared structural lock with a caller-supplied acquisition
     * timeout. A zero timeout attempts acquisition once. The callback itself is not timed out
     * or retried; its failures propagate after releasing the lock.
     *
     * @param file table-store file to lock
     * @param timeoutMillis non-negative maximum acquisition wait in milliseconds
     * @param code callback to execute while holding the lock
     * @param target supplier of the callback target, invoked only after acquisition
     * @param <T> target type
     * @param <R> result type
     * @return the callback result
     * @throws IllegalArgumentException if the timeout is negative
     * @throws IllegalStateException if the lock cannot be acquired
     */
    public static <T, R> R doWithSharedLock(@NotNull final File file,
                                          final long timeoutMillis,
                                          @NotNull final Function<T, ? extends R> code,
                                          @NotNull final Supplier<T> target) {
        if (timeoutMillis < 0)
            throw new IllegalArgumentException("timeoutMillis must not be negative");
        return doWithLock(file, code, target, true, timeoutMillis);
    }

    /**
     * Executes a code block with an exclusive lock on the specified file.
     *
     * @param file   The file to lock.
     * @param code   The function to execute with the locked file.
     * @param target A supplier providing the target object for the code block.
     * @param <T>    Type of the target object.
     * @param <R>    Return type of the function.
     * @return The result of the function applied to the target.
     */
    public static <T, R> R doWithExclusiveLock(@NotNull final File file,
                                               @NotNull final Function<T, ? extends R> code,
                                               @NotNull final Supplier<T> target) {
        return doWithLock(file, code, target, false, timeoutMS);
    }

    /**
     * Handles file locking and executes the provided code block.
     *
     * @param file    The file to lock.
     * @param code    The function to execute.
     * @param target  The target supplier.
     * @param shared  Whether to use a shared lock.
     * @param timeoutMillis Maximum acquisition wait in milliseconds.
     * @param <T>     Type of the target object.
     * @param <R>     Return type of the function.
     * @return The result of the function applied to the target.
     */
    @SuppressWarnings("try") // Holding the acquired lock protects the callback until resource closure.
    private static <T, R> R doWithLock(@NotNull final File file,
                                       @NotNull final Function<T, ? extends R> code,
                                       @NotNull final Supplier<T> target,
                                       final boolean shared,
                                       final long timeoutMillis) {
        // Closing an extra channel can release this process's existing file lock on Unix.
        if (!shared && ReentrantFileLock.isHeldByCurrentThread(file))
            return code.apply(target.get());

        final String type = shared ? SHARED_LOCK_TYPE : EXCLUSIVE_LOCK_TYPE;
        final StandardOpenOption readOrWrite = shared ? StandardOpenOption.READ : StandardOpenOption.WRITE;

        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        final long startNanos = System.nanoTime();
        try (FileChannel channel = FileChannel.open(file.toPath(), readOrWrite);
             FileLock lock = awaitLock(channel, shared, file, startNanos, timeoutNanos)) {
            return code.apply(target.get());
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't perform operation with " + type + " file lock", e);
        }
    }

    private static FileLock awaitLock(FileChannel channel, boolean shared, File file,
                                      long startNanos, long timeoutNanos) {
        for (int count = 1; ; count++) {
            FileLock lock = tryLock(channel, shared, file, count, startNanos);
            if (lock != null)
                return lock;
            long remaining = timeoutNanos - (System.nanoTime() - startNanos);
            if (remaining <= 0)
                break;
            long delay = Math.min(250L, (long) count * count);
            Jvm.pause(Math.min(delay, TimeUnit.NANOSECONDS.toMillis(remaining)));
        }
        String type = shared ? SHARED_LOCK_TYPE : EXCLUSIVE_LOCK_TYPE;
        throw new IllegalStateException("Unable to claim exclusive " + type + " lock on file " + file);
    }

    private static FileLock tryLock(FileChannel channel, boolean shared, File file,
                                    int count, long startNanos) {
        try {
            return shared
                    ? channel.tryLock(EXCLUSIVE_LOCK_START, EXCLUSIVE_LOCK_SIZE, true)
                    : ReentrantFileLock.tryLock(file, channel);
        } catch (IOException | OverlappingFileLockException unavailable) {
            if (count > 9 && Jvm.isDebugEnabled(SingleTableStore.class)) {
                final String type = shared ? SHARED_LOCK_TYPE : EXCLUSIVE_LOCK_TYPE;
                final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                final String message = "Failed to acquire " + type + " lock on the table store file. Retrying, file="
                        + file.getAbsolutePath() + ", count=" + count + ", elapsed=" + elapsedMs + " ms";
                Jvm.debug().on(SingleTableStore.class, "", new StackTrace(message));
            }
            return null;
        }
    }

    @NotNull
    @Override
    public File file() {
        return mappedFile.file();
    }

    @Override
    public boolean readOnly() {
        return mappedBytes.isBackingFileReadOnly();
    }

    @Override
    public String dump(WireType wireType) {
        return dump(wireType, false);
    }

    /**
     * Dumps the contents of the {@code Wire} in either a verbose or abbreviated format.
     *
     * @param wireType The type of wire being dumped.
     * @param abbrev   Whether the output should be abbreviated.
     * @return A string representing the contents of the wire.
     */
    private String dump(@NotNull WireType wireType, final boolean abbrev) {

        final MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        try {
            bytes.readLimit(bytes.realCapacity());
            Wire wire = wireType.apply(bytes);
            return Wires.fromSizePrefixedBlobs(wire, abbrev);
        } finally {
            bytes.releaseLast();
        }
    }

    @Override
    protected void performClose() {
        mappedBytes.close();
    }

    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender can be at different locations.
     */
    @NotNull
    @Override
    public MappedBytes bytes() {
        throwExceptionIfClosed();

        return MappedBytes.mappedBytes(mappedFile);
    }

    @NotNull
    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "wireType=" + wireType +
                ", mappedFile=" + mappedFile +
                '}';
    }

    /**
     * Writes the table store data into the given {@link WireOut} for marshalling.
     *
     * @param wire The wire into which the table store data is written.
     */
    @Override
    public void writeMarshallable(@NotNull final WireOut wire) {

        wire.write(MetaDataField.wireType).object(wireType);

        if (metadata != Metadata.NoMeta.INSTANCE)
            wire.write(MetaDataField.metadata).typedMarshallable(this.metadata);

        // align to a word whether needed or not as a micro-optimisation.
        wire.writeAlignTo(Integer.BYTES, 0);
    }

    /**
     * Acquires a {@link LongValue} mapped to a specific key in the table store.
     * If the key does not exist, it will create a new entry with the specified default value.
     * A failed creation releases its provisional binding; the original publication failure is retained.
     *
     * @param key          The key for the value to acquire.
     * @param defaultValue The default value to use if the key doesn't exist.
     * @return The acquired {@link LongValue}.
     */
    @Override
    public synchronized LongValue acquireValueFor(CharSequence key, final long defaultValue) {
        return acquireOrGetValueFor(key, defaultValue, true);
    }

    @Override
    public synchronized LongValue getValueFor(CharSequence key) {
        //! TableStoreTest#getValueForDoesNotCreateMissingKey requires lookup-only semantics.
        return acquireOrGetValueFor(key, 0, false);
    }

    private LongValue acquireOrGetValueFor(CharSequence key, long defaultValue, boolean createIfAbsent) {

        if (mappedBytes.isClosed())
            throw new ClosedIllegalStateException("Closed");

        mappedBytes.reserve(this);
        final long previousReadPosition = mappedBytes.readPosition();
        final long previousReadLimit = mappedBytes.readLimit();
        final long previousWritePosition = mappedBytes.writePosition();
        final long previousWriteLimit = mappedBytes.writeLimit();
        boolean restoreState = true;
        try {
            prepareForTableScan();
            while (mappedWire.readDataHeader()) {
                final int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;
                //! laterScanSeesEntriesAppendedByAnotherStore and scansWhileAnotherStoreAppendsKeys
        //! require caller cursors/limits and existing bindings to survive this structural scan.
        final long readPosition = mappedBytes.readPosition();
                final int length = Wires.lengthOf(header);
                final ValueIn valueIn = readEventIfNameEquals(mappedWire, key);
                if (valueIn != null) {
                    return valueIn.int64ForBinding(null);
                }
                mappedBytes.readPosition(readPosition + length);
            }
            if (!createIfAbsent)
                return null;
            if (mappedBytes.isBackingFileReadOnly())
                throw new IllegalStateException("key " + key + " does not exist in readOnly TableStore and cannot be created");
            mappedBytes.writeLimit(mappedBytes.realCapacity());
            long start = mappedBytes.readPosition();
            mappedBytes.writePosition(start);
            final long pos = mappedWire.enterHeader(256L);
            final LongValue longValue = wireType.newLongReference().get();
            boolean acquired = false;
            try {
                mappedWire.writeEventName(key).int64forBinding(defaultValue, longValue);
                mappedWire.writeAlignTo(Integer.BYTES, 0);
                mappedWire.updateHeader(pos, false, 0);
                long end = mappedBytes.writePosition();
                long chuckSize = mappedFile.chunkSize();
                long overlapSize = mappedFile.overlapSize();
                long endOfChunk = (start + chuckSize - 1) / chuckSize * chuckSize;
                if (end >= endOfChunk + overlapSize)
                    throw new IllegalStateException("Misaligned write");
                // Creation retains the write cursor; failed publication and lookup restore it.
                restoreState = false;
                acquired = true;
                return longValue;
            } finally {
                // Publication can fail after the binding reserves mapped bytes. The caller never owns that value.
                // FailedMetadataPublicationTest covers checked failures, runtime failures and errors.
                if (!acquired)
                    Closeable.closeQuietly(longValue);
            }

        } catch (StreamCorruptedException | EOFException e) {
            throw new IORuntimeException("Unable to acquire table value in " + file()
                    + ", readPosition=" + mappedBytes.readPosition() + ", readLimit=" + mappedBytes.readLimit()
                    + ", writePosition=" + mappedBytes.writePosition() + ", writeLimit=" + mappedBytes.writeLimit(), e);

        } finally {
            try {
                if (restoreState) {
                    mappedBytes.readPosition(0);
                    if (mappedBytes.writePosition() != previousWritePosition)
                        mappedBytes.writePosition(previousWritePosition);
                    mappedBytes.writeLimit(previousWriteLimit);
                    mappedBytes.readLimit(previousReadLimit);
                    mappedBytes.readPosition(previousReadPosition);
                }
            } finally {
                mappedBytes.release(this);
            }
        }
    }

    @Nullable
    private static ValueIn readEventIfNameEquals(WireIn wireIn, CharSequence expected) {
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            StringBuilder sb = stlSb.get();
            final ValueIn valueIn = wireIn.readEventName(sb);
            if (StringUtils.equalsCaseIgnore(expected, sb)) {
                return valueIn;
            }
            return null;
        }
    }

    @Override
    public synchronized <T> void forEachKey(T accumulator, TableStoreIterator<T> tsIterator) {
        mappedBytes.reserve(this);
        final long readPosition = mappedBytes.readPosition();
        final long readLimit = mappedBytes.readLimit();
        final long writePosition = mappedBytes.writePosition();
        final long writeLimit = mappedBytes.writeLimit();
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            StringBuilder sb = stlSb.get();
            prepareForTableScan();
            while (mappedWire.readDataHeader()) {
                final int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;
                final long recordPosition = mappedBytes.readPosition();
                final int length = Wires.lengthOf(header);
                final ValueIn valueIn = mappedWire.readEventName(sb);
                tsIterator.accept(accumulator, sb, valueIn);
                mappedBytes.readPosition(recordPosition + length);
            }

        } catch (EOFException e) {
            throw new IORuntimeException(e);

        } finally {
            try {
                mappedBytes.readPosition(0);
                // Resetting an unchanged cursor at a mapping boundary can map/grow
                // the next chunk while our caller still holds the structural lock.
                if (mappedBytes.writePosition() != writePosition)
                    mappedBytes.writePosition(writePosition);
                mappedBytes.writeLimit(writeLimit);
                mappedBytes.readLimit(readLimit);
                mappedBytes.readPosition(readPosition);
            } finally {
                mappedBytes.release(this);
            }
        }
    }

    @Override
    public <R> R doWithExclusiveLock(@NotNull final Function<TableStore<T>, ? extends R> code) {
        return doWithExclusiveLock(file(), code, () -> this);
    }

    @Override
    public T metadata() {
        return metadata;
    }

    @Override
    public boolean readOnly() {
        //! ReadonlyNamedTailerIndexesTest#readsNamedTailersWithoutWriteAccessOrMetadataMutation
        //! requires shared-lock lookup rather than an exclusive, potentially writing path.
        return mappedBytes.isBackingFileReadOnly();
    }

    private void prepareForTableScan() {
        mappedBytes.readPosition(0);
        final long scanLimit = mappedBytes.realCapacity();
        //! laterScanSeesEntriesAppendedByAnotherStore fails if this instance's stale local
        //! writeLimit truncates a subsequent scan after another process grows the table.
        mappedBytes.writeLimit(mappedBytes.capacity());
        mappedBytes.readLimit(scanLimit);
    }

}
