/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
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
//! SingleTableStoreSharedLockTimeoutTest#positiveTimeoutAcquiresAContendedLockAfterRelease and
//! #zeroTimeoutPerformsOneAttemptWhenTheStructuralLockIsHeld require monotonic bounded waiting.
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
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
        return doWithLock(file, code, target, true);
    }

    /**
     * Executes a code block with a shared structural lock, waiting for no longer than the
     * supplied timeout. This overload is intended for off-critical-path metadata snapshots whose
     * caller owns the timeout policy.
     *
     * @param file          the table-store file to lock
     * @param timeoutMillis maximum time to wait in milliseconds; zero performs one attempt
     * @param code          the function to execute while the lock is held
     * @param target        supplier of the function target
     * @param <T>           target type
     * @param <R>           result type
     * @return the result of applying {@code code}
     * @throws IllegalArgumentException if {@code timeoutMillis} is negative
     */
    public static <T, R> R doWithSharedLock(@NotNull final File file,
                                            final long timeoutMillis,
                                            @NotNull final Function<T, ? extends R> code,
                                            @NotNull final Supplier<T> target) {
        //! negativeSharedLockTimeoutIsRejected keeps invalid policy input distinct from lock
        //! contention, while callerCanSupplySharedLockTimeout and
        //! zeroTimeoutPerformsOneAttemptWhenTheStructuralLockIsHeld pin one attempt at zero.
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
        return doWithLock(file, code, target, false);
    }

    /**
     * Handles file locking and executes the provided code block.
     *
     * @param file    The file to lock.
     * @param code    The function to execute.
     * @param target  The target supplier.
     * @param shared  Whether to use a shared lock.
     * @param <T>     Type of the target object.
     * @param <R>     Return type of the function.
     * @return The result of the function applied to the target.
     */
    private static <T, R> R doWithLock(@NotNull final File file,
                                       @NotNull final Function<T, ? extends R> code,
                                       @NotNull final Supplier<T> target,
                                       final boolean shared) {
        return doWithLock(file, code, target, shared, timeoutMS);
    }

    private static <T, R> R doWithLock(@NotNull final File file,
                                       @NotNull final Function<T, ? extends R> code,
                                       @NotNull final Supplier<T> target,
                                       final boolean shared,
                                       final long timeoutMillis) {
        final String type = shared ? "shared" : "exclusive";
        final StandardOpenOption readOrWrite = shared ? StandardOpenOption.READ : StandardOpenOption.WRITE;

        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        final long startNanos = System.nanoTime();
        final long startMs = System.currentTimeMillis();
        try (final FileChannel channel = FileChannel.open(file.toPath(), readOrWrite)) {
            //! callerCanSupplySharedLockTimeout fails if a zero timeout skips the initial tryLock;
            //! positiveTimeoutAcquiresAContendedLockAfterRelease requires bounded retries thereafter.
            for (int count = 1; ; count++) {
                try (FileLock fileLock = channel.tryLock(EXCLUSIVE_LOCK_START, EXCLUSIVE_LOCK_SIZE, shared)) {
                    if (fileLock != null) {
                        return code.apply(target.get());
                    }
                } catch (IOException | OverlappingFileLockException e) {
                    // failed to acquire the lock, wait until other operation completes
                    if (count > 9) {
                        if (Jvm.isDebugEnabled(SingleTableStore.class)) {
                            final long elapsedMs = System.currentTimeMillis() - startMs;
                            final String message = "Failed to acquire " + type + " lock on the table store file. Retrying, file=" + file.getAbsolutePath() + ", count=" + count + ", elapsed=" + elapsedMs + " ms";
                            Jvm.debug().on(SingleTableStore.class, "", new StackTrace(message));
                        }
                    }
                }
                final long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
                //! SingleTableStoreSharedLockTimeoutTest#positiveTimeoutAcquiresAContendedLockAfterRelease
                //! requires retries to use only the remaining monotonic budget; an unconditional
                //! quadratic pause can overshoot the caller's bound or miss a timely lock release.
                if (remainingNanos <= 0)
                    break;
                final long backoffMillis = Math.min(250L, (long) count * count);
                final long delayNanos = Math.min(
                        TimeUnit.MILLISECONDS.toNanos(backoffMillis),
                        remainingNanos);
                LockSupport.parkNanos(delayNanos);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't perform operation with " + type + " file lock", e);
        }
        throw new IllegalStateException("Unable to claim exclusive " + type + " lock on file " + file);
    }

    @NotNull
    @Override
    public File file() {
        return mappedFile.file();
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
        //! TableStoreTest#getValueForDoesNotCreateMissingKey distinguishes lookup from acquire;
        //! destructive planning must not mutate metadata while checking optional state.
        return acquireOrGetValueFor(key, 0, false);
    }

    private LongValue acquireOrGetValueFor(CharSequence key, final long defaultValue, boolean createIfAbsent) {

        if (mappedBytes.isClosed())
            throw new ClosedIllegalStateException("Closed");

        mappedBytes.reserve(this);
        //! SingleTableStoreForEachKeyGuardTest#laterScanSeesEntriesAppendedByAnotherStore requires
        //! lookup to restore all four caller-visible cursors and limits after observing growth.
        final long previousReadPosition = mappedBytes.readPosition();
        final long previousReadLimit = mappedBytes.readLimit();
        final long previousWritePosition = mappedBytes.writePosition();
        final long previousWriteLimit = mappedBytes.writeLimit();
        boolean restoreScanState = true;
        try {
            prepareForTableScan();
            while (mappedWire.readDataHeader()) {
                final int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;
                final long readPosition = mappedBytes.readPosition();
                final int length = Wires.lengthOf(header);
                final ValueIn valueIn = readEventIfNameEquals(mappedWire, key);
                if (valueIn != null) {
                    return valueIn.int64ForBinding(null);
                }
                mappedBytes.readPosition(readPosition + length);
            }
            if (!createIfAbsent) {
                //! TableStoreTest#getValueForDoesNotCreateMissingKey demonstrates that lookup must
                //! return before the legacy acquire path appends a durable table entry.
                return null;
            }
            if (mappedBytes.isBackingFileReadOnly())
                throw new IllegalStateException("key " + key + " does not exist in readOnly TableStore and cannot be created");
            mappedBytes.writeLimit(mappedBytes.realCapacity());
            long start = mappedBytes.readPosition();
            mappedBytes.writePosition(start);
            final long pos = mappedWire.enterHeader(256L);
            final LongValue longValue = wireType.newLongReference().get();
            mappedWire.writeEventName(key).int64forBinding(defaultValue, longValue);
            mappedWire.writeAlignTo(Integer.BYTES, 0);
            mappedWire.updateHeader(pos, false, 0);
            long end = mappedBytes.writePosition();
            long chuckSize = mappedFile.chunkSize();
            long overlapSize = mappedFile.overlapSize();
            long endOfChunk = (start + chuckSize - 1) / chuckSize * chuckSize;
            if (end >= endOfChunk + overlapSize)
                throw new IllegalStateException("Misaligned write");
            //! laterScanSeesEntriesAppendedByAnotherStore requires lookup paths to restore the
            //! caller's limits; a completed creation deliberately keeps acquire's write position.
            restoreScanState = false;
            return longValue;

        } catch (StreamCorruptedException | EOFException e) {
            throw new IORuntimeException(e);

        } finally {
            if (restoreScanState)
                restoreAfterTableScan(previousReadPosition, previousReadLimit,
                        previousWritePosition, previousWriteLimit);
            mappedBytes.release(this);
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
        //! SingleTableStoreForEachKeyGuardTest#laterScanSeesEntriesAppendedByAnotherStore requires a
        //! structural scan to restore read/write positions and limits while a previously bound value
        //! remains usable. #scansWhileAnotherStoreAppendsKeys supplies concurrent stress coverage.
        final long previousReadPosition = mappedBytes.readPosition();
        final long previousReadLimit = mappedBytes.readLimit();
        final long previousWritePosition = mappedBytes.writePosition();
        final long previousWriteLimit = mappedBytes.writeLimit();
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            StringBuilder sb = stlSb.get();
            prepareForTableScan();
            while (mappedWire.readDataHeader()) {
                final int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;
                final long readPosition = mappedBytes.readPosition();
                final int length = Wires.lengthOf(header);
                final ValueIn valueIn = mappedWire.readEventName(sb);
                tsIterator.accept(accumulator, sb, valueIn);
                mappedBytes.readPosition(readPosition + length);
            }

        } catch (EOFException e) {
            throw new IORuntimeException(e);

        } finally {
            //! laterScanSeesEntriesAppendedByAnotherStore fails if this scan leaks any cursor or
            //! limit into later binding operations; the concurrent test supplies stress coverage.
            restoreAfterTableScan(previousReadPosition, previousReadLimit,
                    previousWritePosition, previousWriteLimit);
            mappedBytes.release(this);
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

    private void restoreAfterTableScan(long readPosition, long readLimit,
                                       long writePosition, long writeLimit) {
        mappedBytes.readPosition(0);
        mappedBytes.readLimit(readLimit);
        mappedBytes.writePosition(writePosition);
        mappedBytes.writeLimit(writeLimit);
        mappedBytes.readPosition(readPosition);
    }
}
