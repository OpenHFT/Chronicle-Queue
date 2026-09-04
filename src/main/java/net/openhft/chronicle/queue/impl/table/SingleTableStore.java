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
    @SuppressWarnings({"unused", "unchecked"})
    @UsedViaReflection
    private SingleTableStore(@NotNull final WireIn wire) {
        try {
            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();

            final WireType decodedWireType;
            try {
                decodedWireType = Objects.requireNonNull(
                        wire.read(MetaDataField.wireType).object(WireType.class),
                        "the table-store header has no wire type");
            } catch (CorruptTableStoreException e) {
                throw e;
            } catch (RuntimeException e) {
                //! The wire type is Queue-owned schema, not an extensible constructor hook. A missing or unreadable value
                //! proves that this header cannot describe a table store; retrying the file lock cannot repair it.
                //! SingleTableBuilderCorruptMetadataTest#partialStoreConstructionIsCorruptionAndReleasesTheMapping
                //! discriminates this field boundary and its partial-instance cleanup.
                throw new CorruptTableStoreException(mappedFile.file(),
                        "the first header has no readable wire type", e);
            }
            try {
                this.wireType = requireSupportedWireType(decodedWireType);
            } catch (IllegalArgumentException e) {
                throw new CorruptTableStoreException(mappedFile.file(),
                        "the first header uses unsupported wire type " + decodedWireType, e);
            }

            wire.consumePadding();
            if (wire.bytes().readRemaining() > 0) {
                final Object decodedMetadata = wire.read(MetaDataField.metadata).typedMarshallable();
                if (!(decodedMetadata instanceof Metadata)) {
                    //! Decode extensible metadata without an inferred cast, then reject and close the wrong type explicitly.
                    //! Otherwise the compiler-generated cast loses a decoded Closeable and leaks it behind a generic CCE.
                    //! SingleTableBuilderCorruptMetadataTest#nullAndWrongTypedNestedMetadataAreReportedAsCorruption
                    //! covers null and closeable wrong-type values.
                    Closeable.closeQuietly(decodedMetadata);
                    throw new CorruptTableStoreException(mappedFile.file(),
                            "the first header holds " + (decodedMetadata == null
                                    ? "null" : decodedMetadata.getClass().getName()) + " instead of metadata");
                }
                this.metadata = (T) decodedMetadata;
            } else {
                @SuppressWarnings("unchecked")
                T instance = (T) Metadata.NoMeta.INSTANCE;
                this.metadata = instance;
            }

            mappedWire = wireType.apply(mappedBytes);
            mappedWire.usePadding(true);
        } catch (RuntimeException | Error e) {
            //! Demarshallable construction registers this closeable before all persisted fields are decoded. If decoding
            //! fails, no caller receives the partial store and only this path can retire it. The
            //! SingleTableBuilderCorruptMetadataTest#partialStoreConstructionIsCorruptionAndReleasesTheMapping test
            //! fails through resource tracing if the partial instance is left registered.
            // AbstractCloseable registered this instance before the read started. The caller never
            // receives a reference that it could close, so close it here.
            close();
            throw e;
        }

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
        this.wireType = requireSupportedWireType(wireType);
        this.metadata = metadata;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        mappedWire = wireType.apply(mappedBytes);
        mappedWire.usePadding(true);

        singleThreadedCheckDisabled(true);
    }

    //! Apply the same fail-closed format gate to newly selected and persisted wire types. Otherwise a crafted first header
    //! can bypass the builder's pre-file check and recreate an unbounded record reader after deserialisation.
    //! SingleTableStoreCorruptRecordTest#unsupportedWireTypesAreRejectedBeforeCreatingAFile and
    //! SingleTableBuilderCorruptMetadataTest#unsupportedPersistedWireTypeIsCorruptionAndReleasesTheMapping cover both edges.
    //! SingleTableStoreCorruptRecordTest#supportedBinaryWireTypesRoundTripBoundValues proves the four BinaryWire variants
    //! share the tagged bound-long grammar; #rawTableStoreStillScansValuesWrittenDuringItsInitialOpen preserves bounded RAW.
    static WireType requireSupportedWireType(WireType wireType) {
        final WireType required = Objects.requireNonNull(wireType);
        switch (required) {
            case BINARY:
            case BINARY_LIGHT:
            case FIELDLESS_BINARY:
            case COMPRESSED_BINARY:
            case RAW:
                return required;
            default:
                throw new IllegalArgumentException("Table stores require a binary or raw WireType, not " + required);
        }
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
     * <p>
     * Only a failure to take the lock is retried, until {@code chronicle.table.store.timeoutMS} expires.
     * A failure thrown by the code block propagates to the caller unchanged.
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
        final String type = shared ? "shared" : "exclusive";
        final StandardOpenOption readOrWrite = shared ? StandardOpenOption.READ : StandardOpenOption.WRITE;

        //! A wall-clock correction could previously shorten the retry window or extend it indefinitely.
        //! A monotonic elapsed-time comparison keeps the configured lock timeout stable. There is no
        //! deterministic public seam for moving the wall clock during this private loop, so
        //! SingleTableStoreLockTest's shared/exclusive contention cases retain integration evidence for the retry loop.
        final long startNanos = System.nanoTime();
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMS);
        Throwable lastAcquisitionFailure = null;
        try (final FileChannel channel = FileChannel.open(file.toPath(), readOrWrite)) {
            for (int count = 1; System.nanoTime() - startNanos < timeoutNanos; count++) {
                FileLock fileLock = null;
                try {
                    fileLock = channel.tryLock(EXCLUSIVE_LOCK_START, EXCLUSIVE_LOCK_SIZE, shared);
                } catch (OverlappingFileLockException e) {
                    //! FileChannel.tryLock reports inter-process contention with null and same-JVM
                    //! contention with OverlappingFileLockException. Both can clear, so retry them;
                    //! SingleTableStoreLockTest.sharedContentionIsRetriedBeforeBodyRunsOnce and
                    //! exclusiveContentionIsRetriedBeforeBodyRunsOnce prove the same-JVM signal waits
                    //! and then runs the body exactly once. Exercising null requires a subprocess and
                    //! platform-specific file-lock fixture, so that specified API branch is not directly tested.
                    //! In contrast, IOException is the API's terminal "other I/O error" signal; retrying
                    //! it hid permanent failures and discarded their cause. A portable public API cannot
                    //! deterministically inject that branch.
                    lastAcquisitionFailure = e;
                    // failed to acquire the lock, wait until other operation completes
                    if (count > 9) {
                        if (Jvm.isDebugEnabled(SingleTableStore.class)) {
                            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                            final String message = "Failed to acquire " + type
                                    + " lock on the table store file. Retrying, file=" + file.getAbsolutePath()
                                    + ", count=" + count + ", elapsed=" + elapsedMs + " ms";
                            Jvm.debug().on(SingleTableStore.class, "", new StackTrace(message));
                        }
                    }
                }
                if (fileLock != null) {
                    //! Keep the body outside the acquisition handlers so even an
                    //! OverlappingFileLockException thrown by user code is not mistaken for contention.
                    //! The BodyFailed boundary makes all body Throwables bypass the surrounding IOException
                    //! handler, while explicit cleanup retains lock/channel close failures as suppressed.
                    //! SingleTableStoreLockTest.exclusiveBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity
                    //! and sharedBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity prove one execution
                    //! and the exact failure instance. FileLock/FileChannel do not offer a public, portable
                    //! close-failure injection seam, so generated cleanup suppression is not directly tested.
                    BodyFailed bodyFailed = null;
                    try {
                        return code.apply(target.get());
                    } catch (Throwable t) {
                        bodyFailed = new BodyFailed(t);
                        throw bodyFailed;
                    } finally {
                        try {
                            fileLock.close();
                        } catch (Throwable closeFailure) {
                            if (bodyFailed != null)
                                bodyFailed.addSuppressed(closeFailure);
                            else
                                throw Jvm.rethrow(closeFailure);
                        }
                    }
                }
                int delay = Math.min(250, count * count);
                Jvm.pause(delay);
            }
            //! Preserve the last concrete same-JVM contention signal on timeout. Null is the only
            //! inter-process contention result exposed by FileChannel, so there is no cause to retain for it.
            //! Throw before the channel resource closes so a close failure is suppressed on the timeout
            //! rather than replacing it. A fast deterministic test would require an injectable lock or
            //! mutable static timeout; the contention tests retain the public surface without either seam.
            final IllegalStateException timeout = new IllegalStateException(
                    "Unable to claim the " + type + " lock on file " + file);
            if (lastAcquisitionFailure != null)
                timeout.initCause(lastAcquisitionFailure);
            throw timeout;
        } catch (BodyFailed e) {
            //! Lock and channel cleanup run while BodyFailed is primary, so Java attaches their failures
            //! to the wrapper. Move those suppressed failures onto the original body Throwable before
            //! rethrowing it; otherwise unwrapping would silently discard cleanup evidence.
            final Throwable bodyFailure = e.getCause();
            for (Throwable suppressed : e.getSuppressed())
                bodyFailure.addSuppressed(suppressed);
            throw Jvm.rethrow(bodyFailure);
        } catch (IOException e) {
            //! Once cleanup failures are no longer discarded, IOException can come from opening,
            //! acquiring, releasing, or closing the lock channel. Report that honest common boundary
            //! and retain the concrete cause; the public FileChannel path has no portable failure injector.
            throw new IllegalStateException("I/O failure while using the " + type + " file lock on " + file, e);
        }
    }

    /**
     * Carries a failure thrown by the locked body past the handlers that exist for the lock itself.
     */
    private static final class BodyFailed extends RuntimeException {
        private static final long serialVersionUID = 0L;

        BodyFailed(Throwable cause) {
            super(cause);
        }
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
        //! The partial-construction failure above can close this instance before mappedBytes is assigned. The
        //! SingleTableBuilderCorruptMetadataTest#partialStoreConstructionIsCorruptionAndReleasesTheMapping test retains
        //! the original decode failure and rejects an unexpected close-time NullPointerException if this guard is removed.
        // mappedBytes is null when the deserialization constructor failed before it assigned the field
        if (mappedBytes != null)
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

        if (mappedBytes.isClosed())
            throw new ClosedIllegalStateException("Closed");

        mappedBytes.reserve(this);
        try {
            //! SingleTableStoreCorruptRecordTest#shorterPositiveRecordIsRejected proves matching, later-target and
            //! missing-key lookups validate visited records; #laterCorruptRecordIsRejectedWhenVisited proves a healthy
            //! earlier match may return while traversal to later or missing keys reaches and rejects the later fault.
            //! #longerInRangeRecordIsRejected catches a declared end that silently misaligns the following scan.
            final LongValue existing = readTableStoreRecords((recordKey, valueIn) ->
                    StringUtils.equalsCaseIgnore(key, recordKey) ? valueIn.int64ForBinding(null) : null);
            if (existing != null)
                return existing;

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
            return longValue;

        } catch (StreamCorruptedException | EOFException e) {
            throw new IORuntimeException(e);

        } finally {
            mappedBytes.release(this);
        }
    }

    @Nullable
    private <R> R readTableStoreRecords(TableStoreRecordReader<R> reader) throws EOFException {
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            StringBuilder sb = stlSb.get();

            mappedBytes.readPosition(0);
            // A read limit above writeLimit can fail while another TableStore is being written.
            //! Snapshot the logical write limit before scanning. Using real capacity alone lets a reader interpret
            //! preallocated zero space, while a limit beyond writeLimit can fail during another process's publication.
            //! SingleTableStoreCorruptRecordTest#anIntactTableStoreReadsBackEveryKeyItWrote exercises this scan through
            //! missing-key creation, then matching and iteration callbacks after reopening the mapped file.
            final long scanLimit = Math.min(mappedBytes.writeLimit(), mappedBytes.realCapacity());
            mappedBytes.readLimit(scanLimit);
            positionAfterTableStoreHeader(scanLimit);
            while (true) {
                final WireIn.HeaderType headerType = mappedWire.readDataHeader(true);
                if (headerType == WireIn.HeaderType.NONE || headerType == WireIn.HeaderType.EOF)
                    break;
                final long recordStart = mappedBytes.readPosition();
                //! Only the first document is table-store metadata. SingleTableStoreCorruptRecordTest
                //! #laterMetadataHeaderIsRejected proves that asking Wire to skip all metadata would hide a corrupted record,
                //! omit its key from iteration and allow acquisition to append a duplicate.
                if (headerType != WireIn.HeaderType.DATA)
                    throw new CorruptTableStoreException(file(), "the record at " + recordStart
                            + " is marked as metadata instead of data");

                final int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;

                final long bodyStart = mappedBytes.readPosition();
                final int length = Wires.lengthOf(header);
                //! Subtraction avoids overflow while proving the declared end remains inside the snapshotted scan limit.
                //! SingleTableStoreCorruptRecordTest#recordBeyondTheReadLimitIsRejected checks this exact diagnostic,
                //! before Wire can turn the invalid boundary into an unrelated parsing failure.
                if (length > scanLimit - bodyStart)
                    throw new CorruptTableStoreException(file(), "the record at " + recordStart
                            + " declares a length of " + length + ", which does not fit inside the read limit of " + scanLimit);

                final long recordEnd = bodyStart + length;
                final ValueIn valueIn;
                final long valueStart;
                mappedBytes.readLimit(recordEnd);
                try {
                    valueIn = mappedWire.readEventName(sb);
                    valueStart = mappedBytes.readPosition();
                    validateRecordConsumption(recordStart, recordEnd);
                } catch (CorruptTableStoreException e) {
                    throw e;
                } catch (RuntimeException e) {
                    //! Parser failures while constrained to this record prove that its declared body is unreadable.
                    //! SingleTableStoreCorruptRecordTest#shorterPositiveRecordIsRejected fails if an under-length body
                    //! escapes as a lower-level Wire exception instead of the table-store corruption contract.
                    throw new CorruptTableStoreException(file(), "the record at " + recordStart
                            + " cannot be read within its declared length of " + length, e);
                } finally {
                    mappedBytes.readLimit(scanLimit);
                }

                mappedBytes.readPosition(valueStart);
                mappedBytes.readLimit(recordEnd);
                try {
                    //! Every supported record receives a bounded dry read before reset; callbacks may read or ignore the
                    //! value but cannot influence the next record boundary. The intact-table test exercises both forms.
                    final R result = reader.read(sb, valueIn);
                    if (result != null)
                        return result;
                } finally {
                    mappedBytes.readLimit(scanLimit);
                    mappedBytes.readPosition(recordEnd);
                }
            }
            return null;
        }
    }

    private void positionAfterTableStoreHeader(long scanLimit) throws EOFException {
        //! The table-store schema permits metadata only in its first document. Validating that document explicitly prevents
        //! the generic Wire scanner from skipping a misplaced metadata record and hiding a later key.
        //! SingleTableStoreCorruptRecordTest#laterMetadataHeaderIsRejected discriminates that classification.
        final WireIn.HeaderType headerType = mappedWire.readDataHeader(true);
        if (headerType != WireIn.HeaderType.META_DATA)
            throw new CorruptTableStoreException(file(), "the first record is not table-store metadata");

        final int header = mappedBytes.readVolatileInt();
        final long bodyStart = mappedBytes.readPosition();
        final int length = Wires.lengthOf(header);
        if (length > scanLimit - bodyStart)
            throw new CorruptTableStoreException(file(), "the table-store header declares a length of " + length
                    + ", which does not fit inside the read limit of " + scanLimit);
        mappedBytes.readPosition(bodyStart + length);
    }

    //! SingleTableStoreCorruptRecordTest#longerInRangeRecordIsRejected discriminates exact body consumption, while
    //! #nonLongValueCodeIsRejectedByEveryTraversal and #malformedBinaryPaddingIsRejected cover value schema and padding.
    //! Bounding the Wire before parsing prevents #shorterPositiveRecordIsRejected from borrowing successor bytes.
    //! Only Wire's explicit padding codes are accepted between the value and the declared end.
    private void validateRecordConsumption(long recordStart, long recordEnd) {
        if (wireType == WireType.RAW) {
            //! RAW encodes a bound long as eight untagged bytes followed by zero alignment padding. The
            //! SingleTableStoreCorruptRecordTest#rawTableStoreStillScansValuesWrittenDuringItsInitialOpen test fails if
            //! this bounded legacy path is rejected or sent through BinaryWire's tagged-value parser, while
            //! #rawRecordLengthsAreBoundedDuringTheInitialOpen rejects short and long declared bodies without mutation.
            validateRawRecordConsumption(recordStart, recordEnd);
        } else {
            consumeExplicitBinaryPadding(recordStart);
            if (mappedBytes.readRemaining() == 0)
                throw new CorruptTableStoreException(file(), "the record at " + recordStart + " holds no value");

            //! TableStore records contain bound longs, not arbitrary same-width Wire values. The
            //! SingleTableStoreCorruptRecordTest#nonLongValueCodeIsRejectedByEveryTraversal test replaces INT64 with
            //! FLOAT64 without changing the record length and fails if generic skipValue validation is restored.
            final int valueCode = mappedBytes.peekUnsignedByte();
            if (valueCode != 0 && valueCode != BinaryWireCode.INT64)
                throw new CorruptTableStoreException(file(), "the record at " + recordStart
                        + " holds Wire type 0x" + Integer.toHexString(valueCode) + " instead of a bound long");
            // BinaryWire's binding reader accepts zero as a legacy long marker and consumes its following eight bytes.
            mappedBytes.readSkip(1L + Long.BYTES);
            consumeExplicitBinaryPadding(recordStart);
        }

        final long actualEnd = mappedBytes.readPosition();
        if (actualEnd != recordEnd)
            throw new CorruptTableStoreException(file(), "the record at " + recordStart + " declares an end at "
                    + recordEnd + " but its content ends at " + actualEnd);
    }

    private void validateRawRecordConsumption(long recordStart, long recordEnd) {
        //! Enforce RAW's complete bound-long and exact zero-padding shape without changing its initial-open compatibility.
        //! SingleTableStoreCorruptRecordTest#rawTableStoreStillScansValuesWrittenDuringItsInitialOpen guards that path;
        //! reopening a RAW typed header remains a pre-existing RawWire limitation and is not reclassified here.
        if (mappedBytes.readRemaining() < Long.BYTES)
            throw new CorruptTableStoreException(file(), "the RAW record at " + recordStart
                    + " ends before its bound long");
        mappedBytes.readSkip(Long.BYTES);

        final long expectedPadding = (-mappedBytes.readPosition()) & (Integer.BYTES - 1L);
        if (mappedBytes.readRemaining() != expectedPadding)
            throw new CorruptTableStoreException(file(), "the RAW record at " + recordStart + " declares an end at "
                    + recordEnd + " but requires " + expectedPadding + " padding bytes");
        while (mappedBytes.readRemaining() > 0) {
            if (mappedBytes.readUnsignedByte() != 0)
                throw new CorruptTableStoreException(file(), "the RAW record at " + recordStart
                        + " contains non-zero alignment padding");
        }
    }

    private void consumeExplicitBinaryPadding(long recordStart) {
        //! Accept only Wire's one-byte and length-prefixed padding encodings, and bound PADDING32 before skipping it.
        //! Otherwise malformed padding can consume the next record while still satisfying the outer mapped-file limit.
        //! SingleTableStoreCorruptRecordTest#malformedBinaryPaddingIsRejected covers truncated and oversized PADDING32.
        while (mappedBytes.readRemaining() > 0) {
            final int code = mappedBytes.peekUnsignedByte();
            if (code == BinaryWireCode.PADDING) {
                mappedBytes.readSkip(1);
                continue;
            }
            if (code != BinaryWireCode.PADDING32)
                return;

            if (mappedBytes.readRemaining() < 5)
                throw new CorruptTableStoreException(file(), "the record at " + recordStart
                        + " ends inside a padding header");
            mappedBytes.readSkip(1);
            final long padding = mappedBytes.readUnsignedInt();
            if (padding > mappedBytes.readRemaining())
                throw new CorruptTableStoreException(file(), "the record at " + recordStart
                        + " declares " + padding + " padding bytes with only " + mappedBytes.readRemaining() + " remaining");
            mappedBytes.readSkip(padding);
        }
    }

    @FunctionalInterface
    private interface TableStoreRecordReader<R> {
        @Nullable
        R read(CharSequence key, ValueIn valueIn);
    }

    @Override
    public synchronized <T> void forEachKey(T accumulator, TableStoreIterator<T> tsIterator) {
        mappedBytes.reserve(this);
        try {
            //! SingleTableStoreCorruptRecordTest#longerInRangeRecordIsRejected requires iteration to use the same exact
            //! traversal as acquisition. #anIntactTableStoreReadsBackEveryKeyItWrote proves callbacks may consume or
            //! ignore ValueIn: validation resets the reader before the callback, then advances to the declared end.
            readTableStoreRecords((key, valueIn) -> {
                tsIterator.accept(accumulator, key, valueIn);
                return null;
            });

        } catch (EOFException e) {
            throw new IORuntimeException(e);

        } finally {
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
}
