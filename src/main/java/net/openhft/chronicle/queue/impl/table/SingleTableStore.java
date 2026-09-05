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
import net.openhft.chronicle.core.util.ClassNotFoundRuntimeException;
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

    //! Use a package-owned marker for Queue-established schema failures during reflective construction. Searching for the
    //! public corruption API in arbitrary constructor causes lets application code relabel its own failure as disk corruption.
    //! SingleTableBuilderCorruptMetadataTest#applicationCorruptionExceptionIsNotPromoted protects that origin boundary.
    static final class SchemaCorruptionException extends RuntimeException {
        private static final long serialVersionUID = 0L;

        SchemaCorruptionException(String message) {
            super(message);
        }

        SchemaCorruptionException(String message, Throwable observed) {
            super(message + " (decoder reported " + observed.getClass().getName() + ")");
        }
    }

    static final class MetadataConstructorFailed extends RuntimeException {
        private static final long serialVersionUID = 0L;

        MetadataConstructorFailed(Throwable cause) {
            super(cause);
        }
    }

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
    SingleTableStore(@NotNull final WireIn wire) {
        try {
            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();

            final WireType decodedWireType;
            try {
                decodedWireType = Objects.requireNonNull(
                        readTableStoreField(wire, MetaDataField.wireType).object(WireType.class),
                        "the table-store header has no wire type");
            } catch (RuntimeException e) {
                //! The wire type is Queue-owned schema, not an extensible constructor hook. A missing or unreadable value
                //! proves that this header cannot describe a table store; retrying the file lock cannot repair it.
                //! SingleTableBuilderCorruptMetadataTest#partialStoreConstructionIsCorruptionAndReleasesTheMapping
                //! discriminates this field boundary and its partial-instance cleanup.
                throw new SchemaCorruptionException("the first header has no readable wire type", e);
            }
            try {
                this.wireType = requirePersistedWireType(decodedWireType);
            } catch (IllegalArgumentException e) {
                throw new SchemaCorruptionException("the first header uses an unsupported wire type", e);
            }

            final ValueIn metadataValue;
            try {
                consumeTableStorePadding(wire);
                metadataValue = wire.bytes().readRemaining() > 0
                        ? readTableStoreField(wire, MetaDataField.metadata)
                        : null;
            } catch (RuntimeException e) {
                //! Padding and the metadata field token belong to Queue's persisted schema, before any extensible constructor
                //! is entered. SingleTableBuilderCorruptMetadataTest
                //! #metadataFieldFramingFailureIsCorruptionWithoutConstruction fails if malformed framing is attributed to
                //! application code or allowed to invoke that code.
                throw new SchemaCorruptionException("the first header does not hold readable metadata", e);
            }
            if (metadataValue != null) {
                final Class<? extends Metadata> metadataType = preflightMetadataType(metadataValue);
                final Object decodedMetadata;
                try {
                    decodedMetadata = metadataValue.applyToMarshallable(nestedWire -> {
                        final long containingWriteLimit = mappedBytes.writeLimit();
                        try {
                            //! Constrain ordinary Wire framing and reads to the metadata body's declared boundary. This is not a
                            //! sandbox for an extensible constructor that deliberately mutates the Bytes cursor or limits.
                            //! The outer builder has already capped this value to the containing STStore marshallable.
                            mappedBytes.writeLimit(mappedBytes.readLimit());
                            return Demarshallable.newInstance(metadataType, nestedWire);
                        } catch (Throwable e) {
                            throw new MetadataConstructorFailed(e);
                        } finally {
                            mappedBytes.writeLimit(containingWriteLimit);
                        }
                    });
                } catch (MetadataConstructorFailed e) {
                    throw e;
                } catch (RuntimeException e) {
                    //! Framing around the nested marshallable is persisted Queue schema, while only the callback invokes
                    //! extensible application code. Keep the two origins distinct and redact parser diagnostics.
                    //! SingleTableBuilderCorruptMetadataTest#nestedMetadataFramingFailureIsCorruptionAndRedacted
                    //! fails if malformed framing is returned as an application failure or leaks persisted content.
                    throw new SchemaCorruptionException("the first header does not hold readable metadata", e);
                }
                if (!(decodedMetadata instanceof Metadata)) {
                    //! Decode extensible metadata without an inferred cast, then reject and close the wrong type explicitly.
                    //! Otherwise the compiler-generated cast loses a decoded Closeable and leaks it behind a generic CCE.
                    //! SingleTableBuilderCorruptMetadataTest#nullAndWrongTypedNestedMetadataAreReportedAsCorruption
                    //! covers null and closeable wrong-type values.
                    Closeable.closeQuietly(decodedMetadata);
                    throw new SchemaCorruptionException("the first header does not hold metadata");
                }
                this.metadata = (T) decodedMetadata;
            } else {
                @SuppressWarnings("unchecked")
                T instance = (T) Metadata.NoMeta.INSTANCE;
                this.metadata = instance;
            }

            try {
                //! The Queue-owned STStore body may end with BinaryWire alignment padding, but not comments, an unrecognised
                //! field or an arbitrary trailing value. Consume only explicit padding before the builder verifies the body end.
                //! SingleTableBuilderCorruptMetadataTest#trailingTableStoreCommentIsCorruption loses a generic consumePadding.
                //! #writerProducedFinalPaddingAcrossAllAlignmentsReopens covers the current writer's four alignment outcomes;
                //! it is current-encoding compatibility evidence, not a claim about released historical fixtures.
                consumeTableStorePadding(wire);
            } catch (RuntimeException e) {
                throw new SchemaCorruptionException("the table-store body has unreadable trailing padding", e);
            }

            mappedWire = WireType.BINARY_LIGHT.apply(mappedBytes);
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

    private static Class<? extends Metadata> preflightMetadataType(ValueIn metadataValue) {
        //! Resolve the persisted metadata alias before invoking its extensible constructor. This lets an unavailable alias
        //! be reported without retaining attacker-controlled text, while a ClassNotFoundRuntimeException thrown by a known
        //! metadata constructor remains that application's failure. SingleTableBuilderCorruptMetadataTest
        //! #nestedMetadataAliasAndConstructorFailuresAreNotMisreportedAsCorruption discriminates both origins.
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            if (!metadataValue.isTyped())
                throw new SchemaCorruptionException("the first header does not hold metadata");
            final StringBuilder typeName = stlSb.get();
            try {
                metadataValue.typePrefix(typeName, StringBuilder::append);
            } catch (RuntimeException e) {
                throw new SchemaCorruptionException("the first header does not hold readable metadata", e);
            }
            final Class<?> metadataType;
            try {
                metadataType = metadataValue.classLookup().forName(typeName);
            } catch (ClassNotFoundRuntimeException e) {
                throw new MetadataConstructorFailed(
                        new IORuntimeException("The persisted table-store metadata type is unavailable"));
            }
            if (!Metadata.class.isAssignableFrom(metadataType))
                throw new SchemaCorruptionException("the first header does not hold metadata");
            return metadataType.asSubclass(Metadata.class);
        }
    }

    private static void consumeTableStorePadding(WireIn wire) {
        while (wire.bytes().readRemaining() > 0) {
            final int code = wire.bytes().peekUnsignedByte();
            if (code == BinaryWireCode.PADDING) {
                wire.bytes().readSkip(1L);
                continue;
            }
            if (code != BinaryWireCode.PADDING32)
                return;
            if (wire.bytes().readRemaining() < 5L)
                throw new SchemaCorruptionException("the table-store body ends inside a padding header");
            wire.bytes().readSkip(1L);
            final long padding = wire.bytes().readUnsignedInt();
            if (padding > wire.bytes().readRemaining())
                throw new SchemaCorruptionException("the table-store body has padding beyond its declared end");
            wire.bytes().readSkip(padding);
        }
    }

    private static ValueIn readTableStoreField(WireIn wire, MetaDataField expected) {
        //! STStore is a closed, ordered Queue-owned schema: general WireKey lookup scans over unknown fields and makes their
        //! acceptance depend on where they occur. Read the immediate field and validate its name instead.
        //! SingleTableBuilderCorruptMetadataTest#unknownTableStoreFieldsAreRejectedAtEveryPosition loses this boundary.
        final int code = wire.bytes().peekUnsignedByte();
        if (code != BinaryWireCode.FIELD_NAME_ANY
                && (code < BinaryWireCode.FIELD_NAME0 || code > BinaryWireCode.FIELD_NAME31))
            throw new SchemaCorruptionException("the table-store body does not contain the expected field");
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            final StringBuilder actual = stlSb.get();
            final ValueIn value = wire.read(actual);
            if (!StringUtils.isEqual(actual, expected.name()))
                throw new SchemaCorruptionException("the table-store body does not contain the expected field");
            return value;
        }
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
        this.wireType = requirePersistedWireType(wireType);
        this.metadata = metadata;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        //! Retain the BINARY label when reading/writing its header, but use only the canonical BINARY_LIGHT record codec.
        //! SingleTableStoreCorruptRecordTest#canonicalAndLegacyBinarySelectorsRoundTripTheCommonSubset covers both labels.
        mappedWire = WireType.BINARY_LIGHT.apply(mappedBytes);
        mappedWire.usePadding(true);

        singleThreadedCheckDisabled(true);
    }

    //! BINARY_LIGHT is the canonical table-store format. BINARY remains a legacy selector, but table stores use only the
    //! common BinaryWire subset; READ_ANY is only a selector for an existing read-only header and is never persisted.
    //! SingleTableStoreCorruptRecordTest#unsupportedWritableWireTypesFailBeforeCreatingAFile and
    //! #readAnyReopensExistingBinaryTableStore distinguish selection from persistence, while
    //! SingleTableBuilderCorruptMetadataTest#unsupportedPersistedWireTypeIsCorruptionAndReleasesTheMapping covers the header.
    //! SingleTableStoreCorruptRecordTest#canonicalAndLegacyBinarySelectorsRoundTripTheCommonSubset exercises the retained pair
    //! against the resolved Wire version; it is not released-file evidence.
    //! #canonicalAndLegacySelectorsCrossOpenWritableAndReadOnlyStores protects current cross-selector behavior, while
    //! #unsupportedWritableWireTypesFailBeforeCreatingAFile covers fieldless, compressed, RAW and text families.
    static WireType requireSelectedWireType(WireType wireType, boolean readOnly) {
        if (readOnly && wireType == WireType.READ_ANY)
            return wireType;
        return requirePersistedWireType(wireType);
    }

    static WireType requirePersistedWireType(WireType wireType) {
        final WireType required = Objects.requireNonNull(wireType);
        switch (required) {
            case BINARY:
            case BINARY_LIGHT:
                return required;
            default:
                throw new IllegalArgumentException(
                        "Table stores require BINARY_LIGHT or the legacy BINARY selector, not " + required);
        }
    }

    /**
     * Executes a code block with a shared lock on the specified file.
     * A failure from the target supplier or body is rethrown as the original object after lock and channel cleanup is
     * attempted; cleanup failures are attached to it as suppressed exceptions.
     *
     * @param file   The file to lock.
     * @param code   The function to execute with the locked file.
     * @param target A supplier providing the target object for the code block.
     * @param <T>    Type of the target object.
     * @param <R>    Return type of the function.
     * @return The result of the function applied to the target.
     * @throws IllegalStateException if lock acquisition times out, or lock/channel acquisition or cleanup fails without a
     *                               body failure taking precedence
     */
    public static <T, R> R doWithSharedLock(@NotNull final File file,
                                            @NotNull final Function<T, ? extends R> code,
                                            @NotNull final Supplier<T> target) {
        return doWithLock(file, code, target, true);
    }

    /**
     * Executes a code block with an exclusive lock on the specified file.
     * A failure from the target supplier or body is rethrown as the original object after lock and channel cleanup is
     * attempted; cleanup failures are attached to it as suppressed exceptions.
     *
     * @param file   The file to lock.
     * @param code   The function to execute with the locked file.
     * @param target A supplier providing the target object for the code block.
     * @param <T>    Type of the target object.
     * @param <R>    Return type of the function.
     * @return The result of the function applied to the target.
     * @throws IllegalStateException if lock acquisition times out, or lock/channel acquisition or cleanup fails without a
     *                               body failure taking precedence
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

        try {
            return doWithLock(file, code, target, shared, TimeUnit.MILLISECONDS.toNanos(timeoutMS),
                    new FileChannelLockRuntime(file, readOrWrite, shared));
        } catch (IOException e) {
            throw new IllegalStateException("I/O failure while using the " + type + " file lock on " + file, e);
        }
    }

    static <T, R> R doWithLock(@NotNull final File file,
                               @NotNull final Function<T, ? extends R> code,
                               @NotNull final Supplier<T> target,
                               final boolean shared,
                               final long timeoutNanos,
                               @NotNull final LockRuntime lockRuntime) {
        final String type = shared ? "shared" : "exclusive";

        //! A wall-clock correction could previously shorten the retry window or extend it indefinitely.
        //! A monotonic elapsed-time comparison keeps the configured lock timeout stable.
        //! SingleTableStoreLockTest#scriptedTimeoutRetainsOverlapCause and
        //! #scriptedNullContentionTimesOutWithoutInventingACause exercise the Queue-owned deadline branches; the
        //! real-lock contention tests retain integration evidence without claiming control of the operating-system clock.
        Throwable lastAcquisitionFailure = null;
        try (LockRuntime runtime = lockRuntime) {
            final long startNanos = runtime.nanoTime();
            for (int count = 1; runtime.nanoTime() - startNanos < timeoutNanos; count++) {
                boolean locked = false;
                try {
                    locked = runtime.tryLock();
                } catch (OverlappingFileLockException e) {
                    //! FileChannel.tryLock reports inter-process contention with null and same-JVM
                    //! contention with OverlappingFileLockException. Both can clear, so retry them.
                    //! SingleTableStoreLockTest#scriptedNullContentionSignalsAttemptBeforeBodyAndThenSucceeds
                    //! discriminates the null branch and its attempt-before-release scheduling boundary, while
                    //! SingleTableStoreLockTest.sharedContentionIsRetriedBeforeBodyRunsOnce and
                    //! exclusiveContentionIsRetriedBeforeBodyRunsOnce prove the same-JVM signal waits
                    //! and then runs the body exactly once with a real FileChannel. #subprocessContentionRetriesNullBeforeBody
                    //! observes the null result while a separate JVM holds the range, then releases it before body execution.
                    //! In contrast, IOException is the API's terminal "other I/O error" signal; the
                    //! scriptedAcquisitionIOExceptionRetainsExactCauseAndSkipsBody test proves Queue propagates
                    //! the exact cause without invoking either the target or body.
                    lastAcquisitionFailure = e;
                    // failed to acquire the lock, wait until other operation completes
                    if (count > 9) {
                        if (Jvm.isDebugEnabled(SingleTableStore.class)) {
                            final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(runtime.nanoTime() - startNanos);
                            final String message = "Failed to acquire " + type
                                    + " lock on the table store file. Retrying, file=" + file.getAbsolutePath()
                                    + ", count=" + count + ", elapsed=" + elapsedMs + " ms";
                            Jvm.debug().on(SingleTableStore.class, "", new StackTrace(message));
                        }
                    }
                }
                if (locked) {
                    //! Keep the body outside the acquisition handlers so even an
                    //! OverlappingFileLockException thrown by user code is not mistaken for contention.
                    //! The BodyFailed boundary makes all body Throwables bypass the surrounding IOException
                    //! handler, while explicit cleanup retains lock/channel close failures as suppressed.
                    //! SingleTableStoreLockTest.exclusiveBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity
                    //! and sharedBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity prove one execution
                    //! and the exact failure instance. #bodyFailureRetainsLockAndChannelCleanupFailures and
                    //! #successfulBodyReportsLockCloseFailureWithChannelCloseSuppressed plus
                    //! #successfulBodyReportsChannelCloseFailure exercise Queue's cleanup precedence through the
                    //! package-private seam, not a platform-generated close failure.
                    BodyFailed bodyFailed = null;
                    try {
                        return code.apply(target.get());
                    } catch (Throwable t) {
                        bodyFailed = new BodyFailed(t);
                        throw bodyFailed;
                    } finally {
                        try {
                            runtime.closeLock();
                        } catch (Throwable closeFailure) {
                            if (bodyFailed != null)
                                bodyFailed.addSuppressed(closeFailure);
                            else
                                throw Jvm.rethrow(closeFailure);
                        }
                    }
                }
                int delay = Math.min(250, count * count);
                runtime.pause(delay);
            }
            //! Preserve the last concrete same-JVM contention signal on timeout. Null is the only
            //! inter-process contention result exposed by FileChannel, so there is no cause to retain for it.
            //! Throw before the channel resource closes so a close failure is suppressed on the timeout
            //! rather than replacing it. SingleTableStoreLockTest#scriptedTimeoutRetainsOverlapCause and
            //! #scriptedNullContentionTimesOutWithoutInventingACause discriminate both outcomes without claiming
            //! that the injected null result proves an operating-system or subprocess lock implementation;
            //! #timeoutRetainsChannelCloseFailureAsSuppressed covers the cleanup edge.
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
            //! and retain the concrete cause. SingleTableStoreLockTest's scripted acquisition and cleanup
            //! tests discriminate Queue's propagation and suppression rules without claiming a platform failure.
            throw new IllegalStateException("I/O failure while using the " + type + " file lock on " + file, e);
        }
    }

    interface LockRuntime extends java.io.Closeable {
        long nanoTime();

        boolean tryLock() throws IOException;

        void closeLock() throws IOException;

        void pause(int millis);
    }

    static final class FileChannelLockRuntime implements LockRuntime {
        private final FileChannel channel;
        private final boolean shared;
        private FileLock fileLock;

        FileChannelLockRuntime(File file, StandardOpenOption readOrWrite, boolean shared) throws IOException {
            this.channel = FileChannel.open(file.toPath(), readOrWrite);
            this.shared = shared;
        }

        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public boolean tryLock() throws IOException {
            fileLock = channel.tryLock(EXCLUSIVE_LOCK_START, EXCLUSIVE_LOCK_SIZE, shared);
            return fileLock != null;
        }

        @Override
        public void closeLock() throws IOException {
            final FileLock acquired = fileLock;
            fileLock = null;
            if (acquired != null)
                acquired.close();
        }

        @Override
        public void pause(int millis) {
            Jvm.pause(millis);
        }

        @Override
        public void close() throws IOException {
            channel.close();
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
            final LongValue longValue = WireType.BINARY_LIGHT.newLongReference().get();
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
            //! Capture one immutable scan bound no greater than either this Bytes view's write limit or mapped capacity.
            //! This is a local bounds guarantee, not a durable logical EOF or a concurrent-publication barrier; the public
            //! TableStore API does not expose the internal Bytes view needed for a direct losing test.
            final long scanLimit = Math.min(mappedBytes.writeLimit(), mappedBytes.realCapacity());
            mappedBytes.readLimit(scanLimit);
            positionAfterTableStoreHeader(scanLimit);
            while (true) {
                final long bytesBeforeHeader = scanLimit - mappedBytes.readPosition();
                if (bytesBeforeHeader == 0)
                    break;
                if (bytesBeforeHeader < Integer.BYTES)
                    throw new CorruptTableStoreException(file(), "the table store ends with " + bytesBeforeHeader
                            + " trailing bytes instead of a complete record header");
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
                //! Each writer-produced record ends on the four-byte boundary where Wire may place the next header.
                //! A merely in-range end can otherwise make the next scan align past bytes that belong to no record.
                //! SingleTableStoreCorruptRecordTest#recordEndMustBeHeaderAligned rejects that silent gap.
                if ((recordEnd & (Integer.BYTES - 1L)) != 0)
                    throw new CorruptTableStoreException(file(), "the record at " + recordStart
                            + " declares an end that is not aligned for the next header");

                final ValueIn valueIn;
                final long valueStart;
                mappedBytes.readLimit(recordEnd);
                try {
                    if (mappedBytes.peekUnsignedByte() != BinaryWireCode.EVENT_NAME)
                        throw new CorruptTableStoreException(file(), "the record at " + recordStart
                                + " does not start with an event-name token");
                    valueIn = mappedWire.readEventName(sb);
                    //! Both supported table-store selectors write EVENT_NAME. BinaryWire.readEventName also accepts field-name
                    //! and field-number tokens as present identifiers, so ValueIn presence alone cannot enforce this schema.
                    //! SingleTableStoreCorruptRecordTest#missingEventNameIsRejectedByEveryTraversal covers a value-only body;
                    //! #nonCanonicalFieldNameTokenIsRejectedByEveryTraversal loses the exact-token guard; and
                    //! #explicitEmptyEventNameRemainsValid proves presence, rather than key length, is the schema boundary.
                    if (!valueIn.isPresent())
                        throw new CorruptTableStoreException(file(), "the record at " + recordStart
                                + " has no present event name");
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
                    //! Validate the value before invoking the callback so an iterator that ignores ValueIn cannot bypass
                    //! corruption checks. SingleTableStoreCorruptRecordTest#nonLongValueCodeIsRejectedByEveryTraversal
                    //! and #malformedBinaryPaddingIsRejected fail if validation is delegated to callback consumption.
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
        //! The first document is the table-store schema header; a data document there cannot establish the persisted wire
        //! type or metadata. SingleTableStoreCorruptRecordTest#firstRecordMustBeMetadata discriminates this check.
        if (scanLimit < Integer.BYTES)
            throw new CorruptTableStoreException(file(), "the table store has no complete metadata header");
        final WireIn.HeaderType headerType = mappedWire.readDataHeader(true);
        if (headerType != WireIn.HeaderType.META_DATA)
            throw new CorruptTableStoreException(file(), "the first record is not table-store metadata");

        final int header = mappedBytes.readVolatileInt();
        final long bodyStart = mappedBytes.readPosition();
        final int length = Wires.lengthOf(header);
        //! A ready zero-length metadata document cannot contain the persisted table-store schema. Advancing by zero would
        //! make the data scanner reinterpret bytes outside that document as if the header had established a wire type.
        //! SingleTableStoreCorruptRecordTest#firstMetadataLengthMustBePositive discriminates this boundary.
        if (length <= 0)
            throw new CorruptTableStoreException(file(), "the table-store metadata header has no body");
        if (length > scanLimit - bodyStart)
            throw new CorruptTableStoreException(file(), "the table-store header declares a length of " + length
                    + ", which does not fit inside the read limit of " + scanLimit);
        final long headerEnd = bodyStart + length;
        //! A binary-family data-header reader starts at this declared end, so its metadata document must leave it on the
        //! four-byte boundary used by the writer for every following header.
        //! SingleTableStoreCorruptRecordTest#firstMetadataEndMustBeHeaderAligned rejects an otherwise silent alignment gap.
        if ((headerEnd & (Integer.BYTES - 1L)) != 0)
            throw new CorruptTableStoreException(file(),
                    "the table-store metadata header end is not aligned for the first data header");
        mappedBytes.readPosition(headerEnd);
    }

    //! SingleTableStoreCorruptRecordTest#longerInRangeRecordIsRejected discriminates exact body consumption, while
    //! #nonLongValueCodeIsRejectedByEveryTraversal and #malformedBinaryPaddingIsRejected cover value schema and padding.
    //! Bounding the Wire before parsing prevents #shorterPositiveRecordIsRejected from borrowing successor bytes.
    //! Only Wire's explicit padding codes are accepted between the value and the declared end.
    private void validateRecordConsumption(long recordStart, long recordEnd) {
        consumeExplicitBinaryPadding(recordStart);
        if (mappedBytes.readRemaining() == 0)
            throw new CorruptTableStoreException(file(), "the record at " + recordStart + " holds no value");

        //! TableStore records contain bound longs, not arbitrary same-width Wire values. The
        //! SingleTableStoreCorruptRecordTest#nonLongValueCodeIsRejectedByEveryTraversal test replaces INT64 with
        //! FLOAT64 without changing the record length and fails if generic skipValue validation is restored.
        final long valueCodeAt = mappedBytes.readPosition();
        final int valueCode = mappedBytes.peekUnsignedByte();
        if (valueCode != 0 && valueCode != BinaryWireCode.INT64)
            throw new CorruptTableStoreException(file(), "the record at " + recordStart
                    + " holds Wire type 0x" + Integer.toHexString(valueCode) + " instead of a bound long");

        //! BinaryWire aligns the eight-byte bound-long payload, not its preceding type code. Bounded but non-canonical
        //! padding can otherwise bind a live LongValue to an unaligned mapped address.
        //! SingleTableStoreCorruptRecordTest#misalignedBoundLongIsRejected discriminates this addressability invariant.
        final long payloadAt = valueCodeAt + 1L;
        if ((payloadAt & (Long.BYTES - 1L)) != 0)
            throw new CorruptTableStoreException(file(), "the record at " + recordStart
                    + " holds a bound long at an unaligned address");
        if (mappedBytes.readRemaining() < 1L + Long.BYTES)
            throw new CorruptTableStoreException(file(), "the record at " + recordStart
                    + " ends inside its bound long");
        // BinaryWire's binding reader accepts zero as a legacy long marker and consumes its following eight bytes.
        mappedBytes.readSkip(1L + Long.BYTES);
        consumeExplicitBinaryPadding(recordStart);

        final long actualEnd = mappedBytes.readPosition();
        if (actualEnd != recordEnd)
            throw new CorruptTableStoreException(file(), "the record at " + recordStart + " declares an end at "
                    + recordEnd + " but its content ends at " + actualEnd);
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
