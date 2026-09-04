/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.util.ClassNotFoundRuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.scoped.ScopedResource;
import net.openhft.chronicle.core.util.Builder;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataKeys;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.BufferUnderflowException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

public class SingleTableBuilder<T extends Metadata> implements Builder<TableStore<T>> {

    static {
        CLASS_ALIASES.addAlias(WireType.class);
        CLASS_ALIASES.addAlias(SingleTableStore.class, "STStore");
    }

    @NotNull
    private final File file;
    @NotNull
    private final T metadata;

    private WireType wireType;
    private boolean readOnly;

    private SingleTableBuilder(@NotNull File path, @NotNull T metadata) {
        this.file = path;
        this.metadata = metadata;
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> builder(@NotNull File file, @NotNull WireType wireType, @NotNull T metadata) {
        if (file.isDirectory()) {
            throw new IllegalArgumentException("Tables should be configured with the table file, not a directory. Actual file used: " + file.getParentFile());
        }
        if (!file.getName().endsWith(SingleTableStore.SUFFIX)) {
            throw new IllegalArgumentException("Invalid file type: " + file.getName());
        }

        return new SingleTableBuilder<>(file, metadata).wireType(wireType);
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> binary(@NotNull Path path, @NotNull T metadata) {
        return binary(path.toFile(), metadata);
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> binary(@NotNull String file, @NotNull T metadata) {
        return binary(new File(file), metadata);
    }

    @NotNull
    public static <T extends Metadata> SingleTableBuilder<T> binary(@NotNull File basePathFile, @NotNull T metadata) {
        return builder(basePathFile, WireType.BINARY_LIGHT, metadata);
    }

    @NotNull
    public TableStore<T> build() {
        //! Table stores require a bounded, addressable long representation. Text-family wires can write a binding outside
        //! the declared body, while auto-detect cannot define a write format. RAW retains its bounded legacy initial-open
        //! path; its pre-existing typed-header reopen limitation is separate. SingleTableStoreCorruptRecordTest
        //! #unsupportedWireTypesAreRejectedBeforeCreatingAFile proves fail-fast has no filesystem side effect.
        SingleTableStore.requireSupportedWireType(wireType);
        if (readOnly) {
            if (!file.exists())
                throw new IORuntimeException("Metadata file not found in readOnly mode");

            // Wait a short time for the file to be initialized
            TimingPauser pauser = Pauser.balanced();
            try {
                while (file.length() < OS.mapAlignment()) {
                    pauser.pause(1, TimeUnit.SECONDS);
                }
            } catch (TimeoutException e) {
                throw new IORuntimeException("Metadata file found in readOnly mode, but not initialized yet");
            }
        }

        MappedBytes bytes = null;
        //! The builder owns this mapping until a complete TableStore is returned; every earlier failure must close it.
        //! SingleTableBuilderCorruptMetadataTest#failedHeaderDecodePreservesTheFileAndReleasesTheMapping fails if this
        //! ownership guard is removed, because the failed mapping keeps the table-store file open on affected platforms.
        boolean handedOver = false;
        try {
            if (!readOnly && file.createNewFile() && !file.canWrite()) {
                throw new IllegalStateException("Cannot write to tablestore file " + file);
            }
            // TODO Change this to a single chunk file in x.28
            bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, readOnly);
            // these MappedBytes are shared, but the assumption is they shouldn't grow. Supports 2K entries.
            bytes.singleThreadedCheckDisabled(true);

            // eagerly initialize backing MappedFile page - otherwise wire.writeFirstHeader() will try to lock the file
            // to allocate the first byte store and that will cause lock overlap
            bytes.readVolatileInt(0);
            Wire wire = wireType.apply(bytes);
            final TableStore<T> store;
            if (readOnly) {
                store = SingleTableStore.doWithSharedLock(file, v -> {
                    try {
                        return readTableStore(wire);
                    } catch (IOException ex) {
                        throw Jvm.rethrow(ex);
                    }
                }, () -> null);
            } else {
                MappedBytes finalBytes = bytes;
                store = SingleTableStore.doWithExclusiveLock(file, v -> {
                    try {
                        if (wire.writeFirstHeader()) {
                            return writeTableStore(finalBytes, wire);
                        } else {
                            return readTableStore(wire);
                        }
                    } catch (IOException ex) {
                        throw Jvm.rethrow(ex);
                    }
                }, () -> null);
            }
            //! Transfer mapping ownership only after the locked body returns a complete store; doing so earlier leaks failures
            //! from first-header decoding or metadata override. The
            //! SingleTableBuilderCorruptMetadataTest#metadataOverrideFailureRetainsIdentityAndReleasesTheMapping test
            //! discriminates this ordering.
            handedOver = true;
            return store;
        } catch (IOException e) {
            throw new IORuntimeException("file=" + file.getAbsolutePath(), e);
        } finally {
            if (bytes != null) {
                // A provisional store closes the same mapping when construction fails after deserialisation.
                if (!bytes.isClosed()) {
                    bytes.singleThreadedCheckReset();
                    // no table store took ownership of the mapping, so nothing else will release it
                    if (!handedOver)
                        bytes.close();
                }
            }
        }
    }

    @NotNull
    private TableStore<T> readTableStore(Wire wire) throws StreamCorruptedException {
        final TableStore<T> existing = decodeTableStore(wire);
        boolean handedOver = false;
        try {
            //! Metadata override is caller policy, not evidence that bytes on disk are corrupt; it must retain its original
            //! failure type. SingleTableBuilderCorruptMetadataTest#metadataOverrideFailureRetainsIdentityAndReleasesTheMapping
            //! fails if override is moved back inside first-header corruption translation.
            metadata.overrideFrom(existing.metadata());
            handedOver = true;
            return existing;
        } finally {
            //! Deserialisation can create a TableStore before a later policy failure, so close that provisional owner unless
            //! returned. SingleTableBuilderCorruptMetadataTest#metadataOverrideFailureRetainsIdentityAndReleasesTheMapping
            //! fails if this cleanup is removed.
            if (!handedOver)
                Closeable.closeQuietly(existing);
        }
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private TableStore<T> decodeTableStore(Wire wire) throws StreamCorruptedException {
        Object decoded = null;
        TableStore<T> existing = null;
        boolean handedOver = false;
        try {
            //! Confine corruption translation to failures that identify the bounded on-disk header or its required type.
            //! Persisted constructors are application hooks, so an arbitrary RuntimeException from one must retain Wire's
            //! original failure chain rather than being relabelled as disk corruption.
            //! SingleTableBuilderCorruptMetadataTest#unknownTypeAliasIsReportedAsCorruption and
            //! #nestedMetadataAliasAndConstructorFailuresAreNotMisreportedAsCorruption discriminate the outer/nested
            //! classification boundary;
            //! SingleChronicleQueueCorruptMetadataTest#aCorruptFirstHeaderFailsWithoutWaitingForTheTableStoreLock
            //! fails if the terminal decode failure is allowed to re-enter lock acquisition.
            wire.readFirstHeader();
            // readFirstHeader limits the read to the length that the first header declares
            final long declaredEnd = wire.bytes().readLimit();

            final ValueIn valueIn = readTableStoreValue(wire);
            decoded = valueIn.typedMarshallable();
            //! Decode without an inferred TableStore cast, then validate the result explicitly; otherwise a valid typed value
            //! of the wrong class escapes as ClassCastException and can activate the queue builder's read-only fallback.
            //! SingleTableBuilderCorruptMetadataTest#nullAndWrongTypedHeaderValuesAreReportedAsCorruption covers both forms.
            if (!(decoded instanceof TableStore)) {
                final String actualType = decoded == null ? "null" : decoded.getClass().getName();
                throw new CorruptTableStoreException(file,
                        "the first header holds " + actualType + " instead of a table store");
            }
            existing = (TableStore<T>) decoded;

            // Every later scan of this file uses the declared length. If the length disagrees with
            // the content that it describes, the whole file is misaligned.
            //! Later record scans begin at the declared end, so a decoded header must consume that boundary exactly; accepting
            //! trailing or over-consumed bytes misaligns every record.
            //! SingleChronicleQueueCorruptMetadataTest#aCorruptFirstHeaderThrowsCorruptTableStoreException discriminates the
            //! declared-versus-consumed check; #anIntactMetadataFileStillBuildsAndReadsBack guards the current healthy encoding.
            final long actualEnd = wire.bytes().readPosition();
            if (actualEnd != declaredEnd)
                throw new CorruptTableStoreException(file, "the first header declares " + declaredEnd
                        + " bytes but its content ends at " + actualEnd);

            handedOver = true;
            return existing;
        } catch (CorruptTableStoreException e) {
            throw e;
        } catch (StreamCorruptedException | BufferUnderflowException | ClassNotFoundRuntimeException |
                 OverlappingFileLockException e) {
            //! Event-name, type-alias and bounded-underflow failures arise before any caller policy hook and identify an
            //! unreadable first header. A malformed nested length can also force mapped-chunk acquisition while this file's
            //! table-store lock is held and surface OverlappingFileLockException at this exact boundary.
            //! SingleTableBuilderCorruptMetadataTest#malformedFirstEventNameIsReportedAsCorruption
            //! and SingleChronicleQueueCorruptMetadataTest#aCorruptHeaderBodyIsReportedAsCorruptionAndReleasesTheMapping
            //! distinguish these parser failures from metadata-constructor and override failures; the nested-constructor
            //! test proves an application-thrown overlap signal remains outside this direct decode classification.
            throw new CorruptTableStoreException(file, "the first header does not hold a readable table store", e);
        } catch (RuntimeException e) {
            final CorruptTableStoreException corruption = findCorruption(e);
            if (corruption != null)
                throw corruption;
            throw e;
        } finally {
            //! A typed value can construct a closeable object before type or boundary validation fails; the outer builder
            //! cannot close an object it never receives. SingleTableBuilderCorruptMetadataTest
            //! #wrongCloseableHeaderValueIsClosedOnRejection and
            //! #partialStoreConstructionIsCorruptionAndReleasesTheMapping exercise the wrong-type and provisional-store
            //! cleanup edges.
            if (!handedOver)
                Closeable.closeQuietly(existing != null ? existing : decoded);
        }
    }

    private CorruptTableStoreException findCorruption(Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause(); cause = cause.getCause()) {
            if (cause instanceof CorruptTableStoreException)
                return (CorruptTableStoreException) cause;
        }
        return null;
    }

    private ValueIn readTableStoreValue(@NotNull Wire wire) throws StreamCorruptedException {
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            StringBuilder name = stlSb.get();
            ValueIn valueIn = wire.readEventName(name);
            if (!StringUtils.isEqual(name, MetaDataKeys.header.name())) {
                throw new StreamCorruptedException("The first message should be the header, was " + name);
            }
            return valueIn;
        }
    }

    @NotNull
    private TableStore<T> writeTableStore(MappedBytes bytes, Wire wire) {
        final TableStore<T> store = new SingleTableStore<>(wireType, bytes, metadata);
        boolean handedOver = false;
        try {
            wire.writeEventName("header").object(store);
            wire.updateFirstHeader();
            handedOver = true;
            return store;
        } finally {
            //! Construction registers the new store before caller metadata is serialised and the first header is published.
            //! SingleTableBuilderCorruptMetadataTest#failedInitialMetadataWriteClosesTheProvisionalStore fails through
            //! resource tracing if metadata serialisation leaves that unreachable store open. Header-publication failure
            //! follows the same finally edge but has no injectable public seam.
            if (!handedOver)
                Closeable.closeQuietly(store);
        }
    }

    @NotNull
    public File file() {
        return file;
    }

    public WireType wireType() {
        return wireType;
    }

    public SingleTableBuilder<T> wireType(WireType wireType) {
        this.wireType = wireType;
        return this;
    }

    public boolean readOnly() {
        return readOnly;
    }

    public SingleTableBuilder<T> readOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }
}
