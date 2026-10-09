/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.scoped.ScopedResource;
import net.openhft.chronicle.core.util.Builder;
import net.openhft.chronicle.core.util.ClassNotFoundRuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataKeys;
import net.openhft.chronicle.queue.impl.single.SCQMeta;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.BinaryWireCode;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

/**
 * Builds the Queue table store persisted in a {@code .cq4t} file.
 *
 * <p>{@link WireType#BINARY_LIGHT} is the canonical format. {@link WireType#BINARY} remains an accepted
 * legacy selector, but table stores use only the subset shared with {@code BINARY_LIGHT}.
 * {@link WireType#READ_ANY} may select an existing read-only store; it is never a persisted format.
 * Other Wire types are not supported for table stores, even when they are supported for queue data.</p>
 *
 * <p>An existing first header must contain Queue's {@link SingleTableStore} persisted schema.
 * Implementations of the broader {@link TableStore} interface are not accepted from disk.</p>
 *
 * @param <T> metadata type stored in the table header
 */
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

    /**
     * Creates a builder with an explicit table-store format selector.
     *
     * @param file     table-store file
     * @param wireType {@code BINARY_LIGHT}, the legacy {@code BINARY} selector restricted to the same common subset,
     *                 or {@code READ_ANY} for read-only opening
     * @param metadata metadata used for a new store and overridden from an existing store
     * @param <T>      metadata type
     * @return a new builder
     * @throws IllegalArgumentException if {@code file} is not a {@code .cq4t} path
     */
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

    /**
     * Opens or creates the configured table store.
     *
     * @return the opened store
     * @throws IllegalArgumentException if the selected Wire type is unsupported or {@code READ_ANY} is writable
     * @throws CorruptTableStoreException if an existing first header violates the table-store schema
     * @throws TableStoreUnavailableException if the metadata file is unavailable at the file-opening boundary, or a read-only
     *                                        file is absent or uninitialized
     */
    @NotNull
    public TableStore<T> build() {
        //! Separate read detection from persistent write formats. READ_ANY may inspect an existing read-only binary store but
        //! cannot select the encoding for a new or writable store. BINARY_LIGHT is canonical; BINARY is a legacy-compatible
        //! selector, but table stores use no BINARY-only features. Fieldless, compressed, RAW and text are unsupported here.
        //! SingleTableStoreCorruptRecordTest#readAnyReopensExistingBinaryTableStore and
        //! #unsupportedWritableWireTypesFailBeforeCreatingAFile discriminate the two capabilities.
        SingleTableStore.requireSelectedWireType(wireType, readOnly);
        if (readOnly)
            awaitReadOnlyFirstHeader();

        MappedBytes bytes = null;
        //! The builder owns this mapping until a complete TableStore is returned; every earlier failure must close it.
        //! SingleTableBuilderCorruptMetadataTest#failedHeaderDecodePreservesTheFileAndReleasesTheMapping fails if this
        //! ownership guard is removed, because the failed mapping keeps the table-store file open on affected platforms.
        boolean handedOver = false;
        final AtomicReference<TableStore<T>> provisionalStore = new AtomicReference<>();
        try {
            final int mappingPageSize = PageUtil.getPageSize(file.getAbsolutePath());
            final long mappedChunkSize = OS.mapAlign(OS.SAFE_PAGE_SIZE, mappingPageSize);
            final long firstWritableMappingSize = 2L * mappedChunkSize;
            if (!readOnly) {
                final boolean created;
                try {
                    created = file.createNewFile();
                } catch (IOException e) {
                    //! File creation is part of the availability boundary. A writable queue request against an unwritable
                    //! directory historically falls back to a synthetic read-only table store on non-Windows platforms.
                    //! ReadWriteTest#testNonWriteableDirectoryWithoutMetadataSetsQueueReadOnly loses this translation.
                    throw new TableStoreUnavailableException(file, "Metadata file cannot be created", e);
                }
                if (created && !file.canWrite())
                    throw new TableStoreUnavailableException(file, "Metadata file cannot be opened for writing");

                //! A writable mapping grows a short file while acquiring its first chunk. Reject a ready first header whose
                //! declared body is physically truncated before mapping, while retaining compact files whose header is whole.
                //! SingleTableBuilderCorruptMetadataTest#failedWritableOpenDoesNotGrowTruncatedMetadata discriminates
                //! this pre-mapping boundary; #compactCompleteTableStoreReopensReadOnly records the positive compact case.
                if (!created && file.length() > 0L && file.length() < firstWritableMappingSize)
                    validateFirstHeaderBeforeWritableMapping();
            }
            // TODO Change this to a single chunk file in x.28
            final long readBound;
            try {
                if (readOnly) {
                    final long physicalLength = file.length();
                    //! Inspect a ready first header before creating a rounded native read-only mapping. This makes a declared
                    //! end beyond EOF a Java-level corruption result on every platform rather than relying on an OS mapping.
                    //! SingleTableBuilderCorruptMetadataTest#readOnlyOpenRejectsHeaderBeyondPhysicalFileWithoutMutation
                    //! exercises the pre-mapping boundary and verifies that the original file is unchanged.
                    validateFirstHeaderBeforeMapping(physicalLength);
                    //! Released table stores can be page-aligned and only 64 KiB. Use exact, zero-overlap chunks for compact
                    //! files so opening never maps beyond EOF but later file growth remains visible; a single mapping freezes
                    //! the old capacity and misreports a healthy boundary-crossing record as corruption.
                    //! SingleTableBuilderCorruptMetadataTest#compactReadOnlyTableStoreObservesLaterGrowth discriminates the
                    //! growable mapping; #compactCompleteTableStoreReopensReadOnly retains the static positive control.
                    final boolean compact = physicalLength < firstWritableMappingSize
                            && physicalLength % mappingPageSize == 0L;
                    bytes = compact
                            ? MappedBytes.mappedBytes(file, physicalLength, 0L, mappingPageSize, true)
                            : MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, mappingPageSize, true);
                    bytes.readLimit(physicalLength);
                    readBound = physicalLength;
                } else {
                    bytes = MappedBytes.mappedBytes(
                            file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, mappingPageSize, false);
                    // A concurrent creator can publish the first header while this builder waits for the lock.
                    // The current physical bound is therefore captured inside the exclusive body below.
                    readBound = -1L;
                }
            } catch (FileNotFoundException e) {
                //! Translate only the file-opening boundary to availability. Decoder constructors may themselves nest a
                //! FileNotFoundException, but that does not prove the metadata file was absent or inaccessible.
                //! SingleChronicleQueueCorruptMetadataTest#nestedFileNotFoundFromMetadataDoesNotActivateReadonlyFallback
                //! fails if cause provenance is inferred outside this scope.
                throw new TableStoreUnavailableException(file, "Metadata file is unavailable", e);
            }
            // these MappedBytes are shared, but the assumption is they shouldn't grow. Supports 2K entries.
            bytes.singleThreadedCheckDisabled(true);

            // Writable mappings must initialize their first page before taking the table-store lock; otherwise
            // wire.writeFirstHeader() can take the mapped-file growth lock underneath it. Read-only mappings never grow.
            if (!readOnly)
                bytes.readVolatileInt(0);
            //! BINARY is retained as a selector/header label only. Use the BINARY_LIGHT codec for both labels so a future
            //! legacy codec change cannot introduce BINARY-only features into table-store content.
            //! SingleTableStoreCorruptRecordTest#canonicalAndLegacySelectorsCrossOpenWritableAndReadOnlyStores covers both.
            Wire wire = (wireType == WireType.READ_ANY ? WireType.READ_ANY : WireType.BINARY_LIGHT).apply(bytes);
            final TableStore<T> store;
            if (readOnly) {
                final MappedBytes readOnlyBytes = bytes;
                store = SingleTableStore.doWithSharedLock(file, v -> {
                    try {
                        //! Re-clamp the snapshot after taking the cooperative table-store lock, before the first mapped read.
                        //! A process that ignores this lock can still truncate an active mapping; Queue does not claim to make
                        //! arbitrary external file mutation safe. The physical-bound and compact-positive tests cover the
                        //! deterministic boundaries under Queue's locking protocol.
                        final long lockedReadBound = Math.min(
                                readBound, readOnlyBytes.mappedFile().actualSize());
                        //! Clamp the captured Bytes directly. Calling ReadAnyWire.bytes() first can trigger format detection
                        //! against the old limit and touch stale mapped bytes before this bound takes effect.
                        readOnlyBytes.readPosition(0L);
                        readOnlyBytes.readLimit(lockedReadBound);
                        final TableStore<T> opened = readTableStore(readOnlyBytes, wire, lockedReadBound);
                        provisionalStore.set(opened);
                        return opened;
                    } catch (IOException ex) {
                        throw Jvm.rethrow(ex);
                    }
                }, () -> null);
            } else {
                MappedBytes finalBytes = bytes;
                store = SingleTableStore.doWithExclusiveLock(file, v -> {
                    try {
                        if (wire.writeFirstHeader()) {
                            final TableStore<T> opened = writeTableStore(finalBytes, wire);
                            provisionalStore.set(opened);
                            return opened;
                        } else {
                            //! A builder can observe the file before a concurrent creator publishes its first header, then
                            //! acquire the lock afterwards. Read the mapped channel's size under that lock rather than reusing
                            //! the pre-lock observation. RollCycleMultiThreadStressTest#stress exercises that publication race.
                            final long lockedReadBound = finalBytes.mappedFile().actualSize();
                            final TableStore<T> opened = readTableStore(finalBytes, wire, lockedReadBound);
                            provisionalStore.set(opened);
                            return opened;
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
            //! A body can finish constructing a store before lock or channel cleanup fails. Keep that provisional owner
            //! reachable until the lock helper returns, and close it if no caller receives it. The scripted cleanup tests in
            //! SingleTableStoreLockTest cover propagation; no public FileChannel seam deterministically injects this exact
            //! post-construction builder edge, so the ownership guard currently has static rather than losing-test evidence.
            if (!handedOver)
                Closeable.closeQuietly(provisionalStore.get());
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
    private TableStore<T> readTableStore(MappedBytes mappedBytes,
                                         Wire wire,
                                         long readBound) throws StreamCorruptedException {
        final TableStore<T> existing = decodeTableStore(mappedBytes, wire, readBound);
        boolean handedOver = false;
        try {
            //! Metadata override is caller policy, not evidence that bytes on disk are corrupt; ordinary failures retain their
            //! original type. SingleTableBuilderCorruptMetadataTest#metadataOverrideFailureRetainsIdentityAndReleasesTheMapping
            //! fails if override is moved back inside first-header corruption translation.
            metadata.overrideFrom(existing.metadata());
            handedOver = true;
            return existing;
        } catch (TableStoreUnavailableException e) {
            //! Only file opening may authorize synthetic fallback. An override can rethrow an availability signal obtained
            //! from another operation, so preserve that reserved type as a cause rather than as this build's opening outcome.
            //! SingleTableBuilderCorruptMetadataTest#metadataOverrideCannotOriginateAnAvailabilitySignal loses this boundary.
            throw new IORuntimeException("Persisted table-store metadata override failed", e);
        } finally {
            //! Deserialisation can create a TableStore before a later policy failure, so close that provisional owner unless
            //! returned. SingleTableBuilderCorruptMetadataTest#metadataOverrideFailureRetainsIdentityAndReleasesTheMapping
            //! fails if this cleanup is removed.
            if (!handedOver)
                Closeable.closeQuietly(existing);
        }
    }

    @NotNull
    private TableStore<T> decodeTableStore(MappedBytes mappedBytes,
                                           Wire wire,
                                           long readBound) throws StreamCorruptedException {
        TableStore<T> existing = null;
        boolean handedOver = false;
        try {
            //! Confine corruption translation to failures that identify the bounded on-disk header or its required type.
            //! Persisted constructors are application hooks, so their RuntimeException chain or Error cause must not be
            //! relabelled as disk corruption. #metadataConstructorErrorRetainsCauseAndReleasesTheMapping covers Error;
            //! SingleTableBuilderCorruptMetadataTest#unknownTypeAliasIsReportedAsCorruption and
            //! #nestedMetadataAliasAndConstructorFailuresAreNotMisreportedAsCorruption discriminate the outer/nested
            //! classification boundary;
            //! SingleChronicleQueueCorruptMetadataTest#aCorruptFirstHeaderFailsWithoutWaitingForTheTableStoreLock
            //! fails if the terminal decode failure is allowed to re-enter lock acquisition.
            //! READ_ANY must not inspect the SPB header as if it were the first Wire token. Validate through the captured
            //! Bytes, then let readFirstHeader move to the body before READ_ANY performs format detection.
            //! SingleTableStoreCorruptRecordTest#readAnyIgnoresFieldCodeBytesInTheFirstHeader loses this ordering.
            validateFirstHeaderPhysicalBound(mappedBytes, readBound);
            wire.readFirstHeader();
            // readFirstHeader limits the read to the length that the first header declares
            final long declaredEnd = wire.bytes().readLimit();

            final long previousWriteLimit = mappedBytes.writeLimit();
            try {
                //! BinaryWire's nested-marshallable reader may otherwise raise readLimit as far as writeLimit. Hard-cap the
                //! containing first header so a forged STStore length cannot borrow preallocated or later-file bytes, grow a
                //! writable mapping, or invoke a constructor outside the declared document.
                //! SingleTableBuilderCorruptMetadataTest#outerTableStoreFramingFailureIsCorruptionWithoutConstruction and
                //! #oversizedOuterTableStoreLengthCannotEscapeTheFirstHeader discriminate the bounded document behavior.
                mappedBytes.writeLimit(declaredEnd);

                final ValueIn valueIn = readTableStoreValue(wire);
                preflightTableStoreType(valueIn);
                //! The trusted type prefix must be followed by BinaryWire's explicit marshallable framing. Permissive scalar,
                //! null, padding-only or absent framing can otherwise invoke the STStore constructor on malformed input.
                //! SingleTableBuilderCorruptMetadataTest#outerSchemaRequiresMarshallableFraming covers each rejected form.
                requireMarshallableFrame(valueIn, "the first header does not hold a readable table store");
                //! Only the trusted mapping-owning SingleTableStore schema may receive this builder mapping. Accepting any
                //! TableStore can return an object that neither retains nor closes these MappedBytes. Consume the allowlisted
                //! prefix and construct SingleTableStore directly so a mutable global alias cannot substitute a foreign class.
                //! SingleTableBuilderCorruptMetadataTest#foreignTableStoreIsRejectedAndReleasesTheMapping discriminates
                //! the persisted-schema and ownership boundaries; #remappedTableStoreAliasCannotConstructForeignImplementation
                //! covers the global-alias losing case.
                existing = valueIn.applyToMarshallable(nestedWire -> {
                    final long containingWriteLimit = mappedBytes.writeLimit();
                    try {
                        //! Cap nested metadata parsing to the STStore marshallable body. SingleTableStore applies a still
                        //! narrower cap before invoking an extensible metadata constructor.
                        mappedBytes.writeLimit(mappedBytes.readLimit());
                        try {
                            final SingleTableStore<T> decoded = new SingleTableStore<T>(nestedWire);
                            //! Wire normally skips unread marshallable fields. The Queue-owned STStore schema instead requires
                            //! its complete body to be consumed, so unknown trailing fields cannot hide inside a valid outer end.
                            //! SingleTableBuilderCorruptMetadataTest#trailingTableStoreFieldAfterMetadataIsCorruption
                            //! fails if the enclosing Wire reader is allowed to skip those fields.
                            if (mappedBytes.readPosition() != mappedBytes.readLimit()) {
                                Closeable.closeQuietly(decoded);
                                throw new SingleTableStore.SchemaCorruptionException(
                                        "the table-store body length does not match its decoded content");
                            }
                            return decoded;
                        } catch (SingleTableStore.SchemaCorruptionException e) {
                            throw e;
                        } catch (SingleTableStore.MetadataConstructorFailed e) {
                            throw new TableStoreConstructorFailed(e.getCause(), e.metadataType());
                        } catch (RuntimeException e) {
                            throw new SingleTableStore.SchemaCorruptionException(
                                    "the first header does not hold a readable table store", e);
                        }
                    } finally {
                        mappedBytes.writeLimit(containingWriteLimit);
                    }
                });

                // Every later scan of this file uses the declared length. If the length disagrees with
                // the content that it describes, the whole file is misaligned.
                //! Later record scans begin at the declared end, so a decoded header must consume that boundary exactly;
                //! accepting trailing or over-consumed bytes misaligns every record.
                //! SingleChronicleQueueCorruptMetadataTest#aCorruptFirstHeaderThrowsCorruptTableStoreException discriminates
                //! the declared-versus-consumed check; #anIntactMetadataFileStillBuildsAndReadsBack guards the healthy encoding.
                final long actualEnd = mappedBytes.readPosition();
                if (actualEnd != declaredEnd)
                    throw new SingleTableStore.SchemaCorruptionException(
                            "the first header length does not match its decoded content");

                handedOver = true;
                return existing;
            } finally {
                mappedBytes.writeLimit(previousWriteLimit);
            }
        } catch (SingleTableStore.SchemaCorruptionException e) {
            throw new CorruptTableStoreException(file, e.getMessage(), e);
        } catch (TableStoreConstructorFailed e) {
            final Throwable constructorFailure = e.getCause();
            if (e.metadataType() == SCQMeta.class) {
                //! SCQMeta and its nested SCQRoll are Queue-owned persisted schema, not application constructor hooks. A
                //! decode failure inside that body is corruption just like a damaged STStore envelope, with redacted detail.
                //! SingleChronicleQueueCorruptMetadataTest#queueOwnedMetadataBodyDamageIsCorruption discriminates the scope.
                throw new CorruptTableStoreException(file, "the queue metadata body cannot be decoded",
                        redactedDecodeCause(constructorFailure));
            }
            if (constructorFailure instanceof IORuntimeException
                    && !(constructorFailure instanceof CorruptTableStoreException)
                    && !(constructorFailure instanceof TableStoreUnavailableException))
                throw (IORuntimeException) constructorFailure;
            //! A metadata constructor can throw public Queue exception types, but that does not give the failure file-open
            //! provenance. Preserve it as application-constructor context so it cannot activate read-only fallback.
            //! SingleChronicleQueueCorruptMetadataTest#constructorAvailabilitySignalDoesNotActivateReadonlyFallback
            //! discriminates this boundary.
            throw new IORuntimeException("Persisted table-store constructor failed", constructorFailure);
        } catch (StreamCorruptedException | BufferUnderflowException | BufferOverflowException | ClassNotFoundRuntimeException |
                 OverlappingFileLockException e) {
            //! Event-name, type-alias and bounded-underflow failures arise before any caller policy hook and identify an
            //! unreadable first header. A malformed nested length can also force mapped-chunk acquisition while this file's
            //! table-store lock is held and surface OverlappingFileLockException at this exact boundary.
            //! SingleTableBuilderCorruptMetadataTest#malformedFirstEventNameIsReportedAsCorruption
            //! and SingleChronicleQueueCorruptMetadataTest#aCorruptHeaderBodyIsReportedAsCorruptionAndReleasesTheMapping
            //! distinguish these parser failures from metadata-constructor and override failures; the nested-constructor
            //! test proves an application-thrown overlap signal remains outside this direct decode classification.
            throw new CorruptTableStoreException(file, "the first header does not hold a readable table store",
                    redactedDecodeCause(e));
        } catch (RuntimeException e) {
            //! All extensible-constructor failures have crossed the origin marker above. Any remaining RuntimeException in
            //! this scope came from Queue/Wire decoding or boundary enforcement and is a redacted corruption diagnosis.
            //! SingleTableBuilderCorruptMetadataTest#outerTableStoreFramingFailureIsCorruptionWithoutConstruction
            //! and #metadataFieldFramingFailureIsCorruptionWithoutConstruction cover the two framing levels.
            throw new CorruptTableStoreException(file, "the first header does not hold a readable table store",
                    redactedDecodeCause(e));
        } finally {
            //! The alias preflight prevents wrong outer types from being constructed. A trusted STStore can still register
            //! itself before a nested field or boundary fails, so close every provisional object that reached this scope.
            //! SingleTableBuilderCorruptMetadataTest#wrongCloseableHeaderValueIsNotConstructed and
            //! #partialStoreConstructionIsCorruptionAndReleasesTheMapping cover both ownership edges.
            if (!handedOver)
                Closeable.closeQuietly(existing);
        }
    }

    private IORuntimeException redactedDecodeCause(Throwable failure) {
        //! Persisted event names, aliases and byte dumps must not enter the throwable chain that normal logging renders.
        //! Preserve the decoder's failure class as bounded diagnostic context without retaining its data-bearing message.
        //! SingleTableBuilderCorruptMetadataTest#corruptionCauseChainDoesNotExposeStoredContent covers event and alias input.
        return new IORuntimeException("First-header decoding failed with " + failure.getClass().getName());
    }

    private void awaitReadOnlyFirstHeader() {
        if (!file.exists())
            throw new TableStoreUnavailableException(file, "Metadata file not found in readOnly mode");

        //! A creator grows the file before taking the table-store lock and publishing its first header. Zero and
        //! NOT_COMPLETE therefore mean "not published yet", not corruption; wait before acquiring the shared lock.
        //! SingleTableBuilderCorruptMetadataTest#readOnlyOpenWaitsForFirstHeaderPublication loses this distinction.
        final TimingPauser shortFilePauser = Pauser.balanced();
        final TimingPauser publicationPauser = Pauser.balanced();
        try {
            while (true) {
                boolean preallocated = false;
                try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                    final long physicalLength = raf.length();
                    preallocated = physicalLength >= OS.mapAlignment();
                    if (physicalLength >= Integer.BYTES) {
                        final int header = Integer.reverseBytes(raf.readInt());
                        if (Wires.isReady(header)) {
                            if (Wires.lengthOf(header) > physicalLength - Integer.BYTES)
                                throw new CorruptTableStoreException(file,
                                        "the first header extends beyond the physical file");
                            return;
                        }
                    }
                } catch (FileNotFoundException e) {
                    throw new TableStoreUnavailableException(file, "Metadata file is unavailable", e);
                } catch (IOException e) {
                    throw new TableStoreUnavailableException(file, "Metadata file cannot be inspected", e);
                }
                if (preallocated)
                    publicationPauser.pause(SingleTableStore.lockTimeoutMillis(), TimeUnit.MILLISECONDS);
                else
                    shortFilePauser.pause(1L, TimeUnit.SECONDS);
            }
        } catch (TimeoutException e) {
            throw new TableStoreUnavailableException(file,
                    "Metadata file found in readOnly mode, but not initialized yet", e);
        }
    }

    private void validateFirstHeaderPhysicalBound(MappedBytes mappedBytes, long readBound) {
        if (readBound < Integer.BYTES)
            throw new SingleTableStore.SchemaCorruptionException(
                    "the file does not contain a complete first header");
        final int header = mappedBytes.readVolatileInt(0L);
        if (Wires.isReady(header) && Wires.lengthOf(header) > readBound - Integer.BYTES)
            throw new SingleTableStore.SchemaCorruptionException(
                    "the first header extends beyond the physical file");
    }

    private void validateFirstHeaderBeforeWritableMapping() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (raf.length() < Integer.BYTES)
                throw new CorruptTableStoreException(file, "the file does not contain a complete first header");
            final int header = Integer.reverseBytes(raf.readInt());
            //! A short existing file with a nonzero unfinished header cannot be repaired by preallocating it. The regular
            //! writer maps its initial allocation before publishing this marker; preserve the short failed-publication file.
            //! SingleTableBuilderCorruptMetadataTest#failedWritableOpenDoesNotGrowAnIncompleteFirstHeader loses this guard.
            if (header != 0 && !Wires.isReady(header))
                throw new CorruptTableStoreException(file, "the file does not contain a ready first header");
            if (Wires.isReady(header) && Wires.lengthOf(header) > raf.length() - Integer.BYTES)
                throw new CorruptTableStoreException(file, "the first header extends beyond the physical file");
        } catch (FileNotFoundException e) {
            throw new TableStoreUnavailableException(file, "Metadata file is unavailable", e);
        }
    }

    private void validateFirstHeaderBeforeMapping(long physicalLength) throws IOException {
        if (physicalLength < Integer.BYTES)
            throw new CorruptTableStoreException(file,
                    "the file does not contain a complete first header");
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            final int header = Integer.reverseBytes(raf.readInt());
            if (Wires.isReady(header) && Wires.lengthOf(header) > physicalLength - Integer.BYTES)
                throw new CorruptTableStoreException(file,
                        "the first header extends beyond the physical file");
        }
    }

    private void preflightTableStoreType(ValueIn valueIn) {
        //! Validate the persisted alias before reflective construction. A post-construction instanceof check still lets an
        //! arbitrary TableStore constructor execute and temporarily hands it the builder's mapping.
        //! SingleTableBuilderCorruptMetadataTest#nullAndWrongTypedHeaderValuesAreReportedAsCorruption covers null, scalar
        //! and typed-but-foreign values without granting any of them construction or mapping ownership.
        //! SingleTableBuilderCorruptMetadataTest#foreignTableStoreIsRejectedAndReleasesTheMapping fails if that constructor
        //! runs, while direct SingleTableStore construction protects the mutable-alias case.
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            if (!valueIn.isTyped())
                throw new SingleTableStore.SchemaCorruptionException(
                        "the first header does not hold the table-store schema");
            final StringBuilder typeName = stlSb.get();
            try {
                valueIn.typePrefix(typeName, StringBuilder::append);
            } catch (RuntimeException e) {
                throw new SingleTableStore.SchemaCorruptionException(
                        "the first header does not hold a readable table-store schema", e);
            }
            if (!StringUtils.isEqual(typeName, "STStore")
                    && !StringUtils.isEqual(typeName, SingleTableStore.class.getName()))
                throw new SingleTableStore.SchemaCorruptionException(
                        "the first header does not hold the table-store schema");
        }
    }

    static void requireMarshallableFrame(ValueIn valueIn, String failureMessage) {
        if (valueIn.wireIn().bytes().readRemaining() == 0L)
            throw new SingleTableStore.SchemaCorruptionException(failureMessage);
        final int code = valueIn.wireIn().bytes().peekUnsignedByte();
        if (code != BinaryWireCode.BYTES_LENGTH8
                && code != BinaryWireCode.BYTES_LENGTH16
                && code != BinaryWireCode.BYTES_LENGTH32)
            throw new SingleTableStore.SchemaCorruptionException(failureMessage);

        final long start = valueIn.wireIn().bytes().readPosition();
        final long length;
        try {
            length = valueIn.readLength();
        } catch (RuntimeException e) {
            throw new SingleTableStore.SchemaCorruptionException(failureMessage, e);
        } finally {
            valueIn.wireIn().bytes().readPosition(start);
        }
        final int prefixLength = code == BinaryWireCode.BYTES_LENGTH8 ? 2
                : code == BinaryWireCode.BYTES_LENGTH16 ? 3 : 5;
        if (length < 0L || length > valueIn.wireIn().bytes().readRemaining() - prefixLength)
            throw new SingleTableStore.SchemaCorruptionException(failureMessage);
    }

    private static final class TableStoreConstructorFailed extends RuntimeException {
        private static final long serialVersionUID = 0L;
        private final Class<? extends Metadata> metadataType;

        TableStoreConstructorFailed(Throwable cause, Class<? extends Metadata> metadataType) {
            super(cause);
            this.metadataType = metadataType;
        }

        Class<? extends Metadata> metadataType() {
            return metadataType;
        }
    }

    private ValueIn readTableStoreValue(@NotNull Wire wire) throws StreamCorruptedException {
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            //! The table-store writer emits an EVENT_NAME token for the outer `header`. BinaryWire's general read method also
            //! accepts field names/numbers and leading comments, so validate the exact Queue-owned token before invoking it.
            //! SingleTableBuilderCorruptMetadataTest#outerHeaderRequiresTheCanonicalEventNameToken loses this guard.
            if (wire.bytes().peekUnsignedByte() != net.openhft.chronicle.wire.BinaryWireCode.EVENT_NAME)
                throw new StreamCorruptedException("The first event is not encoded as the table-store header event");
            StringBuilder name = stlSb.get();
            ValueIn valueIn = wire.read(name);
            if (!StringUtils.isEqual(name, MetaDataKeys.header.name())) {
                throw new StreamCorruptedException("The first event name is not the table-store header");
            }
            return valueIn;
        }
    }

    @NotNull
    private TableStore<T> writeTableStore(MappedBytes bytes, Wire wire) {
        final TableStore<T> store = new SingleTableStore<>(wireType, bytes, metadata);
        boolean handedOver = false;
        try {
            try {
                //! Persist Queue's fixed outer alias instead of consulting the mutable global class-to-name mapping. A custom
                //! alias registered before this builder initializes must not make its own healthy output unreadable.
                //! SingleTableBuilderCorruptMetadataTest#writerAlwaysUsesCanonicalTableStoreAlias discriminates this choice.
                wire.writeEventName("header").typedMarshallable("STStore", store);
            } catch (CorruptTableStoreException | TableStoreUnavailableException e) {
                //! Public Queue diagnosis types thrown by application metadata do not acquire file-open provenance merely
                //! because they occur during initial serialization. Preserve them as causes of an ordinary write failure.
                //! SingleTableBuilderCorruptMetadataTest#metadataWriteCannotOriginateReservedTableStoreSignals covers both.
                throw new IORuntimeException("Initial table-store metadata serialization failed", e);
            }
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

    /**
     * Selects the table-store format. Use {@code BINARY_LIGHT} for new stores. {@code BINARY} remains only as a legacy
     * selector whose table-store layout uses the same common subset; {@code READ_ANY} is valid only together with
     * {@link #readOnly(boolean) readOnly(true)}.
     *
     * @param wireType format selector
     * @return this builder
     */
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
