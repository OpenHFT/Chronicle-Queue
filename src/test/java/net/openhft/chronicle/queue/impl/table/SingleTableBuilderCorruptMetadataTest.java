/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ClassNotFoundRuntimeException;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataField;
import net.openhft.chronicle.wire.BinaryWireCode;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Exercises the first-header classification and ownership boundaries without passing through the
 * queue builder's read-only fallback.
 */
public class SingleTableBuilderCorruptMetadataTest extends QueueTestCommon {
    private static final int LENGTH_MASK = 0x3FFFFFFF;

    @Test
    public void nullAndWrongTypedHeaderValuesAreReportedAsCorruption() throws IOException {
        final File nullValue = writeFirstHeader("null-value", "header", ValueOut::nu11);
        final CorruptTableStoreException nullFailure = assertCorruption(nullValue, false);
        assertTrue("message was " + nullFailure.getMessage(),
                nullFailure.getMessage().contains("does not hold the table-store schema"));
        assertCorruption(nullValue, true);

        final File wrongType = writeFirstHeader("wrong-type", "header",
                value -> value.typedMarshallable(new WrongHeaderValue()));
        final CorruptTableStoreException wrongTypeFailure = assertCorruption(wrongType, false);
        assertTrue("message was " + wrongTypeFailure.getMessage(),
                wrongTypeFailure.getMessage().contains("does not hold the table-store schema"));
        assertFalse("diagnostic exposed application content: " + wrongTypeFailure.getMessage(),
                wrongTypeFailure.getMessage().contains(WrongHeaderValue.SECRET));
        assertCorruption(wrongType, true);

        final File scalarValue = writeFirstHeader("scalar-value", "header", value -> value.int32(17));
        assertCorruption(scalarValue, false);
        assertCorruption(scalarValue, true);
    }

    @Test
    public void wrongCloseableHeaderValueIsNotConstructed() throws IOException {
        CloseableWrongHeaderValue.CONSTRUCT_COUNT.set(0);
        CloseableWrongHeaderValue.CLOSE_COUNT.set(0);
        final File file = writeFirstHeader("wrong-closeable-type", "header",
                value -> value.typedMarshallable(new CloseableWrongHeaderValue()));
        CloseableWrongHeaderValue.CONSTRUCT_COUNT.set(0);
        CloseableWrongHeaderValue.CLOSE_COUNT.set(0);

        assertCorruption(file, false);

        assertEquals("the rejected outer type was constructed", 0,
                CloseableWrongHeaderValue.CONSTRUCT_COUNT.get());
        assertEquals("a rejected outer object was closed", 0, CloseableWrongHeaderValue.CLOSE_COUNT.get());
        assertFileCanBeDeleted(file);
    }

    @Test
    public void nullAndWrongTypedNestedMetadataAreReportedAsCorruption() throws IOException {
        final File nullMetadata = writeTableStoreHeader("null-nested-metadata", ValueOut::nu11);
        final CorruptTableStoreException nullFailure = assertCorruption(nullMetadata, false);
        assertTrue("message was " + nullFailure.getMessage(),
                nullFailure.getMessage().contains("does not hold metadata"));

        CloseableWrongHeaderValue.CONSTRUCT_COUNT.set(0);
        CloseableWrongHeaderValue.CLOSE_COUNT.set(0);
        final File wrongMetadata = writeTableStoreHeader("wrong-nested-metadata",
                value -> value.typedMarshallable(new CloseableWrongHeaderValue()));
        CloseableWrongHeaderValue.CONSTRUCT_COUNT.set(0);
        CloseableWrongHeaderValue.CLOSE_COUNT.set(0);
        final CorruptTableStoreException wrongFailure = assertCorruption(wrongMetadata, false);
        assertTrue("message was " + wrongFailure.getMessage(),
                wrongFailure.getMessage().contains("does not hold metadata"));
        assertEquals("the rejected nested metadata type was constructed",
                0, CloseableWrongHeaderValue.CONSTRUCT_COUNT.get());
        assertEquals("a rejected nested metadata object was closed",
                0, CloseableWrongHeaderValue.CLOSE_COUNT.get());

        final File scalarMetadata = writeTableStoreHeader("scalar-nested-metadata", value -> value.int32(17));
        assertCorruption(scalarMetadata, false);
        assertFileCanBeDeleted(nullMetadata);
        assertFileCanBeDeleted(wrongMetadata);
        assertFileCanBeDeleted(scalarMetadata);
    }

    @Test
    public void unknownTypeAliasIsReportedAsCorruption() throws IOException {
        final File file = writeFirstHeader("unknown-alias", "header",
                value -> value.typedMarshallable("missing.queue.table.store.Type", wire -> {
                }));

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertNotNull("the parser failure was not retained", thrown.getCause());
        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold the table-store schema"));
        assertThrowableChainOmits(thrown, "missing.queue.table.store.Type");
    }

    @Test
    public void malformedFirstEventNameIsReportedAsCorruption() throws IOException {
        final File file = writeFirstHeader("wrong-event", "not-header",
                value -> value.typedMarshallable(new WrongHeaderValue()));

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertNotNull("the event-name failure was not retained", thrown.getCause());
        assertThrowableChainOmits(thrown, "not-header");
    }

    @Test
    public void outerHeaderRequiresTheCanonicalEventNameToken() throws IOException {
        final File file = writeFirstHeader("non-canonical-header-token", "header",
                value -> value.typedMarshallable("STStore",
                        wire -> wire.write(MetaDataField.wireType).object(WireType.BINARY_LIGHT)));
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.seek(Integer.BYTES);
            assertEquals("fixture did not use EVENT_NAME", BinaryWireCode.EVENT_NAME, raf.readUnsignedByte());
            raf.seek(Integer.BYTES);
            raf.write(BinaryWireCode.FIELD_NAME_ANY);
        }

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold a readable table store"));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void partialStoreConstructionIsCorruptionAndReleasesTheMapping() throws IOException {
        final File file = writeFirstHeader("partial-store", "header",
                value -> value.typedMarshallable("STStore",
                        wire -> wire.write(MetaDataField.wireType).nu11()));

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertNotNull("the partial-construction failure was not retained", thrown.getCause());
        assertFileCanBeDeleted(file);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void unsupportedPersistedWireTypeIsCorruptionAndReleasesTheMapping() throws IOException {
        for (WireType wireType : new WireType[]{
                WireType.TEXT,
                WireType.FIELDLESS_BINARY,
                WireType.COMPRESSED_BINARY,
                WireType.RAW
        }) {
            final File file = writeFirstHeader("unsupported-persisted-" + wireType.name().toLowerCase(), "header",
                    value -> value.typedMarshallable("STStore",
                            wire -> wire.write(MetaDataField.wireType).object(wireType)));
            final byte[] before = Files.readAllBytes(file.toPath());

            final CorruptTableStoreException thrown = assertThrows(CorruptTableStoreException.class,
                    () -> SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build());

            assertTrue("message was " + thrown.getMessage(),
                    thrown.getMessage().contains("unsupported wire type"));
            assertArrayEquals("rejecting " + wireType + " changed the file",
                    before, Files.readAllBytes(file.toPath()));
            assertFileCanBeDeleted(file);
        }
    }

    @Test
    public void metadataOverrideFailureRetainsIdentityAndReleasesTheMapping() throws IOException {
        final File file = newTableStoreFile("override-failure");
        try (TableStore<OverrideFailingMetadata> store = SingleTableBuilder
                .binary(file, new OverrideFailingMetadata((BufferUnderflowException) null))
                .build()) {
            assertNotNull("the valid setup store was not built", store);
        }
        final BufferUnderflowException expected = new BufferUnderflowException();

        final BufferUnderflowException thrown = assertThrows(BufferUnderflowException.class,
                () -> SingleTableBuilder.binary(file, new OverrideFailingMetadata(expected)).build());

        assertSame("metadata policy failure was translated or wrapped", expected, thrown);
        assertFileCanBeDeleted(file);
    }

    @Test
    public void metadataOverrideCannotOriginateAnAvailabilitySignal() throws IOException {
        final File file = newTableStoreFile("override-availability");
        try (TableStore<OverrideFailingMetadata> store = SingleTableBuilder
                .binary(file, new OverrideFailingMetadata((RuntimeException) null))
                .build()) {
            assertNotNull("the valid setup store was not built", store);
        }
        final TableStoreUnavailableException expected = new TableStoreUnavailableException(file, "override sentinel");

        final IORuntimeException thrown = assertThrows(IORuntimeException.class,
                () -> SingleTableBuilder.binary(file, new OverrideFailingMetadata(expected)).build());

        assertFalse("metadata override impersonated the file-opening boundary", thrown instanceof TableStoreUnavailableException);
        assertFalse("metadata policy was classified as disk corruption", thrown instanceof CorruptTableStoreException);
        assertSame("the override failure was discarded", expected, thrown.getCause());
        assertFileCanBeDeleted(file);
    }

    @Test
    public void nestedMetadataAliasAndConstructorFailuresAreNotMisreportedAsCorruption() throws IOException {
        final File file = newTableStoreFile("metadata-constructor-failure");
        ReadFailingMetadata.FAILURE = null;
        try (TableStore<ReadFailingMetadata> store = SingleTableBuilder
                .binary(file, new ReadFailingMetadata())
                .build()) {
            assertNotNull(store);
        }

        final UnsupportedOperationException applicationFailure =
                new UnsupportedOperationException("deliberate application metadata failure");
        final BufferUnderflowException boundsFailure = new BufferUnderflowException();
        final OverlappingFileLockException overlapFailure = new OverlappingFileLockException();
        final ClassNotFoundRuntimeException applicationClassFailure =
                new ClassNotFoundRuntimeException(new ClassNotFoundException("application constructor sentinel"));
        try {
            assertMetadataConstructorFailureNotCorrupt(file, applicationFailure);
            assertMetadataConstructorFailureNotCorrupt(file, boundsFailure);
            assertMetadataConstructorFailureNotCorrupt(file, overlapFailure);
            assertMetadataConstructorFailureNotCorrupt(file, applicationClassFailure);
            assertFileCanBeDeleted(file);
        } finally {
            ReadFailingMetadata.FAILURE = null;
        }

        final File unknownNestedAlias = writeTableStoreHeader("unknown-nested-metadata",
                value -> value.typedMarshallable("missing.queue.metadata.Type", wire -> {
                }));
        final IORuntimeException aliasFailure = assertThrows(IORuntimeException.class,
                () -> SingleTableBuilder.binary(unknownNestedAlias, Metadata.NoMeta.INSTANCE).build());
        assertFalse("an unavailable extensible metadata type was labelled as disk corruption",
                aliasFailure instanceof CorruptTableStoreException);
        assertThrowableChainOmits(aliasFailure, "missing.queue.metadata.Type");
        assertFileCanBeDeleted(unknownNestedAlias);
    }

    @Test
    public void metadataConstructorErrorRetainsCauseAndReleasesTheMapping() throws IOException {
        final File file = tableStoreWithReadFailingMetadata("metadata-constructor-error");
        final AssertionError expected = new AssertionError("deliberate metadata constructor error");
        ReadFailingMetadata.FAILURE = expected;
        try {
            final IORuntimeException actual = assertThrows(IORuntimeException.class,
                    () -> SingleTableBuilder.binary(file, new ReadFailingMetadata()).build());
            assertSame("metadata constructor Error was discarded", expected, rootCause(actual));
            assertFileCanBeDeleted(file);
        } finally {
            ReadFailingMetadata.FAILURE = null;
        }
    }

    @Test
    public void nestedMetadataFramingFailureIsCorruptionAndRedacted() throws IOException {
        final File file = newTableStoreFile("nested-metadata-framing");
        try (TableStore<SecretMetadata> store = SingleTableBuilder.binary(file, new SecretMetadata()).build()) {
            assertNotNull(store);
        }
        final long lengthCodeAt = nestedMetadataLengthCodeAt(file);
        overwriteLengthWithMaximum(file, lengthCodeAt);
        final long corruptedLength = Files.size(file.toPath());
        SecretMetadata.READ_COUNT.set(0);

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold readable metadata"));
        assertEquals("malformed metadata framing invoked its constructor", 0, SecretMetadata.READ_COUNT.get());
        assertEquals("failed nested framing decode grew the file", corruptedLength, Files.size(file.toPath()));
        assertThrowableChainOmits(thrown, SecretMetadata.SECRET);
        assertFileCanBeDeleted(file);
    }

    @Test
    public void metadataRequiresMarshallableFraming() throws IOException {
        final File file = newTableStoreFile("metadata-missing-frame");
        try (TableStore<SecretMetadata> store = SingleTableBuilder.binary(file, new SecretMetadata()).build()) {
            assertNotNull(store);
        }
        replaceLengthPrefixWithPadding(file, nestedMetadataLengthCodeAt(file));
        SecretMetadata.READ_COUNT.set(0);

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold readable metadata"));
        assertEquals("metadata without marshallable framing invoked its constructor", 0, SecretMetadata.READ_COUNT.get());
        assertFileCanBeDeleted(file);

        assertUnframedMetadataIsRejected("metadata-scalar-frame", value -> value.int32(17));
        assertUnframedMetadataIsRejected("metadata-null-frame", ValueOut::nu11);
        assertUnframedMetadataIsRejected("metadata-absent-frame", value -> { });
    }

    @Test
    public void outerTableStoreFramingFailureIsCorruptionWithoutConstruction() throws IOException {
        final File file = newTableStoreFile("outer-table-store-framing");
        try (TableStore<SecretMetadata> store = SingleTableBuilder.binary(file, new SecretMetadata()).build()) {
            assertNotNull(store);
        }
        increaseLengthByOne(file, outerTableStoreLengthCodeAt(file));
        final long corruptedLength = Files.size(file.toPath());
        SecretMetadata.READ_COUNT.set(0);

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold a readable table store"));
        assertEquals("out-of-header STStore framing invoked metadata", 0, SecretMetadata.READ_COUNT.get());
        assertEquals("failed outer framing decode grew the file", corruptedLength, Files.size(file.toPath()));
        assertThrowableChainOmits(thrown, SecretMetadata.SECRET);
        assertFileCanBeDeleted(file);
    }

    @Test
    public void outerSchemaRequiresMarshallableFraming() throws IOException {
        final File file = newTableStoreFile("outer-missing-frame");
        try (TableStore<SecretMetadata> store = SingleTableBuilder.binary(file, new SecretMetadata()).build()) {
            assertNotNull(store);
        }
        replaceLengthPrefixWithPadding(file, outerTableStoreLengthCodeAt(file));
        SecretMetadata.READ_COUNT.set(0);

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold a readable table store"));
        assertEquals("STStore without marshallable framing invoked metadata", 0, SecretMetadata.READ_COUNT.get());
        assertFileCanBeDeleted(file);

        assertUnframedOuterStoreIsRejected("outer-scalar-frame", value -> value.int32(17));
        assertUnframedOuterStoreIsRejected("outer-null-frame", ValueOut::nu11);
        assertUnframedOuterStoreIsRejected("outer-absent-frame", value -> { });
    }

    @Test
    public void oversizedOuterTableStoreLengthCannotEscapeTheFirstHeader() throws IOException {
        final File file = newTableStoreFile("oversized-outer-table-store-framing");
        try (TableStore<SecretMetadata> store = SingleTableBuilder.binary(file, new SecretMetadata()).build()) {
            assertNotNull(store);
        }
        overwriteLengthWithMaximum(file, outerTableStoreLengthCodeAt(file));
        final long corruptedLength = Files.size(file.toPath());
        SecretMetadata.READ_COUNT.set(0);

        assertCorruption(file, false);

        assertEquals("oversized STStore framing invoked metadata", 0, SecretMetadata.READ_COUNT.get());
        assertEquals("oversized STStore framing grew the file", corruptedLength, Files.size(file.toPath()));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void metadataFieldFramingFailureIsCorruptionWithoutConstruction() throws IOException {
        final File file = newTableStoreFile("metadata-field-framing");
        try (TableStore<SecretMetadata> store = SingleTableBuilder.binary(file, new SecretMetadata()).build()) {
            assertNotNull(store);
        }
        final long fieldCodeAt = metadataFieldCodeAt(file);
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.seek(fieldCodeAt);
            raf.write(BinaryWireCode.PADDING32);
            raf.writeInt(Integer.reverseBytes(LENGTH_MASK));
        }
        SecretMetadata.READ_COUNT.set(0);

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertEquals("malformed metadata field framing invoked its constructor", 0, SecretMetadata.READ_COUNT.get());
        assertThrowableChainOmits(thrown, SecretMetadata.SECRET);
        assertFileCanBeDeleted(file);
    }

    @Test
    public void trailingTableStoreFieldAfterMetadataIsCorruption() throws IOException {
        final File file = writeFirstHeader("trailing-table-store-field", "header",
                value -> value.typedMarshallable("STStore", wire -> {
                    wire.write(MetaDataField.wireType).object(WireType.BINARY_LIGHT);
                    wire.write(MetaDataField.metadata).typedMarshallable(new SecretMetadata());
                    wire.write("unexpected").int32(17);
                }));
        SecretMetadata.READ_COUNT.set(0);

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("table-store body length does not match its decoded content"));
        assertEquals("the valid metadata body was not read", 1, SecretMetadata.READ_COUNT.get());
        assertFileCanBeDeleted(file);
    }

    @Test
    public void unknownTableStoreFieldsAreRejectedAtEveryPosition() throws IOException {
        final File beforeWireType = writeFirstHeader("unknown-before-wire-type", "header",
                value -> value.typedMarshallable("STStore", wire -> {
                    wire.write("unexpected").int32(1);
                    wire.write(MetaDataField.wireType).object(WireType.BINARY_LIGHT);
                }));
        final File beforeMetadata = writeFirstHeader("unknown-before-metadata", "header",
                value -> value.typedMarshallable("STStore", wire -> {
                    wire.write(MetaDataField.wireType).object(WireType.BINARY_LIGHT);
                    wire.write("unexpected").int32(2);
                    wire.write(MetaDataField.metadata).typedMarshallable(new SecretMetadata());
                }));

        assertCorruption(beforeWireType, false);
        assertCorruption(beforeMetadata, false);

        assertFileCanBeDeleted(beforeWireType);
        assertFileCanBeDeleted(beforeMetadata);
    }

    @Test
    public void legacyRecoveryFieldBeforeMetadataStillOpens() throws IOException {
        final File file = writeFirstHeader("legacy-recovery", "header",
                value -> value.typedMarshallable("STStore", wire -> {
                    wire.write(MetaDataField.wireType).object(WireType.BINARY);
                    wire.write(MetaDataField.recovery).typedMarshallable("LegacyTimedStoreRecovery",
                            recovery -> recovery.write("timeStamp").int64(0L));
                    wire.write(MetaDataField.metadata).typedMarshallable(new SecretMetadata());
                    wire.writeAlignTo(Integer.BYTES, 0);
                }));

        try (TableStore<SecretMetadata> store = SingleTableBuilder
                .builder(file, WireType.BINARY_LIGHT, new SecretMetadata())
                .build();
             LongValue value = store.acquireValueFor("legacy.recovery", 73L)) {
            assertEquals(SecretMetadata.SECRET, store.metadata().payload);
            assertEquals(73L, value.getValue());
        }
        try (TableStore<SecretMetadata> store = SingleTableBuilder
                .builder(file, WireType.READ_ANY, new SecretMetadata())
                .readOnly(true)
                .build();
             LongValue value = store.acquireValueFor("legacy.recovery", -1L)) {
            assertEquals(SecretMetadata.SECRET, store.metadata().payload);
            assertEquals(73L, value.getValue());
        }
        assertFileCanBeDeleted(file);
    }

    @Test
    public void trailingTableStoreCommentIsCorruption() throws IOException {
        final File file = writeFirstHeader("trailing-table-store-comment", "header",
                value -> value.typedMarshallable("STStore", wire -> {
                    wire.write(MetaDataField.wireType).object(WireType.BINARY_LIGHT);
                    wire.write(MetaDataField.metadata).typedMarshallable(new SecretMetadata());
                    wire.writeComment("comment-not-produced-by-table-store-writer");
                }));

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("table-store body length does not match its decoded content"));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void writerProducedFinalPaddingAcrossAllAlignmentsReopens() throws IOException {
        final boolean[] observedPadding = new boolean[Integer.BYTES];
        for (int payloadLength = 0; payloadLength < 16; payloadLength++) {
            final File file = newTableStoreFile("final-padding-" + payloadLength);
            try (TableStore<PaddingMetadata> store = SingleTableBuilder
                    .binary(file, new PaddingMetadata(payloadLength))
                    .build()) {
                assertNotNull(store);
            }
            final int paddingBytes = tableStoreTrailingPaddingBytes(file);
            assertTrue("writer produced unexpected final padding length " + paddingBytes,
                    paddingBytes >= 0 && paddingBytes < Integer.BYTES);
            observedPadding[paddingBytes] = true;
            try (TableStore<PaddingMetadata> store = SingleTableBuilder
                    .binary(file, new PaddingMetadata(0))
                    .build()) {
                assertEquals(payloadLength, store.metadata().payload.length());
            }
            assertFileCanBeDeleted(file);
        }
        for (int paddingBytes = 0; paddingBytes < observedPadding.length; paddingBytes++)
            assertTrue("no fixture exercised " + paddingBytes + " final padding bytes", observedPadding[paddingBytes]);
    }

    @Test
    public void foreignTableStoreIsRejectedAndReleasesTheMapping() throws IOException {
        ForeignTableStore.CONSTRUCT_COUNT.set(0);
        ForeignTableStore.CLOSE_COUNT.set(0);
        final ForeignTableStore fixture = new ForeignTableStore();
        final File file;
        try {
            file = writeFirstHeader("foreign-table-store", "header",
                    value -> value.typedMarshallable(ForeignTableStore.class.getName(), fixture));
        } finally {
            fixture.close();
        }
        ForeignTableStore.CONSTRUCT_COUNT.set(0);
        ForeignTableStore.CLOSE_COUNT.set(0);

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold the table-store schema"));
        assertEquals("the foreign table store constructor ran", 0, ForeignTableStore.CONSTRUCT_COUNT.get());
        assertEquals("a rejected foreign table store was closed", 0, ForeignTableStore.CLOSE_COUNT.get());
        assertFileCanBeDeleted(file);
    }

    @Test
    public void remappedTableStoreAliasCannotConstructForeignImplementation() throws IOException {
        final File file = newTableStoreFile("remapped-table-store-alias");
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            assertEquals(SingleTableStore.class, store.getClass());
        }

        expectException("Replaced class net.openhft.chronicle.queue.impl.table.SingleTableStore with class "
                + ForeignTableStore.class.getName());
        expectException("Replaced class " + ForeignTableStore.class.getName()
                + " with class net.openhft.chronicle.queue.impl.table.SingleTableStore");
        ForeignTableStore.CONSTRUCT_COUNT.set(0);
        ForeignTableStore.CLOSE_COUNT.set(0);
        CLASS_ALIASES.addAlias(ForeignTableStore.class, "STStore");
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            assertEquals("mutable alias substituted a foreign implementation", SingleTableStore.class, store.getClass());
            assertEquals("remapped foreign table store was constructed", 0, ForeignTableStore.CONSTRUCT_COUNT.get());
        } finally {
            CLASS_ALIASES.addAlias(SingleTableStore.class, "STStore");
        }
        assertFileCanBeDeleted(file);
    }

    @Test
    public void writerAlwaysUsesCanonicalTableStoreAlias() throws Exception {
        final File file = newTableStoreFile("canonical-writer-alias");
        final String javaExecutable = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
        final java.util.List<String> command = new java.util.ArrayList<>();
        command.add(javaExecutable);
        command.addAll(java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(AliasFirstWriter.class.getName());
        command.add(file.getAbsolutePath());
        final Process writer = new ProcessBuilder(command)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();

        try {
            assertTrue("alias-first writer did not terminate", writer.waitFor(15L, TimeUnit.SECONDS));
            assertEquals("alias-first writer failed", 0, writer.exitValue());
        } finally {
            if (writer.isAlive()) {
                writer.destroyForcibly();
                writer.waitFor(15L, TimeUnit.SECONDS);
            }
        }
        assertEquals("STStore", persistedOuterAlias(file));
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build();
             LongValue value = store.acquireValueFor("alias.first", -1L)) {
            assertEquals(47L, value.getValue());
        }
        assertFileCanBeDeleted(file);
    }

    @Test
    public void applicationCorruptionExceptionIsNotPromoted() throws IOException {
        final File file = tableStoreWithReadFailingMetadata("application-corruption");
        final CorruptTableStoreException direct =
                new CorruptTableStoreException(file, "application-created sentinel");
        try {
            ReadFailingMetadata.FAILURE = direct;
            final IORuntimeException directFailure = assertThrows(IORuntimeException.class,
                    () -> SingleTableBuilder.binary(file, new ReadFailingMetadata()).build());
            assertFalse("application exception was promoted as Queue's diagnosis",
                    directFailure instanceof CorruptTableStoreException);
            assertSame("the direct application failure was lost", direct, rootCause(directFailure));

            final CorruptTableStoreException nested =
                    new CorruptTableStoreException(file, "nested application-created sentinel");
            final IORuntimeException wrapper = new IORuntimeException("application wrapper", nested);
            ReadFailingMetadata.FAILURE = wrapper;
            final IORuntimeException wrappedFailure = assertThrows(IORuntimeException.class,
                    () -> SingleTableBuilder.binary(file, new ReadFailingMetadata()).build());
            assertFalse("wrapped application exception was promoted as Queue's diagnosis",
                    wrappedFailure instanceof CorruptTableStoreException);
            assertSame("the application wrapper was removed", wrapper, findInChain(wrappedFailure, wrapper));
            assertSame("the nested application failure was lost", nested, rootCause(wrappedFailure));

            final SingleTableStore.SchemaCorruptionException unrelatedMarker =
                    new SingleTableStore.SchemaCorruptionException("unrelated table-store failure");
            final CorruptTableStoreException unrelatedCorruption =
                    new CorruptTableStoreException(file, "unrelated corruption", unrelatedMarker);
            final IORuntimeException launderingWrapper =
                    new IORuntimeException("application-carried corruption", unrelatedCorruption);
            ReadFailingMetadata.FAILURE = launderingWrapper;
            final IORuntimeException launderingFailure = assertThrows(IORuntimeException.class,
                    () -> SingleTableBuilder.binary(file, new ReadFailingMetadata()).build());
            assertFalse("an unrelated Queue marker was promoted as this file's diagnosis",
                    launderingFailure instanceof CorruptTableStoreException);
            assertSame("the application wrapper carrying the unrelated failure was removed",
                    launderingWrapper, findInChain(launderingFailure, launderingWrapper));
        } finally {
            ReadFailingMetadata.FAILURE = null;
        }
        assertFileCanBeDeleted(file);
    }

    @Test
    public void corruptionCauseChainDoesNotExposeStoredContent() throws IOException {
        final String eventSecret = "stored-event-name-secret";
        final File wrongEvent = writeFirstHeader("redacted-event", eventSecret,
                value -> value.typedMarshallable(new WrongHeaderValue()));
        assertThrowableChainOmits(assertCorruption(wrongEvent, false), eventSecret);

        final String aliasSecret = "stored.alias.secret.Type";
        final File wrongAlias = writeFirstHeader("redacted-alias", "header",
                value -> value.typedMarshallable(aliasSecret, wire -> {
                }));
        assertThrowableChainOmits(assertCorruption(wrongAlias, false), aliasSecret);
        assertFileCanBeDeleted(wrongEvent);
        assertFileCanBeDeleted(wrongAlias);
    }

    private void assertMetadataConstructorFailureNotCorrupt(File file, RuntimeException expected) {
        ReadFailingMetadata.FAILURE = expected;
        final IORuntimeException thrown = assertThrows(IORuntimeException.class,
                () -> SingleTableBuilder.binary(file, new ReadFailingMetadata()).build());
        assertFalse("application failure was labelled as disk corruption",
                thrown instanceof CorruptTableStoreException);
        assertSame("application constructor failure lost its identity", expected, findInChain(thrown, expected));
    }

    private void assertReservedWriteFailureIsIsolated(String stem, RuntimeException expected) throws IOException {
        final File file = newTableStoreFile(stem);
        final IORuntimeException actual = assertThrows(IORuntimeException.class,
                () -> SingleTableBuilder.binary(file, new ReservedSignalWriteMetadata(expected)).build());
        assertFalse("application write failure became corruption", actual instanceof CorruptTableStoreException);
        assertFalse("application write failure became availability", actual instanceof TableStoreUnavailableException);
        assertSame(expected, actual.getCause());
        assertFileCanBeDeleted(file);
    }

    private void assertUnframedMetadataIsRejected(String stem, Consumer<ValueOut> frameWriter) throws IOException {
        final File file = writeFirstHeader(stem, "header", value -> value.typedMarshallable("STStore", wire -> {
            wire.write(MetaDataField.wireType).object(WireType.BINARY_LIGHT);
            final ValueOut metadataValue = wire.write(MetaDataField.metadata);
            metadataValue.typePrefix(SecretMetadata.class);
            frameWriter.accept(metadataValue);
            wire.writeAlignTo(Integer.BYTES, 0);
        }));
        SecretMetadata.READ_COUNT.set(0);

        assertCorruption(file, false);

        assertEquals("unframed metadata invoked its constructor", 0, SecretMetadata.READ_COUNT.get());
        assertFileCanBeDeleted(file);
    }

    private void assertUnframedOuterStoreIsRejected(String stem, Consumer<ValueOut> frameWriter) throws IOException {
        final File file = writeFirstHeader(stem, "header", value -> {
            value.typePrefix("STStore");
            frameWriter.accept(value);
        });

        assertCorruption(file, false);

        assertFileCanBeDeleted(file);
    }

    private File tableStoreWithReadFailingMetadata(String stem) throws IOException {
        final File file = newTableStoreFile(stem);
        ReadFailingMetadata.FAILURE = null;
        try (TableStore<ReadFailingMetadata> store = SingleTableBuilder
                .binary(file, new ReadFailingMetadata())
                .build()) {
            assertNotNull(store);
        }
        return file;
    }

    private long nestedMetadataLengthCodeAt(File file) throws IOException {
        final long[] result = {-1L};
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
            bytes.singleThreadedCheckDisabled(true);
            try {
                final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                wire.readFirstHeader();
                final net.openhft.chronicle.wire.ValueIn storeValue = wire.read("header");
                storeValue.typePrefix(new StringBuilder(), StringBuilder::append);
                storeValue.applyToMarshallable(storeWire -> {
                    storeWire.read(MetaDataField.wireType).object(WireType.class);
                    final net.openhft.chronicle.wire.ValueIn metadataValue =
                            storeWire.read(MetaDataField.metadata);
                    metadataValue.typePrefix(new StringBuilder(), StringBuilder::append);
                    result[0] = storeWire.bytes().readPosition();
                    return null;
                });
            } finally {
                bytes.singleThreadedCheckReset();
            }
        }
        assertTrue("metadata length code was not located", result[0] >= 0L);
        return result[0];
    }

    private long outerTableStoreLengthCodeAt(File file) throws IOException {
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
            bytes.singleThreadedCheckDisabled(true);
            try {
                final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                wire.readFirstHeader();
                final net.openhft.chronicle.wire.ValueIn storeValue = wire.read("header");
                storeValue.typePrefix(new StringBuilder(), StringBuilder::append);
                return wire.bytes().readPosition();
            } finally {
                bytes.singleThreadedCheckReset();
            }
        }
    }

    private String persistedOuterAlias(File file) throws IOException {
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
            bytes.singleThreadedCheckDisabled(true);
            try {
                final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                wire.readFirstHeader();
                final net.openhft.chronicle.wire.ValueIn storeValue = wire.read("header");
                final StringBuilder alias = new StringBuilder();
                storeValue.typePrefix(alias, StringBuilder::append);
                return alias.toString();
            } finally {
                bytes.singleThreadedCheckReset();
            }
        }
    }

    private long metadataFieldCodeAt(File file) throws IOException {
        final long[] result = {-1L};
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
            bytes.singleThreadedCheckDisabled(true);
            try {
                final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                wire.readFirstHeader();
                final net.openhft.chronicle.wire.ValueIn storeValue = wire.read("header");
                storeValue.typePrefix(new StringBuilder(), StringBuilder::append);
                storeValue.applyToMarshallable(storeWire -> {
                    storeWire.read(MetaDataField.wireType).object(WireType.class);
                    storeWire.consumePadding();
                    result[0] = storeWire.bytes().readPosition();
                    return null;
                });
            } finally {
                bytes.singleThreadedCheckReset();
            }
        }
        assertTrue("metadata field code was not located", result[0] >= 0L);
        return result[0];
    }

    private int tableStoreTrailingPaddingBytes(File file) throws IOException {
        final int[] result = {-1};
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
            bytes.singleThreadedCheckDisabled(true);
            try {
                final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                wire.readFirstHeader();
                final net.openhft.chronicle.wire.ValueIn storeValue = wire.read("header");
                storeValue.typePrefix(new StringBuilder(), StringBuilder::append);
                storeValue.applyToMarshallable(storeWire -> {
                    storeWire.read(MetaDataField.wireType).object(WireType.class);
                    storeWire.read(MetaDataField.metadata).typedMarshallable();
                    result[0] = (int) storeWire.bytes().readRemaining();
                    return null;
                });
            } finally {
                bytes.singleThreadedCheckReset();
            }
        }
        assertTrue("table-store trailing padding was not measured", result[0] >= 0);
        return result[0];
    }

    private void increaseLengthByOne(File file, long lengthCodeAt) throws IOException {
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.seek(lengthCodeAt);
            final int code = raf.readUnsignedByte();
            switch (code) {
                case BinaryWireCode.BYTES_LENGTH8: {
                    final int length = raf.readUnsignedByte();
                    assertTrue("cannot increase an 8-bit maximum length", length < 0xff);
                    raf.seek(lengthCodeAt + 1L);
                    raf.write(length + 1);
                    break;
                }
                case BinaryWireCode.BYTES_LENGTH16: {
                    final long length = readUnsignedLittleEndian(raf, 2);
                    assertTrue("cannot increase a 16-bit maximum length", length < 0xffff);
                    raf.seek(lengthCodeAt + 1L);
                    writeUnsignedLittleEndian(raf, length + 1L, 2);
                    break;
                }
                case BinaryWireCode.BYTES_LENGTH32: {
                    final long length = readUnsignedLittleEndian(raf, 4);
                    assertTrue("cannot increase a 32-bit maximum length", length < 0xffffffffL);
                    raf.seek(lengthCodeAt + 1L);
                    writeUnsignedLittleEndian(raf, length + 1L, 4);
                    break;
                }
                default:
                    fail("expected a byte-length code at " + lengthCodeAt + ", found 0x" + Integer.toHexString(code));
            }
        }
    }

    private void overwriteLengthWithMaximum(File file, long lengthCodeAt) throws IOException {
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.seek(lengthCodeAt);
            raf.write(BinaryWireCode.BYTES_LENGTH32);
            raf.writeInt(Integer.reverseBytes(LENGTH_MASK));
        }
    }

    private void replaceLengthPrefixWithPadding(File file, long lengthCodeAt) throws IOException {
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.seek(lengthCodeAt);
            final int code = raf.readUnsignedByte();
            final int prefixWidth;
            if (code == BinaryWireCode.BYTES_LENGTH8)
                prefixWidth = 2;
            else if (code == BinaryWireCode.BYTES_LENGTH16)
                prefixWidth = 3;
            else if (code == BinaryWireCode.BYTES_LENGTH32)
                prefixWidth = 5;
            else
                throw new AssertionError("expected a marshallable length prefix, found 0x" + Integer.toHexString(code));
            raf.seek(lengthCodeAt);
            for (int i = 0; i < prefixWidth; i++)
                raf.write(BinaryWireCode.PADDING);
        }
    }

    private long readUnsignedLittleEndian(java.io.RandomAccessFile raf, int width) throws IOException {
        long value = 0L;
        for (int i = 0; i < width; i++)
            value |= (long) raf.readUnsignedByte() << (8 * i);
        return value;
    }

    private void writeUnsignedLittleEndian(java.io.RandomAccessFile raf, long value, int width) throws IOException {
        for (int i = 0; i < width; i++)
            raf.write((int) (value >>> (8 * i)) & 0xff);
    }

    @Test
    public void failedHeaderDecodePreservesTheFileAndReleasesTheMapping() throws IOException {
        final File file = writeFirstHeader("preserved", "header",
                value -> value.typedMarshallable(new WrongHeaderValue()));
        final byte[] before = Files.readAllBytes(file.toPath());

        assertCorruption(file, false);

        assertArrayEquals("a failed writable open changed the table-store bytes",
                before, Files.readAllBytes(file.toPath()));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void failedWritableOpenDoesNotGrowTruncatedMetadata() throws IOException {
        final File file = newTableStoreFile("truncated");
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            assertNotNull(store);
        }
        final long truncatedLength;
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            final int header = Integer.reverseBytes(raf.readInt());
            assertTrue("the fixture does not contain a ready metadata header",
                    Wires.isReady(header) && (header & Wires.META_DATA) != 0);
            truncatedLength = Integer.BYTES + Wires.lengthOf(header) - 1L;
            assertTrue("the fixture's first header cannot be truncated positively", truncatedLength > Integer.BYTES);
            raf.setLength(truncatedLength);
        }
        final byte[] before = Files.readAllBytes(file.toPath());

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue("message was " + thrown.getMessage(), thrown.getMessage().contains("extends beyond the physical file"));
        assertEquals("failed writable open changed the file length", before.length, Files.size(file.toPath()));
        assertArrayEquals("failed writable open changed the truncated bytes", before, Files.readAllBytes(file.toPath()));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void compactCompleteTableStoreReopensReadOnly() throws IOException {
        final File file = newTableStoreFile("compact-complete");
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build();
             LongValue value = store.acquireValueFor("compact.key", 41L)) {
            assertEquals(41L, value.getValue());
        }
        final long completeLength;
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            final int header = Integer.reverseBytes(raf.readInt());
            assertTrue("the fixture does not contain a ready metadata header",
                    Wires.isReady(header) && (header & Wires.META_DATA) != 0);
            final long headerEnd = Integer.BYTES + Wires.lengthOf(header);
            final long recordAt = (headerEnd + Integer.BYTES - 1L) & -Integer.BYTES;
            raf.seek(recordAt);
            final int recordHeader = Integer.reverseBytes(raf.readInt());
            assertTrue("the fixture does not contain a ready data record",
                    Wires.isReady(recordHeader) && Wires.isData(recordHeader));
            final long usedEnd = recordAt + Integer.BYTES + Wires.lengthOf(recordHeader);
            final int mappingPageSize = PageUtil.getPageSize(file.getAbsolutePath());
            completeLength = ((usedEnd + mappingPageSize - 1L) / mappingPageSize) * mappingPageSize;
            assertTrue("the fixture was not preallocated beyond its complete header", completeLength < raf.length());
            assertTrue("compact fixture did not fit inside the ordinary two-chunk mapping",
                    completeLength < 2L * OS.mapAlign(OS.SAFE_PAGE_SIZE, mappingPageSize));
            raf.setLength(completeLength);
        }

        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                .builder(file, WireType.READ_ANY, Metadata.NoMeta.INSTANCE)
                .readOnly(true)
                .build();
             LongValue value = store.acquireValueFor("compact.key", -1L)) {
            assertEquals(Metadata.NoMeta.INSTANCE, store.metadata());
            assertEquals(41L, value.getValue());
            final Map<String, Long> entries = new LinkedHashMap<>();
            store.forEachKey(entries, (result, key, valueIn) -> result.put(key.toString(), valueIn.int64()));
            assertEquals(Long.valueOf(41L), entries.get("compact.key"));
        }
        assertEquals("read-only open changed the compact file", completeLength, Files.size(file.toPath()));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void compactReadOnlyTableStoreObservesLaterGrowth() throws IOException {
        final File file = newTableStoreFile("compact-growth");
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build();
             LongValue value = store.acquireValueFor("compact.initial", 1L)) {
            assertEquals(1L, value.getValue());
        }
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.setLength(64L * 1024L);
        }

        try (TableStore<Metadata.NoMeta> reader = SingleTableBuilder
                .builder(file, WireType.READ_ANY, Metadata.NoMeta.INSTANCE)
                .readOnly(true)
                .build();
             TableStore<Metadata.NoMeta> writer = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            final char[] keyChars = new char[180];
            java.util.Arrays.fill(keyChars, 'k');
            final String keyPrefix = new String(keyChars);
            String lastKey = null;
            for (int i = 0; i < 400; i++) {
                lastKey = keyPrefix + i;
                try (LongValue appended = writer.acquireValueFor(lastKey, i)) {
                    assertEquals(i, appended.getValue());
                }
            }
            try (LongValue value = reader.acquireValueFor(lastKey, -1L)) {
                assertEquals(399L, value.getValue());
            }
            final AtomicInteger keyCount = new AtomicInteger();
            reader.forEachKey(keyCount, (count, key, valueIn) -> count.incrementAndGet());
            assertEquals(401, keyCount.get());
        }
        assertFileCanBeDeleted(file);
    }

    @Test
    public void readOnlyOpenWaitsForFirstHeaderPublication() throws Exception {
        final File file = newTableStoreFile("publication-window");
        final int mappingPageSize = PageUtil.getPageSize(file.getAbsolutePath());
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.setLength(2L * OS.mapAlign(OS.SAFE_PAGE_SIZE, mappingPageSize));
        }
        final AtomicReference<Throwable> creatorFailure = new AtomicReference<>();
        final Thread creator = new Thread(() -> {
            // Exceed the historical one-second short-file wait: a preallocated zero header uses the lock timeout instead.
            Jvm.pause(1_500L);
            try (TableStore<Metadata.NoMeta> created = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
                if (created.metadata() != Metadata.NoMeta.INSTANCE)
                    throw new AssertionError("creator published unexpected metadata");
            } catch (Throwable t) {
                creatorFailure.set(t);
            }
        }, "table-store-header-publisher");
        creator.start();

        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                .builder(file, WireType.READ_ANY, Metadata.NoMeta.INSTANCE)
                .readOnly(true)
                .build()) {
            assertEquals(Metadata.NoMeta.INSTANCE, store.metadata());
        } finally {
            creator.join(5_000L);
        }
        assertFalse("creator did not terminate", creator.isAlive());
        if (creatorFailure.get() != null)
            throw new AssertionError("creator failed", creatorFailure.get());
        assertFileCanBeDeleted(file);
    }

    @Test
    public void failedWritableOpenDoesNotGrowAnIncompleteFirstHeader() throws IOException {
        final File file = newTableStoreFile("incomplete-first-header");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.writeInt(Integer.reverseBytes(Wires.NOT_COMPLETE_UNKNOWN_LENGTH));
        }
        final byte[] before = Files.readAllBytes(file.toPath());

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertTrue(thrown.getMessage().contains("does not contain a ready first header"));
        assertArrayEquals("failed open changed the incomplete file", before, Files.readAllBytes(file.toPath()));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void readOnlyOpenRejectsHeaderBeyondPhysicalFileWithoutMutation() throws IOException {
        final File file = newTableStoreFile("read-only-physical-bound");
        try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            assertNotNull(store);
        }

        final long compactLength = Math.max(OS.mapAlignment(), 4L * 1024L) + 37L;
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            final int header = Integer.reverseBytes(raf.readInt());
            assertTrue("the fixture does not contain a ready metadata header",
                    Wires.isReady(header) && (header & Wires.META_DATA) != 0);
            final long declaredLength = compactLength + 16L;
            assertTrue("declared length does not fit the Wire header", declaredLength <= LENGTH_MASK);
            raf.seek(0L);
            raf.writeInt(Integer.reverseBytes((header & ~LENGTH_MASK) | (int) declaredLength));
            raf.setLength(compactLength);
        }
        final byte[] before = Files.readAllBytes(file.toPath());

        final CorruptTableStoreException thrown = assertThrows(CorruptTableStoreException.class,
                () -> SingleTableBuilder.builder(file, WireType.READ_ANY, Metadata.NoMeta.INSTANCE)
                        .readOnly(true)
                        .build());

        assertTrue("physical-bound guard was not the failing decision: " + thrown,
                thrown.getMessage().contains("extends beyond the physical file"));
        assertEquals("failed read-only open changed the file length", compactLength, Files.size(file.toPath()));
        assertArrayEquals("failed read-only open changed the compact file", before, Files.readAllBytes(file.toPath()));
        assertFileCanBeDeleted(file);
    }

    @Test
    public void failedInitialMetadataWriteClosesTheProvisionalStore() throws IOException {
        final File file = newTableStoreFile("write-failure");
        final AssertionError expected = new AssertionError("deliberate metadata write failure");

        final AssertionError thrown = assertThrows(AssertionError.class,
                () -> SingleTableBuilder.binary(file, new WriteFailingMetadata(expected)).build());

        assertSame("the metadata failure was replaced", expected, thrown);
        assertFileCanBeDeleted(file);
    }

    @Test
    public void metadataWriteCannotOriginateReservedTableStoreSignals() throws IOException {
        final File unavailableFile = newTableStoreFile("reserved-unavailable");
        assertTrue(unavailableFile.delete());
        final TableStoreUnavailableException unavailable = new TableStoreUnavailableException(
                unavailableFile, "application-owned availability signal");
        assertReservedWriteFailureIsIsolated("write-unavailable", unavailable);

        final File corruptFile = newTableStoreFile("reserved-corrupt");
        final CorruptTableStoreException corrupt = new CorruptTableStoreException(
                corruptFile, "application-owned corruption signal");
        assertReservedWriteFailureIsIsolated("write-corrupt", corrupt);
    }

    private CorruptTableStoreException assertCorruption(File file, boolean readOnly) {
        final CorruptTableStoreException thrown = assertThrows(CorruptTableStoreException.class,
                () -> {
                    try (TableStore<Metadata.NoMeta> store = SingleTableBuilder
                            .binary(file, Metadata.NoMeta.INSTANCE)
                            .readOnly(readOnly)
                            .build()) {
                        fail("the builder accepted corrupt metadata: " + store);
                    }
                });
        assertNotNull("corruption had no message", thrown.getMessage());
        return thrown;
    }

    private File writeFirstHeader(String stem, String eventName, Consumer<ValueOut> valueWriter) throws IOException {
        final File file = newTableStoreFile(stem);
        try (MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, false)) {
            bytes.singleThreadedCheckDisabled(true);
            try {
                final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                assertTrue("a new file already held a first header", wire.writeFirstHeader());
                valueWriter.accept(wire.writeEventName(eventName));
                wire.updateFirstHeader();
            } finally {
                bytes.singleThreadedCheckReset();
            }
        }
        return file;
    }

    private File writeTableStoreHeader(String stem, Consumer<ValueOut> metadataWriter) throws IOException {
        return writeFirstHeader(stem, "header", value -> value.typedMarshallable("STStore", wire -> {
            wire.write(MetaDataField.wireType).object(WireType.BINARY_LIGHT);
            metadataWriter.accept(wire.write(MetaDataField.metadata));
        }));
    }

    private File newTableStoreFile(String stem) throws IOException {
        final File dir = getTmpDir();
        Files.createDirectories(dir.toPath());
        final File file = new File(dir, stem + SingleTableStore.SUFFIX);
        assertTrue("could not create " + file, file.createNewFile());
        return file;
    }

    private void assertFileCanBeDeleted(File file) {
        BackgroundResourceReleaser.releasePendingResources();
        assertTrue("failed construction retained the mapping for " + file, file.delete());
    }

    private Throwable rootCause(Throwable failure) {
        Throwable result = failure;
        while (result.getCause() != null && result.getCause() != result)
            result = result.getCause();
        return result;
    }

    private Throwable findInChain(Throwable failure, Throwable expected) {
        for (Throwable current = failure; current != null && current != current.getCause(); current = current.getCause()) {
            if (current == expected)
                return current;
        }
        return null;
    }

    private void assertThrowableChainOmits(Throwable failure, String forbidden) {
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        final ArrayDeque<Throwable> pending = new ArrayDeque<>();
        pending.add(failure);
        while (!pending.isEmpty()) {
            final Throwable current = pending.removeFirst();
            if (!seen.add(current))
                continue;
            assertFalse("throwable exposed persisted content: " + current,
                    String.valueOf(current).contains(forbidden));
            if (current.getCause() != null)
                pending.add(current.getCause());
            Collections.addAll(pending, current.getSuppressed());
        }
    }

    public static final class WrongHeaderValue extends SelfDescribingMarshallable {
        private static final String SECRET = "header-secret-must-not-appear";

        private String payload = SECRET;
    }

    public static final class CloseableWrongHeaderValue extends SelfDescribingMarshallable implements java.io.Closeable {
        private static final AtomicInteger CONSTRUCT_COUNT = new AtomicInteger();
        private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        public CloseableWrongHeaderValue() {
            CONSTRUCT_COUNT.incrementAndGet();
        }

        @Override
        public void close() {
            CLOSE_COUNT.incrementAndGet();
        }
    }

    public static final class WriteFailingMetadata implements Metadata {
        private final transient AssertionError failure;

        private WriteFailingMetadata(AssertionError failure) {
            this.failure = failure;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            throw failure;
        }
    }

    public static final class ReservedSignalWriteMetadata implements Metadata {
        private final transient RuntimeException failure;

        private ReservedSignalWriteMetadata(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            throw failure;
        }
    }

    public static final class OverrideFailingMetadata implements Metadata {
        private final transient RuntimeException failure;

        private OverrideFailingMetadata(RuntimeException failure) {
            this.failure = failure;
        }

        @SuppressWarnings("unused")
        public OverrideFailingMetadata(@NotNull WireIn wire) {
            this.failure = null;
        }

        @Override
        public <T extends Metadata> void overrideFrom(T metadata) {
            if (failure != null)
                throw failure;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            // no persisted fields are needed for this ownership test
        }
    }

    public static final class SecretMetadata implements Metadata {
        private static final String SECRET = "nested-metadata-secret-must-not-appear";
        private static final AtomicInteger READ_COUNT = new AtomicInteger();
        private String payload = SECRET;

        public SecretMetadata() {
        }

        @SuppressWarnings("unused")
        public SecretMetadata(@NotNull WireIn wire) {
            READ_COUNT.incrementAndGet();
            payload = wire.read("payload").text();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("payload").text(payload);
        }
    }

    public static final class PaddingMetadata implements Metadata {
        private final String payload;

        PaddingMetadata(int payloadLength) {
            final char[] chars = new char[payloadLength];
            java.util.Arrays.fill(chars, 'p');
            payload = new String(chars);
        }

        @SuppressWarnings("unused")
        public PaddingMetadata(@NotNull WireIn wire) {
            payload = wire.read("payload").text();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write("payload").text(payload);
        }
    }

    public static final class ReadFailingMetadata implements Metadata {
        private static Throwable FAILURE;

        public ReadFailingMetadata() {
        }

        @SuppressWarnings("unused")
        public ReadFailingMetadata(@NotNull WireIn wire) {
            if (FAILURE != null)
                throw Jvm.rethrow(FAILURE);
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            // no persisted fields are needed for this classification test
        }
    }

    public static final class ForeignTableStore extends ReadonlyTableStore<Metadata.NoMeta> {
        private static final AtomicInteger CONSTRUCT_COUNT = new AtomicInteger();
        private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        public ForeignTableStore() {
            super(Metadata.NoMeta.INSTANCE);
            CONSTRUCT_COUNT.incrementAndGet();
        }

        @SuppressWarnings("unused")
        public ForeignTableStore(@NotNull WireIn wire) {
            super(Metadata.NoMeta.INSTANCE);
            CONSTRUCT_COUNT.incrementAndGet();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            // This foreign schema deliberately retains no reference to the builder's mapped bytes.
        }

        @Override
        protected void performClose() {
            CLOSE_COUNT.incrementAndGet();
        }
    }

    public static final class AliasFirstWriter {
        public static void main(String[] args) {
            CLASS_ALIASES.addAlias(SingleTableStore.class, "AliasRegisteredBeforeBuilder");
            final File file = new File(args[0]);
            try (TableStore<Metadata.NoMeta> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build();
                 LongValue value = store.acquireValueFor("alias.first", 47L)) {
                if (value.getValue() != 47L)
                    throw new AssertionError("unexpected stored value " + value.getValue());
            }
        }
    }
}
