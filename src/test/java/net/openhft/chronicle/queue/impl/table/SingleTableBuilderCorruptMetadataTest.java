/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataField;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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

    @Test
    public void nullAndWrongTypedHeaderValuesAreReportedAsCorruption() throws IOException {
        final File nullValue = writeFirstHeader("null-value", "header", ValueOut::nu11);
        final CorruptTableStoreException nullFailure = assertCorruption(nullValue, false);
        assertTrue("message was " + nullFailure.getMessage(),
                nullFailure.getMessage().contains("holds null instead of a table store"));
        assertCorruption(nullValue, true);

        final File wrongType = writeFirstHeader("wrong-type", "header",
                value -> value.typedMarshallable(new WrongHeaderValue()));
        final CorruptTableStoreException wrongTypeFailure = assertCorruption(wrongType, false);
        assertTrue("message was " + wrongTypeFailure.getMessage(),
                wrongTypeFailure.getMessage().contains(WrongHeaderValue.class.getName()));
        assertFalse("diagnostic exposed application content: " + wrongTypeFailure.getMessage(),
                wrongTypeFailure.getMessage().contains(WrongHeaderValue.SECRET));
        assertCorruption(wrongType, true);
    }

    @Test
    public void wrongCloseableHeaderValueIsClosedOnRejection() throws IOException {
        CloseableWrongHeaderValue.CLOSE_COUNT.set(0);
        final File file = writeFirstHeader("wrong-closeable-type", "header",
                value -> value.typedMarshallable(new CloseableWrongHeaderValue()));

        assertCorruption(file, false);

        assertEquals("the rejected decoded value was not closed", 1, CloseableWrongHeaderValue.CLOSE_COUNT.get());
        assertFileCanBeDeleted(file);
    }

    @Test
    public void nullAndWrongTypedNestedMetadataAreReportedAsCorruption() throws IOException {
        final File nullMetadata = writeTableStoreHeader("null-nested-metadata", ValueOut::nu11);
        final CorruptTableStoreException nullFailure = assertCorruption(nullMetadata, false);
        assertTrue("message was " + nullFailure.getMessage(),
                nullFailure.getMessage().contains("holds null instead of metadata"));

        CloseableWrongHeaderValue.CLOSE_COUNT.set(0);
        final File wrongMetadata = writeTableStoreHeader("wrong-nested-metadata",
                value -> value.typedMarshallable(new CloseableWrongHeaderValue()));
        final CorruptTableStoreException wrongFailure = assertCorruption(wrongMetadata, false);
        assertTrue("message was " + wrongFailure.getMessage(),
                wrongFailure.getMessage().contains(CloseableWrongHeaderValue.class.getName()));
        assertEquals("the rejected nested metadata was not closed",
                1, CloseableWrongHeaderValue.CLOSE_COUNT.get());
        assertFileCanBeDeleted(nullMetadata);
        assertFileCanBeDeleted(wrongMetadata);
    }

    @Test
    public void unknownTypeAliasIsReportedAsCorruption() throws IOException {
        final File file = writeFirstHeader("unknown-alias", "header",
                value -> value.typedMarshallable("missing.queue.table.store.Type", wire -> {
                }));

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertNotNull("the parser failure was not retained", thrown.getCause());
        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold a readable table store"));
    }

    @Test
    public void malformedFirstEventNameIsReportedAsCorruption() throws IOException {
        final File file = writeFirstHeader("wrong-event", "not-header",
                value -> value.typedMarshallable(new WrongHeaderValue()));

        final CorruptTableStoreException thrown = assertCorruption(file, false);

        assertNotNull("the event-name failure was not retained", thrown.getCause());
        assertTrue("cause was " + thrown.getCause(),
                thrown.getCause().getMessage().contains("first message should be the header"));
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
    public void unsupportedPersistedWireTypeIsCorruptionAndReleasesTheMapping() throws IOException {
        final File file = writeFirstHeader("unsupported-persisted-wire", "header",
                value -> value.typedMarshallable("STStore",
                        wire -> wire.write(MetaDataField.wireType).object(WireType.TEXT)));
        final byte[] before = Files.readAllBytes(file.toPath());

        final CorruptTableStoreException thrown = assertThrows(CorruptTableStoreException.class,
                () -> SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build());

        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("unsupported wire type " + WireType.TEXT));
        assertTrue("root cause was " + rootCause(thrown), rootCause(thrown) instanceof IllegalArgumentException);
        assertTrue("message was " + rootCause(thrown).getMessage(),
                rootCause(thrown).getMessage().contains(WireType.TEXT.name()));
        assertArrayEquals("rejecting the unsupported persisted format changed the file",
                before, Files.readAllBytes(file.toPath()));
        assertFileCanBeDeleted(file);
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
        try {
            assertMetadataConstructorFailureNotCorrupt(file, applicationFailure);
            assertMetadataConstructorFailureNotCorrupt(file, boundsFailure);
            assertMetadataConstructorFailureNotCorrupt(file, overlapFailure);
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
        assertFileCanBeDeleted(unknownNestedAlias);
    }

    private void assertMetadataConstructorFailureNotCorrupt(File file, RuntimeException expected) {
        ReadFailingMetadata.FAILURE = expected;
        final IORuntimeException thrown = assertThrows(IORuntimeException.class,
                () -> SingleTableBuilder.binary(file, new ReadFailingMetadata()).build());
        assertFalse("application failure was labelled as disk corruption",
                thrown instanceof CorruptTableStoreException);
        assertSame("application constructor failure lost its identity", expected, rootCause(thrown));
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
    public void failedInitialMetadataWriteClosesTheProvisionalStore() throws IOException {
        final File file = newTableStoreFile("write-failure");
        final AssertionError expected = new AssertionError("deliberate metadata write failure");

        final AssertionError thrown = assertThrows(AssertionError.class,
                () -> SingleTableBuilder.binary(file, new WriteFailingMetadata(expected)).build());

        assertSame("the metadata failure was replaced", expected, thrown);
        assertFileCanBeDeleted(file);
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

    public static final class WrongHeaderValue extends SelfDescribingMarshallable {
        private static final String SECRET = "header-secret-must-not-appear";

        private String payload = SECRET;
    }

    public static final class CloseableWrongHeaderValue extends SelfDescribingMarshallable implements java.io.Closeable {
        private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

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

    public static final class OverrideFailingMetadata implements Metadata {
        private final transient BufferUnderflowException failure;

        private OverrideFailingMetadata(BufferUnderflowException failure) {
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

    public static final class ReadFailingMetadata implements Metadata {
        private static RuntimeException FAILURE;

        public ReadFailingMetadata() {
        }

        @SuppressWarnings("unused")
        public ReadFailingMetadata(@NotNull WireIn wire) {
            if (FAILURE != null)
                throw FAILURE;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            // no persisted fields are needed for this classification test
        }
    }
}
