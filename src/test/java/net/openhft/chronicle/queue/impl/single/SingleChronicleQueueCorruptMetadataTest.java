/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.table.CorruptTableStoreException;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.TableStoreUnavailableException;
import net.openhft.chronicle.wire.BinaryWireCode;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * QUEUE-148. A queue whose metadata file carries a corrupt first header must report the corruption
 * when it is built, and must not wait for the table store lock timeout first.
 */
public class SingleChronicleQueueCorruptMetadataTest extends QueueTestCommon {

    private static final long TABLE_STORE_TIMEOUT_MS = Jvm.getLong("chronicle.table.store.timeoutMS", 10_000L);

    /**
     * The build must not spend the table store lock timeout retrying a failure that cannot succeed.
     */
    @Test
    public void aCorruptFirstHeaderFailsWithoutWaitingForTheTableStoreLock() {
        File queueDir = queueWithCorruptMetadataHeader();

        long startNanos = System.nanoTime();
        Throwable thrown = buildAndCaptureFailure(queueDir);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertTrue("the build took " + elapsedMs + " ms, which is not less than the table store lock timeout of "
                        + TABLE_STORE_TIMEOUT_MS + " ms. Thrown: " + thrown,
                elapsedMs < Math.max(1L, TABLE_STORE_TIMEOUT_MS / 5));
    }

    /**
     * The caller needs a type it can catch, so that it can tell corruption from lock contention.
     */
    @Test
    public void aCorruptFirstHeaderThrowsCorruptTableStoreException() {
        File queueDir = queueWithCorruptMetadataHeader();

        Throwable thrown = buildAndCaptureFailure(queueDir);

        assertEquals("thrown was " + chainOf(thrown), CorruptTableStoreException.class, thrown.getClass());
        assertTrue("the declared-length guard was not the failing decision: " + thrown,
                thrown.getMessage().contains("length does not match its decoded content"));
    }

    /**
     * Corruption remains in the established I/O exception family, but must not activate the queue
     * builder's read-only fallback.
     */
    @Test
    public void corruptMetadataBypassesReadonlyFallbackAndRemainsAnIORuntimeException() {
        File queueDir = queueWithCorruptMetadataHeader();

        Throwable thrown = buildAndCaptureFailure(queueDir, true);

        assertEquals("thrown was " + chainOf(thrown), CorruptTableStoreException.class, thrown.getClass());
        assertTrue("corruption left the established I/O exception family", thrown instanceof IORuntimeException);
    }

    @Test
    public void nestedFileNotFoundFromMetadataDoesNotActivateReadonlyFallback() {
        final File queueDir = getTmpDir();
        assertTrue("could not create queue directory " + queueDir, queueDir.mkdirs() || queueDir.isDirectory());
        final File metadataFile = new File(queueDir, SingleChronicleQueue.QUEUE_METADATA_FILE);
        NestedFileNotFoundMetadata.FAILURE = null;
        try (net.openhft.chronicle.queue.impl.TableStore<NestedFileNotFoundMetadata> setupStore =
                     SingleTableBuilder.binary(metadataFile, new NestedFileNotFoundMetadata()).build()) {
            // The first open writes a valid table-store header for the extensible metadata fixture.
            assertTrue("metadata fixture store was closed during construction", !setupStore.isClosed());
        }

        final FileNotFoundException nested = new FileNotFoundException("constructor sentinel");
        NestedFileNotFoundMetadata.FAILURE = new IORuntimeException(nested);
        try {
            final IORuntimeException thrown = assertThrows(IORuntimeException.class,
                    () -> {
                        try (ChronicleQueue unexpectedQueue = SingleChronicleQueueBuilder
                                .binary(queueDir)
                                .readOnly(true)
                                .build()) {
                            fail("decoder failure activated metadata-unavailable fallback: " + unexpectedQueue);
                        }
                    });
            assertFalse("decoder failure activated metadata-unavailable fallback",
                    thrown instanceof TableStoreUnavailableException);
            assertSame("the constructor's nested cause was discarded", nested, rootCause(thrown));
        } finally {
            NestedFileNotFoundMetadata.FAILURE = null;
        }
    }

    @Test
    public void constructorAvailabilitySignalDoesNotActivateReadonlyFallback() {
        final File queueDir = getTmpDir();
        assertTrue("could not create queue directory " + queueDir, queueDir.mkdirs() || queueDir.isDirectory());
        final File metadataFile = new File(queueDir, SingleChronicleQueue.QUEUE_METADATA_FILE);
        NestedFileNotFoundMetadata.FAILURE = null;
        try (net.openhft.chronicle.queue.impl.TableStore<NestedFileNotFoundMetadata> setupStore =
                     SingleTableBuilder.binary(metadataFile, new NestedFileNotFoundMetadata()).build()) {
            assertFalse("metadata fixture store was closed during construction", setupStore.isClosed());
        }

        final TableStoreUnavailableException fabricated = unavailable(metadataFile, "constructor sentinel");
        NestedFileNotFoundMetadata.FAILURE = fabricated;
        try {
            final IORuntimeException thrown = assertThrows(IORuntimeException.class,
                    () -> SingleChronicleQueueBuilder.binary(queueDir).readOnly(true).build());
            assertFalse("constructor signal activated metadata-unavailable fallback",
                    thrown instanceof TableStoreUnavailableException);
            assertSame("the constructor's signal was discarded", fabricated, rootCause(thrown));
        } finally {
            NestedFileNotFoundMetadata.FAILURE = null;
        }
    }

    @Test
    public void metadataAccessorUnavailabilityDoesNotActivateReadonlyFallback() {
        Assume.assumeFalse("Windows deliberately does not use the synthetic read-only fallback", OS.isWindows());
        final File queueDir = getTmpDir();
        assertTrue("could not create queue directory " + queueDir, queueDir.mkdirs() || queueDir.isDirectory());
        final File metadataFile = new File(queueDir, SingleChronicleQueue.QUEUE_METADATA_FILE);
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(queueDir).readOnly(true);
        AccessFailingSCQMeta.FAILURE = null;
        final AccessFailingSCQMeta fixture = new AccessFailingSCQMeta(
                new SCQRoll(builder.rollCycle(), builder.epoch(), null, null), builder.sourceId());
        try (net.openhft.chronicle.queue.impl.TableStore<AccessFailingSCQMeta> setupStore =
                     SingleTableBuilder.binary(metadataFile, fixture).build()) {
            assertFalse("metadata fixture store was closed during construction", setupStore.isClosed());
        }

        final TableStoreUnavailableException expected = unavailable(metadataFile, "accessor sentinel");
        AccessFailingSCQMeta.FAILURE = expected;
        try {
            final TableStoreUnavailableException actual = assertThrows(
                    TableStoreUnavailableException.class, builder::initializeMetadata);
            assertSame("post-open availability signal activated fallback", expected, actual);
        } finally {
            AccessFailingSCQMeta.FAILURE = null;
            Closeable.closeQuietly(builder.metaStore());
        }
    }

    @Test
    public void metadataAccessorFailuresRetainIdentityAndReleaseTheMapping() {
        assertMetadataAccessorFailureRetainsIdentity(new AssertionError("metadata accessor error"), "error");
        assertMetadataAccessorFailureRetainsIdentity(new IOException("metadata accessor checked failure"), "checked");
    }

    private void assertMetadataAccessorFailureRetainsIdentity(Throwable expected, String stem) {
        final File queueDir = new File(getTmpDir(), stem);
        assertTrue("could not create queue directory " + queueDir, queueDir.mkdirs() || queueDir.isDirectory());
        final File metadataFile = new File(queueDir, SingleChronicleQueue.QUEUE_METADATA_FILE);
        final SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder.binary(queueDir).readOnly(true);
        AccessFailingSCQMeta.FAILURE = null;
        final AccessFailingSCQMeta fixture = new AccessFailingSCQMeta(
                new SCQRoll(builder.rollCycle(), builder.epoch(), null, null), builder.sourceId());
        try (net.openhft.chronicle.queue.impl.TableStore<AccessFailingSCQMeta> setupStore =
                     SingleTableBuilder.binary(metadataFile, fixture).build()) {
            assertFalse("metadata fixture store was closed during construction", setupStore.isClosed());
        }

        AccessFailingSCQMeta.FAILURE = expected;
        try {
            final Throwable actual = assertThrows(expected.getClass(), builder::build);
            assertSame("metadata accessor failure was wrapped or replaced", expected, actual);
            BackgroundResourceReleaser.releasePendingResources();
            assertTrue("failed queue build retained the metadata mapping", metadataFile.delete());
        } finally {
            AccessFailingSCQMeta.FAILURE = null;
            Closeable.closeQuietly(builder.metaStore());
        }
    }

    /**
     * A fault inside the serialised table store, rather than in the declared length, must also be
     * reported as corruption. The after-check of {@link QueueTestCommon} asserts on the same run
     * that the failed build closed the mapping that it opened.
     */
    @Test
    public void aCorruptHeaderBodyIsReportedAsCorruptionAndReleasesTheMapping() {
        File queueDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            queue.createAppender().writeText("hello");
        }
        flipFirstByteOfHeaderBody(new File(queueDir, SingleChronicleQueue.QUEUE_METADATA_FILE));

        Throwable thrown = buildAndCaptureFailure(queueDir);

        assertEquals("thrown was " + chainOf(thrown), CorruptTableStoreException.class, thrown.getClass());
        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("does not hold a readable table store"));
    }

    @Test
    public void queueOwnedMetadataBodyDamageIsCorruption() {
        final File queueDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            queue.createAppender().writeText("hello");
        }
        final File metadataFile = new File(queueDir, SingleChronicleQueue.QUEUE_METADATA_FILE);
        final long sourceIdCodeAt = sourceIdValueCodeAt(metadataFile);
        try (RandomAccessFile raf = new RandomAccessFile(metadataFile, "rw")) {
            raf.seek(sourceIdCodeAt);
            raf.write(BinaryWireCode.TYPE_PREFIX);
        } catch (IOException e) {
            throw new AssertionError("could not damage SCQMeta", e);
        }

        final Throwable thrown = buildAndCaptureFailure(queueDir);

        assertEquals("thrown was " + chainOf(thrown), CorruptTableStoreException.class, thrown.getClass());
        assertTrue("message was " + thrown.getMessage(),
                thrown.getMessage().contains("queue metadata body cannot be decoded"));
    }

    /**
     * The check must not reject a healthy queue.
     */
    @Test
    public void anIntactMetadataFileStillBuildsAndReadsBack() {
        File queueDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            queue.createAppender().writeText("hello");
        }

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            assertEquals("hello", queue.createTailer().readText());
        }
    }

    private Throwable buildAndCaptureFailure(File queueDir) {
        return buildAndCaptureFailure(queueDir, false);
    }

    private Throwable buildAndCaptureFailure(File queueDir, boolean readOnly) {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).readOnly(readOnly).build()) {
            fail("the build accepted a corrupt metadata file: " + queue);
            return null;
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable e) {
            return e;
        }
    }

    private File queueWithCorruptMetadataHeader() {
        File queueDir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            queue.createAppender().writeText("hello");
        }
        corruptDeclaredLengthOfFirstHeader(new File(queueDir, SingleChronicleQueue.QUEUE_METADATA_FILE));
        return queueDir;
    }

    /**
     * Adds one to the first byte of the metadata file. That byte is the low byte of the first header,
     * so the declared length of the header becomes one byte longer than the header that was written.
     */
    private void corruptDeclaredLengthOfFirstHeader(File metadataFile) {
        try (RandomAccessFile raf = new RandomAccessFile(metadataFile, "rw")) {
            int firstByte = raf.read();
            if (firstByte == 0xff)
                fail("the first byte of " + metadataFile + " is 0xff, so this corruption would carry into the length");
            raf.seek(0);
            raf.write(firstByte + 1);
        } catch (IOException e) {
            throw new AssertionError("could not corrupt " + metadataFile, e);
        }
    }

    /**
     * Inverts the first byte that follows the first header. That byte starts the serialised table
     * store, so the header still declares the length that it was written with.
     */
    private void flipFirstByteOfHeaderBody(File metadataFile) {
        try (RandomAccessFile raf = new RandomAccessFile(metadataFile, "rw")) {
            raf.seek(Integer.BYTES);
            int b = raf.read();
            raf.seek(Integer.BYTES);
            raf.write(b ^ 0xff);
        } catch (IOException e) {
            throw new AssertionError("could not corrupt " + metadataFile, e);
        }
    }

    private long sourceIdValueCodeAt(File metadataFile) {
        final long[] result = {-1L};
        try (MappedBytes bytes = MappedBytes.mappedBytes(
                metadataFile, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, true)) {
            bytes.singleThreadedCheckDisabled(true);
            try {
                final Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                wire.readFirstHeader();
                final ValueIn storeValue = wire.read(MetaDataKeys.header);
                storeValue.typePrefix(new StringBuilder(), StringBuilder::append);
                storeValue.applyToMarshallable(storeWire -> {
                    storeWire.read(MetaDataField.wireType).object(WireType.class);
                    final ValueIn metadataValue = storeWire.read(MetaDataField.metadata);
                    metadataValue.typePrefix(new StringBuilder(), StringBuilder::append);
                    metadataValue.applyToMarshallable(metadataWire -> {
                        metadataWire.read(MetaDataField.roll).skipValue();
                        metadataWire.read(MetaDataField.sourceId);
                        result[0] = metadataWire.bytes().readPosition();
                        return null;
                    });
                    return null;
                });
            } finally {
                bytes.singleThreadedCheckReset();
            }
        } catch (IOException e) {
            throw new AssertionError("could not locate SCQMeta sourceId", e);
        }
        assertTrue("SCQMeta sourceId value was not located", result[0] >= 0L);
        return result[0];
    }

    private String chainOf(Throwable thrown) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = thrown; t != null; t = t.getCause())
            sb.append(t).append(" <- ");
        return sb.toString();
    }

    private Throwable rootCause(Throwable failure) {
        Throwable result = failure;
        while (result.getCause() != null && result.getCause() != result)
            result = result.getCause();
        return result;
    }

    private TableStoreUnavailableException unavailable(File file, String message) {
        try {
            final Constructor<TableStoreUnavailableException> constructor =
                    TableStoreUnavailableException.class.getDeclaredConstructor(File.class, String.class);
            constructor.setAccessible(true);
            return constructor.newInstance(file, message);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not create the package-owned availability signal", e);
        }
    }

    public static final class NestedFileNotFoundMetadata implements Metadata {
        private static RuntimeException FAILURE;

        public NestedFileNotFoundMetadata() {
        }

        @SuppressWarnings("unused")
        public NestedFileNotFoundMetadata(@NotNull WireIn wire) {
            if (FAILURE != null)
                throw FAILURE;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            // No persisted fields are needed to exercise exception provenance.
        }
    }

    public static final class AccessFailingSCQMeta extends SCQMeta {
        private static Throwable FAILURE;

        AccessFailingSCQMeta(@NotNull SCQRoll roll, int sourceId) {
            super(roll, sourceId);
        }

        @SuppressWarnings("unused")
        AccessFailingSCQMeta(@NotNull WireIn wire) {
            super(wire);
        }

        @Override
        public int sourceId() {
            if (FAILURE != null)
                throw Jvm.rethrow(FAILURE);
            return super.sourceId();
        }
    }
}
