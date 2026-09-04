/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.table.CorruptTableStoreException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
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
                thrown.getMessage().contains("bytes but its content ends at"));
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

    private String chainOf(Throwable thrown) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = thrown; t != null; t = t.getCause())
            sb.append(t).append(" <- ");
        return sb.toString();
    }
}
