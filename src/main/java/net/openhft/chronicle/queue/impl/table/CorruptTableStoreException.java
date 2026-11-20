/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.io.IORuntimeException;

import java.io.File;

//! Keep corruption in the established IORuntimeException family so existing broad caller catches remain compatible.
//! SingleChronicleQueueCorruptMetadataTest#corruptMetadataBypassesReadonlyFallbackAndRemainsAnIORuntimeException
//! fails if the queue builder hides this subtype behind its read-only fallback or the subtype leaves that family.
/**
 * Thrown when a table store file cannot be read because its content is not consistent.
 * <p>
 * The queue throws this as soon as it finds the fault. It does not retry, because no
 * number of retries makes a corrupt file readable.
 * <p>
 * This subtype remains an {@link IORuntimeException} so existing broad I/O failure handlers
 * continue to catch it. Queue construction nevertheless propagates corruption instead of
 * activating the legacy read-only table-store fallback.
 */
public class CorruptTableStoreException extends IORuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * @param file    The table store file that holds the fault.
     * @param message What is inconsistent about the file.
     */
    public CorruptTableStoreException(File file, String message) {
        super("Corrupt table store file " + file + ": " + message);
    }

    /**
     * @param file    The table store file that holds the fault.
     * @param message What is inconsistent about the file.
     * @param cause   The failure that exposed the fault.
     */
    public CorruptTableStoreException(File file, String message, Throwable cause) {
        super("Corrupt table store file " + file + ": " + message, cause);
    }
}
