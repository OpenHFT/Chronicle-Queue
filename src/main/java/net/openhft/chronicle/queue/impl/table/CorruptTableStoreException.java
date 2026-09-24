/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import java.io.File;

/**
 * Thrown when a table store file cannot be read because its content is not consistent.
 * <p>
 * The queue throws this as soon as it finds the fault. It does not retry, because no
 * number of retries makes a corrupt file readable.
 * <p>
 * This is not an {@link net.openhft.chronicle.core.io.IORuntimeException}. The queue builder
 * catches that type and falls back to a read-only table store. Corruption must not take that
 * path.
 */
public class CorruptTableStoreException extends IllegalStateException {
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
