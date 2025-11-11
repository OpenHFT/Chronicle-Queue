/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * This exception is thrown when a store file, which is expected to be present,
 * is missing. The missing file could be due to accidental deletion or
 * other external causes, potentially leading to issues in data consistency.
 *
 * <p>This class extends {@link IllegalStateException} and provides an
 * informative message to identify the missing store file.
 */
public class MissingStoreFileException extends IllegalStateException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a {@code MissingStoreFileException} with the specified detail message.
     *
     * @param s The detail message, indicating which file is missing or relevant information.
     */
    public MissingStoreFileException(String s) {
        super(s);
    }
}
