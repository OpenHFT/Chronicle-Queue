/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/** An exact write would cross a sealed roll or an incomplete physical record. */
public final class ExactWriteBoundaryException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    ExactWriteBoundaryException(String message) {
        super(message);
    }
}
