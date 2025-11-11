/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * Represents the result of a scan operation within a Chronicle Queue.
 * This enum provides the possible outcomes when scanning for a specific entry
 * or state within a queue.
 */
public enum ScanResult {

    /**
     * The requested entry was found during the scan.
     */
    FOUND,

    /**
     * The scan has not yet reached the desired position or entry.
     */
    NOT_REACHED,

    /**
     * The requested entry was not found in the queue.
     */
    NOT_FOUND,

    /**
     * The end of the file has been reached during the scan.
     */
    END_OF_FILE
}
