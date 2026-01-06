/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

/**
 * Enum representing the state of a file in relation to its usage or existence.
 * <ul>
 *     <li>{@link #OPEN} - The file is currently open and being used.</li>
 *     <li>{@link #CLOSED} - The file is closed and not in use.</li>
 *     <li>{@link #NON_EXISTENT} - The file does not exist in the file system.</li>
 *     <li>{@link #UNDETERMINED} - The state of the file cannot be determined.</li>
 * </ul>
 */
public enum FileState {
    /**
     * The file is open and being used.
     */
    OPEN,
    /**
     * The file is closed and not in use by the queue.
     */
    CLOSED,
    /**
     * The file does not exist in the file system for this queue path.
     */
    NON_EXISTENT,
    /**
     * The state of the file cannot be determined.
     */
    UNDETERMINED
}
