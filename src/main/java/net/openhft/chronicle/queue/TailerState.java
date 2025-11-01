/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

/**
 * Enum representing the possible states of a Chronicle Queue tailer.
 * <ul>
 *     <li>{@link #END_OF_CYCLE} - The tailer has reached the end of the current cycle.</li>
 *     <li>{@link #FOUND_IN_CYCLE} - An entry was found in the current cycle.</li>
 *     <li>{@link #BEYOND_START_OF_CYCLE} - The tailer has moved beyond the start of the cycle.</li>
 *     <li>{@link #CYCLE_NOT_FOUND} - The requested cycle could not be found.</li>
 *     <li>{@link #NOT_REACHED_IN_CYCLE} - The tailer has not yet reached an entry in the cycle.</li>
 *     <li>{@link #UNINITIALISED} - The tailer has not been initialized yet.</li>
 * </ul>
 */
public enum TailerState {
    END_OF_CYCLE,
    FOUND_IN_CYCLE,
    BEYOND_START_OF_CYCLE,
    CYCLE_NOT_FOUND,
    NOT_REACHED_IN_CYCLE,
    UNINITIALISED
}
