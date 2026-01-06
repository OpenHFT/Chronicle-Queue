/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
 *     <li>{@link #UNINITIALISED} - The tailer has not been initialised yet.</li>
 * </ul>
 */
public enum TailerState {
    /**
     * The tailer has reached the end of the current cycle.
     */
    END_OF_CYCLE,
    /**
     * An entry was found in the current cycle.
     */
    FOUND_IN_CYCLE,
    /**
     * The tailer has moved beyond the start of the cycle.
     */
    BEYOND_START_OF_CYCLE,
    /**
     * The requested cycle could not be found.
     */
    CYCLE_NOT_FOUND,
    /**
     * The tailer has not yet reached an entry in the cycle.
     */
    NOT_REACHED_IN_CYCLE,
    /**
     * The tailer has not been initialised yet and has no position.
     */
    UNINITIALISED
}
