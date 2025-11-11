/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

/**
 * Enum representing the direction in which a Chronicle Queue tailer should move when reading entries.
 * <ul>
 *     <li>{@link #NONE} - Do not move after reading an entry.</li>
 *     <li>{@link #FORWARD} - Move to the next entry after reading.</li>
 *     <li>{@link #BACKWARD} - Move to the previous entry after reading.</li>
 * </ul>
 */
public enum TailerDirection {
    NONE(0), // don't move after a read.
    FORWARD(+1), // move to the next entry
    BACKWARD(-1);

    private final int add;

    /**
     * Constructor for the TailerDirection enum.
     *
     * @param add The value to be added to the current position (0 for NONE, +1 for FORWARD, -1 for BACKWARD)
     */
    TailerDirection(int add) {
        this.add = add;
    }

    /**
     * Returns the adjustment value for the direction, used to change the tailer's position.
     *
     * @return The value indicating the direction's adjustment
     */
    public int add() {
        return add;
    }
}
