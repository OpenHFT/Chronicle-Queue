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
    /**
     * Does not advance the tailer after a read, so repeated reads return the same index.
     */
    NONE(0),
    /**
     * Advances to the next entry after each read, moving forward through the queue.
     */
    FORWARD(+1),
    /**
     * Moves to the previous entry after each read, enabling reverse traversal.
     */
    BACKWARD(-1);

    private final int add;

    /**
     * Creates a direction with its index delta for tailer movement.
     *
     * @param add The value to be added to the current position (0 for NONE, +1 for FORWARD, -1 for BACKWARD)
     */
    TailerDirection(int add) {
        this.add = add;
    }

    /**
     * Returns the position delta applied after each read to update the tailer index.
     *
     * @return The value indicating the direction's adjustment
     */
    public int add() {
        return add;
    }
}
