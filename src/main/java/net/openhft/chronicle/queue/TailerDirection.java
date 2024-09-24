/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    NONE(0),      // Don't move after a read.
    FORWARD(+1),  // Move to the next entry
    BACKWARD(-1); // Move to the previous entry

    private final int add; // Value indicating how much to adjust the position after a read

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
