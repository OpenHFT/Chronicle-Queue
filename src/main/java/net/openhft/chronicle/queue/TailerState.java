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
    END_OF_CYCLE,          // Tailer has reached the end of the current cycle
    FOUND_IN_CYCLE,        // An entry was found in the current cycle
    BEYOND_START_OF_CYCLE, // Tailer has moved beyond the start of the cycle
    CYCLE_NOT_FOUND,       // The requested cycle could not be found
    NOT_REACHED_IN_CYCLE,  // Tailer has not yet reached an entry in the cycle
    UNINITIALISED          // Tailer has not been initialized yet
}
