/*
 * Copyright 2016-2022 chronicle.software
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
    OPEN,           // The file is open and being used
    CLOSED,         // The file is closed and not in use
    NON_EXISTENT,   // The file does not exist
    UNDETERMINED    // The state of the file cannot be determined
}
