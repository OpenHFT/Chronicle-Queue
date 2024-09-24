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
